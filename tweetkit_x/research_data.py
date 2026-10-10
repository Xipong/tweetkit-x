"""Normalise X responses without promoting quoted posts into search results.

No network, account access, or MCP dependency in this module. Timeline order is
source order, not an invented chronological re-ranking of X's Top results.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit


class ResearchError(RuntimeError):
    """Safe, machine-readable error; never include cookies or response bodies."""

    def __init__(self, code: str, message: str, **details):
        super().__init__(message)
        self.code, self.details = code, details

    def as_dict(self):
        return {"code": self.code, "message": str(self), **self.details}


def tweet_id(value: str) -> str:
    value = str(value).strip()
    if re.fullmatch(r"[0-9]{1,25}", value):
        return value
    try:
        url = urlsplit(value)
        if (url.scheme != "https" or url.hostname not in
                {"x.com", "www.x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"}
                or url.username or url.password or url.port not in (None, 443)):
            raise ValueError
        match = re.fullmatch(r"/(?:[A-Za-z0-9_]{1,15}|i(?:/web)?)/status/([0-9]{1,25})(?:/(?:photo|video)/[0-9]+)?/?", url.path)
        if match:
            return match[1]
    except ValueError:
        pass
    raise ResearchError("invalid_id", "Supply a numeric post ID or an https://x.com/.../status/... URL.")


def handle(value: str) -> str:
    value = value.strip().lstrip("@")
    if not re.fullmatch(r"[A-Za-z0-9_]{1,15}", value):
        raise ResearchError("invalid_user", "Supply an X handle, not a display name or arbitrary URL.")
    return value


def integer(value, default=None):
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def dig(node, *path, default=None):
    for key in path:
        if not isinstance(node, dict):
            return default
        node = node.get(key)
    return default if node is None else node


def safe_url(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = urlsplit(value)
        return value if parsed.scheme in ("http", "https") and parsed.hostname and not parsed.username else None
    except ValueError:
        return None


def user(node):
    if not isinstance(node, dict):
        return None
    legacy, core = node.get("legacy") or {}, node.get("core") or {}
    uid = node.get("rest_id") or legacy.get("id_str")
    username = core.get("screen_name") or legacy.get("screen_name")
    if not uid or not username:
        return None
    return {
        "id": str(uid), "username": username, "name": core.get("name") or legacy.get("name"),
        "url": f"https://x.com/{username}", "bio": legacy.get("description"),
        "created_at": core.get("created_at") or legacy.get("created_at"),
        "location": legacy.get("location"), "followers": legacy.get("followers_count"),
        "following": legacy.get("friends_count"), "posts": legacy.get("statuses_count"),
        "protected": legacy.get("protected"), "blue_verified": node.get("is_blue_verified"),
        "verified": legacy.get("verified"),
    }


def post(node, depth=0):
    if not isinstance(node, dict):
        return None
    if node.get("__typename") == "TweetWithVisibilityResults":
        return post(node.get("tweet"), depth)
    legacy = node.get("legacy") or {}
    tid = node.get("rest_id") or legacy.get("id_str")
    if not tid or "full_text" not in legacy:
        return None
    author = user(dig(node, "core", "user_results", "result"))
    note = dig(node, "note_tweet", "note_tweet_results", "result", default={})
    entities = {**(legacy.get("entities") or {}), **(note.get("entity_set") or {})}
    media = []
    for m in dig(legacy, "extended_entities", "media", default=entities.get("media", [])):
        media.append({
            "type": m.get("type"), "url": safe_url(m.get("media_url_https") or m.get("media_url")),
            "alt_text": m.get("ext_alt_text"), "width": dig(m, "original_info", "width"),
            "height": dig(m, "original_info", "height"),
            "duration_ms": dig(m, "video_info", "duration_millis"),
            "variants": [{"url": safe_url(v.get("url")), "content_type": v.get("content_type"),
                          "bitrate": v.get("bitrate")} for v in dig(m, "video_info", "variants", default=[])],
        })
    text = note.get("text") or legacy.get("full_text", "")
    result = {
        "id": str(tid), "url": f"https://x.com/{author['username'] if author else 'i'}/status/{tid}",
        "text": text, "author": author, "author_id": str(legacy.get("user_id_str") or (author or {}).get("id") or ""),
        "created_at": legacy.get("created_at"), "language": legacy.get("lang"),
        "conversation_id": legacy.get("conversation_id_str"),
        "in_reply_to_id": legacy.get("in_reply_to_status_id_str"),
        "in_reply_to_user_id": legacy.get("in_reply_to_user_id_str"),
        "text_may_be_truncated": bool(legacy.get("truncated") and not note.get("text")),
        "metrics": {"likes": legacy.get("favorite_count"), "reposts": legacy.get("retweet_count"),
                    "replies": legacy.get("reply_count"), "quotes": legacy.get("quote_count"),
                    "bookmarks": legacy.get("bookmark_count"), "views": integer(dig(node, "views", "count"))},
        "links": [{"url": safe_url(e.get("expanded_url") or e.get("url")),
                   "short_url": safe_url(e.get("url"))} for e in entities.get("urls", [])],
        "hashtags": [e.get("text") for e in entities.get("hashtags", [])],
        "mentions": [e.get("screen_name") for e in entities.get("user_mentions", [])],
        "media": media,
    }
    article = dig(node, "article", "article_results", "result")
    if isinstance(article, dict):
        plain = article.get("plain_text") or "\n".join(
            b.get("text", "") for b in dig(article, "content_state", "blocks", default=[]))
        result["article"] = {"title": article.get("title"), "text": plain or article.get("preview_text"),
                             "preview_only": not bool(plain)}
    for key, raw in (
        ("quoted_post", dig(node, "quoted_status_result", "result")),
        ("reposted_post", dig(legacy, "retweeted_status_result", "result")),
    ):
        if raw and depth < 2:
            result[key] = post(raw, depth + 1)
    if legacy.get("quoted_status_id_str"):
        result["quoted_post_id"] = legacy["quoted_status_id_str"]
    return result


@dataclass
class Timeline:
    posts: list[dict] = field(default_factory=list)
    users: list[dict] = field(default_factory=list)
    next_cursor: str | None = None
    branches: list[dict] = field(default_factory=list)
    unavailable: list[dict] = field(default_factory=list)
    recognized: bool = False


def timeline(payload) -> Timeline:
    """Read entries/modules, not arbitrary Tweet descendants of the payload."""
    out = Timeline()
    post_ids, user_ids = set(), set()

    def content(node, entry_id="", nested=False):
        if isinstance(node, list):
            for item in node:
                content(item, entry_id, nested)
            return
        if not isinstance(node, dict) or node.get("promotedMetadata"):
            return
        if node.get("cursorType") and node.get("value"):
            cursor = {"cursor": node["value"], "type": node["cursorType"], "entry_id": entry_id}
            if node["cursorType"] == "Bottom" and not nested:
                out.next_cursor = node["value"]
            elif node["cursorType"] not in ("Top",):
                out.branches.append(cursor)
            return
        if "tweet_results" in node:
            raw = dig(node, "tweet_results", "result")
            item = post(raw)
            if item and item["id"] not in post_ids:
                post_ids.add(item["id"])
                out.posts.append(item)
            elif not item:
                out.unavailable.append({"entry_id": entry_id, "reason": "Post unavailable or unsupported payload"})
            return  # quoted/reposted posts stay inside their actual result
        if "user_results" in node:
            item = user(dig(node, "user_results", "result"))
            if item and item["id"] not in user_ids:
                user_ids.add(item["id"])
                out.users.append(item)
            return
        for key in ("content", "itemContent", "item"):
            if key in node:
                content(node[key], node.get("entryId", entry_id), nested)
        for key in ("items", "moduleItems"):
            if key in node:
                content(node[key], node.get("entryId", entry_id), True)
        if node.get("itemType") == "TimelineTombstone" or node.get("__typename") == "TweetTombstone":
            out.unavailable.append({"entry_id": entry_id, "reason": "Post unavailable"})

    def instructions(node):
        if isinstance(node, list):
            for child in node:
                instructions(child)
        elif isinstance(node, dict):
            if isinstance(node.get("instructions"), list):
                out.recognized = True
                for instruction in node["instructions"]:
                    for entry in instruction.get("entries", []):
                        content(entry, entry.get("entryId", ""))
                    if "entry" in instruction:
                        content(instruction["entry"], instruction["entry"].get("entryId", ""))
                    if "moduleItems" in instruction:
                        content(instruction["moduleItems"], instruction.get("moduleEntryId", ""), True)
                return
            for key, child in node.items():
                if key not in {"tweet_results", "quoted_status_result", "retweeted_status_result"}:
                    instructions(child)
    instructions(payload)
    if not out.recognized:
        raise ResearchError("schema_changed", "X returned no recognised timeline. This is not evidence of zero matches.")
    return out
