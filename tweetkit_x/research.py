"""Bounded research operations, resumable pages, and author-thread reconstruction."""
from __future__ import annotations

import hashlib
import json
import secrets
import threading
import time
from collections import OrderedDict, deque
from datetime import datetime, timezone

from .research_data import ResearchError, dig, handle, post, timeline, tweet_id, user
from .research_transport import XTransport

COVERAGE = "Only results exposed to this X session; never an exhaustive archive."
DETAIL_FLAGS = {"with_rux_injections": False, "includePromotedContent": False, "withCommunity": True,
                "withQuickPromoteEligibilityTweetFields": True, "withBirdwatchNotes": True,
                "withVoice": True, "withV2Timeline": True}
ARTICLE_FLAGS = {"withArticleRichContentState": True, "withArticlePlainText": True}


def bounded(value, low, high, name):
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ResearchError("invalid_argument", f"{name} must be an integer between {low} and {high}.")
    return value


def query_text(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise ResearchError("invalid_query", "Use a nonempty X search query of at most 2048 characters.")
    return value.strip()


def timestamp():
    return datetime.now(timezone.utc).isoformat()


class CursorStore:
    """Opaque replayable continuations with buffered overflow, TTL, and memory cap.

    Keeping leftovers, rather than slicing a page and returning X's next cursor,
    prevents loss when X returns more records than the requested result limit.
    Cursors intentionally expire after a restart; no account secrets are stored.
    """
    def __init__(self, ttl=900, max_entries=256, max_bytes=32 * 1024 * 1024):
        self.ttl, self.max_entries, self.max_bytes = ttl, max_entries, max_bytes
        self.records, self.bytes = OrderedDict(), 0
        self.lock = threading.RLock()

    def _prune(self):
        for token, (expires, _, body) in list(self.records.items()):
            if expires <= time.monotonic():
                self.bytes -= len(body)
                del self.records[token]

    def put(self, binding, state):
        body = json.dumps(state, ensure_ascii=False, separators=(",", ":")).encode()
        if len(body) > self.max_bytes:
            raise ResearchError("cursor_buffer_full", "A page is too large to retain; reduce the page size.")
        with self.lock:
            self._prune()
            while self.records and (len(self.records) >= self.max_entries or self.bytes + len(body) > self.max_bytes):
                _, (_, _, old) = self.records.popitem(last=False)
                self.bytes -= len(old)
            token = secrets.token_urlsafe(24)
            self.records[token] = (time.monotonic() + self.ttl, binding, body)
            self.bytes += len(body)
            return token

    def get(self, token, binding):
        with self.lock:
            self._prune()
            record = self.records.get(token)
            if not record:
                raise ResearchError("cursor_expired", "Continuation expired, was evicted, or belongs to another server process. Restart this search.")
            if record[1] != binding:
                raise ResearchError("cursor_mismatch", "Use this cursor with the same query, endpoint, and filters.")
            self.records.move_to_end(token)
            return json.loads(record[2])


class ResearchService:
    def __init__(self, transport=None, *, personal=False, cursors=None):
        self.transport = transport or XTransport()
        self.personal = personal
        self.cursors = cursors or CursorStore()
        # requests.Session + account rate state must not race across MCP calls.
        self.lock = threading.RLock()

    def page(self, operation, variables, *, kind="posts", limit=50, cursor=None, max_pages=3,
             field_toggles=None, exclude_ids=(), deadline=None):
        bounded(limit, 1, 200, "limit")
        bounded(max_pages, 1, 10, "max_pages")
        binding = hashlib.sha256(json.dumps([operation, variables, kind, field_toggles, sorted(exclude_ids)], sort_keys=True).encode()).hexdigest()
        state = self.cursors.get(cursor, binding) if cursor else {
            "pending": [], "upstream": None, "started": False, "seen": [], "visited": [],
        }
        seen, visited = set(state["seen"]), set(state["visited"])
        output, warnings, unavailable, branches = [], [], [], []
        pages, output_bytes, error = 0, 0, None
        stop = "visible_end"
        deadline = deadline or time.monotonic() + 40
        while True:
            while state["pending"] and len(output) < limit:
                item = state["pending"][0]
                size = len(json.dumps(item, ensure_ascii=False).encode())
                if size > 1024 * 1024:
                    raise ResearchError("item_too_large", "One result exceeds the 1 MiB output budget.", id=item.get("id"))
                if output_bytes + size > 1024 * 1024:
                    stop = "output_budget"
                    break
                output.append(state["pending"].pop(0))
                output_bytes += size
            if state["pending"] or len(output) >= limit:
                stop = "output_budget" if stop == "output_budget" else "limit"
                break
            if state["started"] and not state["upstream"]:
                break
            if pages >= max_pages or time.monotonic() >= deadline:
                stop = "page_budget" if pages >= max_pages else "time_budget"
                break
            current = state["upstream"] or "__first__"
            if current in visited:
                warnings.append({"code": "repeated_cursor", "message": "X repeated a cursor; automatic traversal stopped."})
                state["upstream"] = None
                stop = "repeated_cursor"
                break
            request = dict(variables)
            if state["upstream"]:
                request["cursor"] = state["upstream"]
            try:
                payload = self.transport.read(operation, request, field_toggles=field_toggles)
                parsed = timeline(payload)
            except ResearchError as exc:
                if not output:
                    raise
                error, stop = exc.as_dict(), "partial_error"
                break
            pages += 1
            visited.add(current)
            state["started"] = True
            state["upstream"] = parsed.next_cursor
            branches.extend(parsed.branches)
            unavailable.extend(parsed.unavailable)
            warnings.extend(payload.get("_research_warnings", []))
            for item in getattr(parsed, kind):
                if item["id"] in seen or item["id"] in exclude_ids:
                    continue
                seen.add(item["id"])
                item["retrieved_at"] = timestamp()
                state["pending"].append(item)
        # A stable but bounded recent-ID window prevents duplicate-page loops;
        # callers conducting huge archives should additionally deduplicate IDs.
        state["seen"] = sorted(seen)[-4000:]
        state["visited"] = sorted(visited)[-500:]
        has_more = bool(state["pending"] or state["upstream"] or not state["started"])
        token = self.cursors.put(binding, state) if has_more else None
        return {"items": output, "next_cursor": token, "has_more": has_more,
                "returned": len(output), "upstream_pages": pages, "stop_reason": stop,
                "coverage": COVERAGE, "warnings": warnings, "unavailable": unavailable,
                "branch_cursors": branches, "error": error}

    def search_x(self, query, product="Top", limit=50, cursor=None, max_pages=3):
        if product not in {"Top", "Latest", "Media"}:
            raise ResearchError("invalid_product", "Use Top, Latest, or Media. For accounts use search_users.")
        query = query_text(query)
        result = self.page("SearchTimeline", {"rawQuery": query, "count": 50, "querySource": "typed_query", "product": product},
                           limit=limit, cursor=cursor, max_pages=max_pages)
        return {"query": query, "product": product, **result}

    def search_many(self, queries, product="Top", limit_per_query=30, max_pages_per_query=2):
        if not isinstance(queries, list) or not 1 <= len(queries) <= 8:
            raise ResearchError("invalid_argument", "Provide 1–8 query variants.")
        queries = list(dict.fromkeys(query_text(q) for q in queries))
        bounded(limit_per_query, 1, 100, "limit_per_query")
        bounded(max_pages_per_query, 1, 3, "max_pages_per_query")
        if product not in {"Top", "Latest", "Media"}:
            raise ResearchError("invalid_product", "Use Top, Latest, or Media.")
        results, reports, deadline = OrderedDict(), [], time.monotonic() + 45
        halted = False
        for query in queries:
            if halted or time.monotonic() >= deadline:
                reports.append({"query": query, "status": "not_attempted", "reason": "budget_or_rate_limit"})
                continue
            try:
                page = self.page("SearchTimeline", {"rawQuery": query, "count": 50, "querySource": "typed_query", "product": product},
                                 limit=limit_per_query, max_pages=max_pages_per_query, deadline=deadline)
                for item in page.pop("items"):
                    tid = item["id"]
                    if tid not in results:
                        results[tid] = {**item, "matched_queries": []}
                    results[tid]["matched_queries"].append(query)
                reports.append({"query": query, "status": "partial" if page["error"] else "ok", **page})
                halted = bool(page["error"] and page["error"]["code"] in {"rate_limited", "account_or_bot_challenge", "authentication_required", "access_denied"})
                # Stop between query variants, retaining the completed page and
                # its cursor. One final page can put the batch just over 1 MiB.
                halted = halted or len(json.dumps(list(results.values()), ensure_ascii=False).encode()) >= 1024 * 1024
            except ResearchError as exc:
                reports.append({"query": query, "status": "error", "error": exc.as_dict()})
                halted = exc.code in {"rate_limited", "account_or_bot_challenge", "authentication_required", "access_denied"}
        return {"items": list(results.values()), "queries": reports, "coverage": COVERAGE,
                "ordering": "First occurrence in query order; not a cross-query relevance ranking."}

    def search_users(self, query, limit=30, cursor=None, max_pages=3):
        return self.page("SearchTimeline", {"rawQuery": query_text(query), "count": 50, "querySource": "typed_query", "product": "People"},
                         kind="users", limit=limit, cursor=cursor, max_pages=max_pages)

    def search_user_posts(self, username, query="", product="Latest", limit=50, cursor=None, max_pages=3):
        scoped = f"from:{handle(username)}"
        if query:
            scoped += f" ({query_text(query)})"
        return self.search_x(scoped, product, limit, cursor, max_pages)

    def search_quotes(self, reference, query="", limit=50, cursor=None, max_pages=3):
        tid = tweet_id(reference)
        # URL search is explicitly approximate; X's index need not expose every quote.
        result = self.search_x(f"url:{tid}" + (f" ({query_text(query)})" if query else ""), "Latest", limit, cursor, max_pages)
        result["warnings"].append({"code": "approximate_quote_search", "message": "URL matches may include non-quotes and omit unindexed quotes. Check quoted_post_id."})
        return result

    def search_conversation(self, reference, query="", limit=50, cursor=None, max_pages=3):
        tid = tweet_id(reference)
        result = self.search_x(f"conversation_id:{tid}" + (f" ({query_text(query)})" if query else ""), "Latest", limit, cursor, max_pages)
        result["warnings"].append({"code": "conversation_index", "message": "Pass the root conversation_id. This search is not a complete reply tree; cross-check get_replies."})
        return result

    def get_tweet(self, reference):
        tid = tweet_id(reference)
        payload = self.transport.read("TweetResultByRestId", {"tweetId": tid, "withCommunity": True,
                    "includePromotedContent": False, "withVoice": True}, field_toggles=ARTICLE_FLAGS)
        raw = dig(payload, "data", "tweetResult", "result") or dig(payload, "data", "tweetResultByRestId", "result")
        item = post(raw)
        if not item or item["id"] != tid:
            raise ResearchError("post_unavailable", "Post unavailable to this session or response format changed; this does not prove deletion.", id=tid)
        return {**item, "retrieved_at": timestamp(), "warnings": payload.get("_research_warnings", [])}

    def get_tweets(self, references):
        if not isinstance(references, list) or not 1 <= len(references) <= 10:
            raise ResearchError("invalid_argument", "Provide 1–10 post IDs or URLs.")
        ids = list(dict.fromkeys(tweet_id(value) for value in references))
        output, deadline, halted = [], time.monotonic() + 45, False
        for tid in ids:
            if halted or time.monotonic() >= deadline:
                output.append({"id": tid, "error": {"code": "not_attempted", "message": "Request budget or rate limit reached."}})
                continue
            try:
                output.append(self.get_tweet(tid))
            except ResearchError as exc:
                output.append({"id": tid, "error": exc.as_dict()})
                halted = exc.code in {"rate_limited", "account_or_bot_challenge", "authentication_required", "access_denied"}
        return {"items": output, "coverage": COVERAGE}

    def get_replies(self, reference, limit=50, cursor=None, max_pages=3, branch_cursor=None):
        tid = tweet_id(reference)
        variables = {"focalTweetId": tid, **DETAIL_FLAGS}
        if branch_cursor:
            if not isinstance(branch_cursor, str) or len(branch_cursor) > 4096:
                raise ResearchError("invalid_cursor", "Invalid X branch cursor.")
            variables["cursor"] = branch_cursor
        result = self.page("TweetDetail", variables, limit=limit, cursor=cursor, max_pages=max_pages,
                           field_toggles=ARTICLE_FLAGS, exclude_ids=(tid,))
        # TweetDetail may return ancestor context. Preserve it, but label it instead
        # of falsely claiming every returned post is a direct reply to the focal ID.
        for item in result["items"]:
            item["relation_to_focal"] = "direct_reply" if item.get("in_reply_to_id") == tid else "conversation_context_or_nested_reply"
        return {"focal_id": tid, **result}

    def get_user_profile(self, username):
        username = handle(username)
        payload = self.transport.read("UserByScreenName", {"screen_name": username})
        result = user(dig(payload, "data", "user", "result"))
        if not result:
            raise ResearchError("user_unavailable", "Profile unavailable to this session or response format changed.")
        return {**result, "retrieved_at": timestamp()}

    def get_user_tweets(self, username, limit=50, cursor=None, max_pages=3):
        profile = self.get_user_profile(username)
        return self.page("UserTweets", {"userId": profile["id"], "count": 50, "includePromotedContent": False,
            "withQuickPromoteEligibilityTweetFields": True, "withVoice": True},
            limit=limit, cursor=cursor, max_pages=max_pages, field_toggles=ARTICLE_FLAGS)

    def connections(self, kind, reference, limit=50, cursor=None, max_pages=3):
        mapping = {"followers": "Followers", "following": "Following", "likers": "Favoriters", "retweeters": "Retweeters"}
        if kind not in mapping:
            raise ResearchError("invalid_argument", "Unknown connection type.")
        variables = {"count": 50, "includePromotedContent": False}
        if kind in {"followers", "following"}:
            variables["userId"] = self.get_user_profile(reference)["id"]
        else:
            variables["tweetId"] = tweet_id(reference)
        return self.page(mapping[kind], variables, kind="users", limit=limit, cursor=cursor, max_pages=max_pages)

    def personal_timeline(self, kind, username=None, limit=50, cursor=None, max_pages=3):
        if not self.personal:
            raise ResearchError("personal_tools_disabled", "Personal account data is disabled. Enable --personal locally.")
        operations = {"home": "HomeTimeline", "following": "HomeLatestTimeline", "bookmarks": "Bookmarks",
                      "likes": "Likes", "notifications": "NotificationsTimeline"}
        if kind not in operations:
            raise ResearchError("invalid_argument", "Unknown personal timeline.")
        variables = {"count": 50, "includePromotedContent": False}
        if kind in {"home", "following"}:
            variables.update({"latestControlAvailable": True, "withCommunity": True})
            if kind == "home":
                variables["requestContext"] = "launch"
        elif kind == "likes":
            variables.update({"userId": self.get_user_profile(username)["id"], "withClientEventToken": False,
                              "withBirdwatchNotes": False, "withVoice": True, "withV2Timeline": True})
        elif kind == "notifications":
            variables["timeline_type"] = "All"
        result = self.page(operations[kind], variables, limit=limit, cursor=cursor, max_pages=max_pages)
        if kind in {"likes", "notifications"}:
            result["warnings"].append({"code": "restricted_or_partial_endpoint", "message": "X can restrict likes. Notifications expose embedded posts, not every notification event."})
        return result

    def get_thread(self, reference, max_posts=150, max_requests=12):
        bounded(max_posts, 1, 300, "max_posts")
        bounded(max_requests, 1, 25, "max_requests")
        calls, deadline = 0, time.monotonic() + 45
        errors, continuations, missing = [], [], []
        pool, scanned = OrderedDict(), set()
        limited, halted = False, False

        def budget():
            return not halted and calls < max_requests and time.monotonic() < deadline

        def lookup(tid):
            nonlocal calls
            calls += 1
            return self.get_tweet(tid)

        focal = lookup(tweet_id(reference))
        pool[focal["id"]] = focal
        root, author_id = focal, focal["author_id"]
        if not author_id:
            raise ResearchError("missing_author", "Cannot safely reconstruct an author thread without an author ID.")
        # Walk upwards from a middle post, but never cross into another author's thread.
        while root.get("in_reply_to_id") and root["in_reply_to_id"] not in pool:
            parent_id = root["in_reply_to_id"]
            if not budget() or len(pool) >= max_posts:
                missing.append(parent_id)
                limited = True
                break
            try:
                parent = lookup(parent_id)
            except ResearchError as exc:
                errors.append({"id": parent_id, **exc.as_dict()})
                missing.append(parent_id)
                halted = exc.code in {"rate_limited", "account_or_bot_challenge", "authentication_required", "access_denied"}
                break
            pool[parent_id] = parent
            if parent["author_id"] != author_id:
                break
            root = parent

        def connected():
            ids = {root["id"]}
            while True:
                new = {tid for tid, item in pool.items() if item["author_id"] == author_id and item.get("in_reply_to_id") in ids}
                if new <= ids:
                    return ids
                ids.update(new)

        queue = deque([(root["id"], None)])
        planned = {(root["id"], None)}
        while queue and budget() and len(pool) < max_posts:
            tid, raw_cursor = queue.popleft()
            if (tid, raw_cursor) in scanned:
                continue
            scanned.add((tid, raw_cursor))
            calls += 1
            variables = {"focalTweetId": tid, **DETAIL_FLAGS}
            if raw_cursor:
                variables["cursor"] = raw_cursor
            try:
                payload = self.transport.read("TweetDetail", variables, field_toggles=ARTICLE_FLAGS)
                parsed = timeline(payload)
            except ResearchError as exc:
                errors.append({"id": tid, **exc.as_dict()})
                continuations.append({"focal_id": tid, "cursor": raw_cursor, "reason": "retry_after_error"})
                if exc.code in {"rate_limited", "account_or_bot_challenge", "authentication_required", "access_denied"}:
                    break
                continue
            # Prioritise author posts before unrelated replies when the output cap fills.
            for item in sorted(parsed.posts, key=lambda p: p["author_id"] != author_id):
                if item["id"] not in pool and len(pool) >= max_posts:
                    limited = True
                    continue
                pool[item["id"]] = item
            errors.extend(parsed.unavailable)
            errors.extend(payload.get("_research_warnings", []))
            continuations.extend({"focal_id": tid, **branch} for branch in parsed.branches)
            if parsed.next_cursor:
                key = (tid, parsed.next_cursor)
                if key not in planned:
                    planned.add(key)
                    queue.append(key)
                else:
                    errors.append({"id": tid, "code": "repeated_cursor"})
            ids = connected()
            parent_ids = {pool[i].get("in_reply_to_id") for i in ids}
            # Inspect leaves before paging thousands of unrelated root replies.
            for child_id in sorted(ids - parent_ids, key=int):
                key = (child_id, None)
                if key not in planned and pool[child_id]["metrics"].get("replies") != 0:
                    planned.add(key)
                    queue.appendleft(key)
        if queue:
            limited = True
            continuations.extend({"focal_id": tid, "cursor": cur, "reason": "request_or_output_budget"} for tid, cur in queue)
        ids = connected()
        # IDs are monotonically time-ordered; parent links also remain available
        # when the author creates branches rather than one linear self-reply chain.
        author_posts = sorted((pool[i] for i in ids), key=lambda item: int(item["id"]))
        context = [item for tid, item in pool.items() if tid not in ids]
        return {"focal_id": focal["id"], "root_id": root["id"], "author_posts": author_posts,
                "context_posts": context, "missing_parent_ids": missing, "continuations": continuations,
                "requests_used": calls, "truncated": limited, "errors": errors,
                "visible_walk_finished": not (limited or errors or continuations or missing),
                "coverage": COVERAGE, "complete_archive": False}

    def get_media(self, reference):
        item = self.get_tweet(reference)
        return {"id": item["id"], "url": item["url"], "media": item["media"],
                "quoted_post": item.get("quoted_post"), "reposted_post": item.get("reposted_post")}

    def search(self, query):
        result = self.search_x(query, limit=30, max_pages=2)
        # Strict search/fetch compatibility interface. Advanced tools provide
        # continuation and coverage fields, which this minimal schema cannot.
        if result["error"]:
            raise ResearchError("partial_search", "Search was incomplete; use search_x to inspect results and continuation.")
        return {"results": [{"id": item["id"], "title": self.title(item), "url": item["url"]} for item in result["items"]]}

    @staticmethod
    def title(item):
        author = (item.get("author") or {}).get("username", "unknown")
        return f"@{author}: {item['text'][:180]}"

    def fetch(self, id):
        item = self.get_tweet(id)
        text = item["text"]
        if item.get("article"):
            text += "\n\nARTICLE: " + (item["article"].get("title") or "") + "\n" + (item["article"].get("text") or "")
        for kind in ("quoted_post", "reposted_post"):
            nested = item.get(kind)
            if nested:
                text += f"\n\n{kind.upper()} ({nested['url']}):\n{nested['text']}"
        return {"id": item["id"], "title": self.title(item), "url": item["url"], "text": text,
                "metadata": {key: value for key, value in item.items() if key != "text"}}
