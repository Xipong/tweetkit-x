import copy
import json
import time
from types import SimpleNamespace

import pytest

from tweetkit_x.research import CursorStore, ResearchService
from tweetkit_x.research_data import ResearchError, handle, post, timeline, tweet_id
from tweetkit_x.research_transport import XTransport, read_image
from tweetkit_x.research_server import EndpointGuard, endpoint_settings


def raw_user(uid="1", username="alice", modern=False):
    fields = {"screen_name": username, "name": username.title()}
    return {"__typename": "User", "rest_id": uid, "legacy": {"description": "bio", **({} if modern else fields)},
            "core": fields if modern else {}}


def raw_post(tid, author="1", parent=None, replies=0, quote=None, note=None):
    item = {"__typename": "Tweet", "rest_id": str(tid),
            "core": {"user_results": {"result": raw_user(author, "alice" if author == "1" else "bob")}},
            "legacy": {"id_str": str(tid), "full_text": f"post {tid}", "user_id_str": author,
                       "in_reply_to_status_id_str": str(parent) if parent is not None else None,
                       "conversation_id_str": "10", "reply_count": replies, "created_at": "date"}}
    if quote:
        item["quoted_status_result"] = {"result": quote}
        item["legacy"]["quoted_status_id_str"] = quote["rest_id"]
    if note:
        item["note_tweet"] = {"note_tweet_results": {"result": {"text": note}}}
    return item


def entry(item):
    return {"entryId": "tweet-" + item["rest_id"], "content": {"itemContent": {"tweet_results": {"result": item}}}}


def payload(items=(), cursor=None, users=(), extra=()):
    entries = [entry(item) for item in items]
    entries += [{"entryId": "user-" + u["rest_id"], "content": {"itemContent": {"user_results": {"result": u}}}} for u in users]
    if cursor:
        entries.append({"entryId": "cursor-bottom", "content": {"cursorType": "Bottom", "value": cursor}})
    entries.extend(extra)
    return {"data": {"timeline": {"instructions": [{"type": "TimelineAddEntries", "entries": entries}]}}}


class FakeTransport:
    def __init__(self, responses=(), route=None):
        self.responses, self.route, self.calls = list(responses), route, []

    def read(self, operation, variables, **kwargs):
        self.calls.append((operation, copy.deepcopy(variables), kwargs))
        result = self.route(operation, variables) if self.route else self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return copy.deepcopy(result)

    def status(self):
        return {"cookie_configured": True, "session_verified_online": False, "graphql_requests": len(self.calls)}

    def close(self):
        pass


@pytest.mark.parametrize("value,expected", [
    ("123", "123"), ("https://x.com/alice/status/123?s=20", "123"),
    ("https://twitter.com/alice/status/123/photo/1", "123"),
    ("https://x.com/i/web/status/123", "123"), ("https://x.com/i/status/123", "123"),
])
def test_post_identifiers(value, expected):
    assert tweet_id(value) == expected


@pytest.mark.parametrize("value", ["abc", "file:///etc/passwd", "http://x.com/a/status/1",
    "https://x.com.evil/a/status/1", "https://secret@x.com/a/status/1", "https://x.com:8080/a/status/1",
    "https://127.0.0.1/a/status/1", "https://x.com/a/status/not-id"])
def test_reject_arbitrary_urls(value):
    with pytest.raises(ResearchError):
        tweet_id(value)


def test_handle_validation():
    assert handle("@Alice_12") == "Alice_12"
    for invalid in ("alice OR from:bob", "алиса", "", "very_very_long_username"):
        with pytest.raises(ResearchError):
            handle(invalid)


def test_quotes_stay_nested_and_top_order_is_preserved():
    quote = raw_post(999, "2", note="FULL QUOTED NOTE")
    data = payload([raw_post(20, quote=quote), raw_post(10)])
    got = timeline(data)
    assert [p["id"] for p in got.posts] == ["20", "10"]
    assert got.posts[0]["quoted_post"]["text"] == "FULL QUOTED NOTE"
    assert got.posts[0]["quoted_post"]["author"]["username"] == "bob"
    assert got.users == []


def test_note_media_article_and_visibility_wrapper():
    item = raw_post(10, note="full long note")
    item["legacy"]["truncated"] = True
    item["legacy"]["extended_entities"] = {"media": [{"type": "photo", "media_url_https": "https://pbs.twimg.com/media/a.png", "ext_alt_text": "chart"}]}
    item["article"] = {"article_results": {"result": {"title": "Title", "plain_text": "full article"}}}
    got = post({"__typename": "TweetWithVisibilityResults", "tweet": item})
    assert got["text"] == "full long note" and not got["text_may_be_truncated"]
    assert got["media"][0]["alt_text"] == "chart"
    assert got["article"]["text"] == "full article" and not got["article"]["preview_only"]


def test_modules_show_more_and_root_cursor_are_distinct():
    module = {"entryId": "conversationthread-10", "content": {"items": [
        {"item": {"itemContent": {"tweet_results": {"result": raw_post(20)}}}},
        {"item": {"itemContent": {"cursorType": "Bottom", "value": "branch"}}},
    ]}}
    got = timeline(payload(cursor="main", extra=[module]))
    assert got.next_cursor == "main"
    assert got.branches[0]["cursor"] == "branch"
    assert [p["id"] for p in got.posts] == ["20"]


def test_replacement_entries_promoted_and_tombstone():
    ad = entry(raw_post(30))
    ad["content"]["itemContent"]["promotedMetadata"] = {"advertiser": "x"}
    data = payload([raw_post(10)], extra=[ad])
    data["data"]["timeline"]["instructions"].append({"type": "TimelineReplaceEntry", "entry": entry(raw_post(20))})
    data["data"]["timeline"]["instructions"].append({"entries": [{"entryId": "tweet-40", "content": {"itemContent": {"tweet_results": {"result": {"__typename": "TweetTombstone"}}}}}]})
    got = timeline(data)
    assert [p["id"] for p in got.posts] == ["10", "20"]
    assert len(got.unavailable) == 1


def test_schema_change_is_not_empty_search():
    with pytest.raises(ResearchError, match="not evidence"):
        timeline({"data": {"new_schema": []}})
    assert timeline(payload()).posts == []


def test_buffered_pagination_never_drops_overflow_and_cursor_is_replayable():
    fake = FakeTransport([payload([raw_post(10), raw_post(20), raw_post(30)], "page2"), payload([raw_post(40)])])
    service = ResearchService(fake)
    one = service.search_x("query", limit=1)
    two = service.search_x("query", limit=1, cursor=one["next_cursor"])
    again = service.search_x("query", limit=1, cursor=one["next_cursor"])
    assert [p["id"] for p in two["items"]] == ["20"] == [p["id"] for p in again["items"]]
    assert len(fake.calls) == 1
    rest = service.search_x("query", limit=10, cursor=two["next_cursor"])
    assert [p["id"] for p in rest["items"]] == ["30", "40"]
    assert fake.calls[-1][1]["cursor"] == "page2"
    assert not rest["has_more"]


def test_cursor_query_mismatch_and_expiration():
    store = CursorStore(ttl=900, max_entries=1)
    token = store.put("a", {"x": 1})
    with pytest.raises(ResearchError) as exc:
        store.get(token, "b")
    assert exc.value.code == "cursor_mismatch"
    store.put("b", {})
    with pytest.raises(ResearchError) as exc:
        store.get(token, "a")
    assert exc.value.code == "cursor_expired"
    short = CursorStore(ttl=-1)
    token = short.put("a", {})
    with pytest.raises(ResearchError):
        short.get(token, "a")


def test_empty_and_duplicate_pages_do_not_prematurely_end_search():
    fake = FakeTransport([payload([], "a"), payload([raw_post(10)], "b"), payload([raw_post(10)], "c"), payload([raw_post(20)])])
    got = ResearchService(fake).search_x("needle", max_pages=5)
    assert [p["id"] for p in got["items"]] == ["10", "20"]
    assert len(fake.calls) == 4


def test_repeated_cursor_terminates_incomplete():
    fake = FakeTransport([payload([raw_post(10)], "repeat"), payload([raw_post(20)], "repeat")])
    got = ResearchService(fake).search_x("needle", max_pages=10)
    assert got["stop_reason"] == "repeated_cursor"
    assert got["warnings"][0]["code"] == "repeated_cursor"
    assert len(fake.calls) == 2


def test_page_budget_continues_and_filter_is_bound():
    fake = FakeTransport([payload([raw_post(10)], "a"), payload([raw_post(20)])])
    service = ResearchService(fake)
    got = service.search_x("q", max_pages=1)
    assert got["stop_reason"] == "page_budget" and got["has_more"]
    with pytest.raises(ResearchError):
        service.search_x("different", cursor=got["next_cursor"])
    with pytest.raises(ResearchError):
        service.search_x("q", product="Latest", cursor=got["next_cursor"])
    assert service.search_x("q", cursor=got["next_cursor"])["items"][0]["id"] == "20"


def test_partial_error_preserves_results_and_resume():
    fake = FakeTransport([payload([raw_post(10)], "a"), ResearchError("rate_limited", "wait", retry_after=60), payload([raw_post(20)])])
    service = ResearchService(fake)
    got = service.search_x("q")
    assert got["items"][0]["id"] == "10" and got["error"]["code"] == "rate_limited"
    assert got["next_cursor"]
    assert service.search_x("q", cursor=got["next_cursor"])["items"][0]["id"] == "20"


def test_batch_provenance_and_resume_with_search_x():
    fake = FakeTransport([payload([raw_post(10), raw_post(20)]), payload([raw_post(10), raw_post(30)])])
    service = ResearchService(fake)
    got = service.search_many(["one", "two"], limit_per_query=1)
    assert len(got["items"]) == 1 and got["items"][0]["matched_queries"] == ["one", "two"]
    token = got["queries"][0]["next_cursor"]
    assert service.search_x("one", cursor=token)["items"][0]["id"] == "20"


def test_batch_stops_after_auth_or_rate_failure():
    fake = FakeTransport([ResearchError("rate_limited", "wait")])
    got = ResearchService(fake).search_many(["one", "two"])
    assert got["queries"][0]["status"] == "error"
    assert got["queries"][1]["status"] == "not_attempted"
    assert len(fake.calls) == 1


def test_people_search_extracts_users_only():
    fake = FakeTransport([payload(users=[raw_user(modern=True)])])
    got = ResearchService(fake).search_users("Alice")
    assert got["items"][0]["username"] == "alice" and fake.calls[0][1]["product"] == "People"


@pytest.mark.parametrize("bad", [0, -1, 201, True, "100"])
def test_invalid_page_limit(bad):
    with pytest.raises(ResearchError):
        ResearchService(FakeTransport()).search_x("q", limit=bad)


def test_thread_from_middle_preserves_author_chain_and_other_context():
    records = {"10": raw_post(10, replies=1), "20": raw_post(20, parent=10, replies=1),
               "30": raw_post(30, parent=20), "40": raw_post(40, author="2", parent=10)}
    def route(op, variables):
        if op == "TweetResultByRestId":
            return {"data": {"tweetResult": {"result": records[variables["tweetId"]]}}}
        return payload(list(records.values()))
    got = ResearchService(FakeTransport(route=route)).get_thread("20")
    assert [p["id"] for p in got["author_posts"]] == ["10", "20", "30"]
    assert [p["id"] for p in got["context_posts"]] == ["40"]
    assert got["root_id"] == "10" and got["requests_used"] == 3
    assert not got["complete_archive"]


def test_thread_expands_author_leaf_before_unrelated_pagination():
    root, second, third = raw_post(10, replies=1), raw_post(20, parent=10, replies=1), raw_post(30, parent=20)
    def route(op, variables):
        if op == "TweetResultByRestId":
            return {"data": {"tweetResult": {"result": root}}}
        if variables["focalTweetId"] == "20":
            return payload([second, third])
        return payload([root, second], "root-more")
    fake = FakeTransport(route=route)
    got = ResearchService(fake).get_thread("10", max_requests=3)
    assert fake.calls[2][1]["focalTweetId"] == "20"
    assert got["author_posts"][-1]["id"] == "30" and got["truncated"]
    assert got["continuations"]


def test_thread_reports_budget_and_missing_parent():
    fake = FakeTransport([{"data": {"tweetResult": {"result": raw_post(20, parent=10)}}}])
    got = ResearchService(fake).get_thread("20", max_requests=1)
    assert got["missing_parent_ids"] == ["10"] and got["truncated"]
    assert len(fake.calls) == 1


def test_thread_does_not_cross_another_authors_parent():
    fake = FakeTransport([{"data": {"tweetResult": {"result": raw_post(20, parent=10)}}},
                          {"data": {"tweetResult": {"result": raw_post(10, author="2")}}}, payload([raw_post(20, parent=10)])])
    got = ResearchService(fake).get_thread("20")
    assert got["root_id"] == "20"
    assert [p["id"] for p in got["context_posts"]] == ["10"]


def test_reply_context_and_branch_forwarding():
    fake = FakeTransport([payload([raw_post(10), raw_post(20, parent=10), raw_post(30, parent=20)])])
    got = ResearchService(fake).get_replies("20", branch_cursor="show-more")
    assert [p["id"] for p in got["items"]] == ["10", "30"]
    assert got["items"][1]["relation_to_focal"] == "direct_reply"
    assert fake.calls[0][1]["cursor"] == "show-more"


def test_unavailable_tweet_is_error_not_empty_text():
    with pytest.raises(ResearchError) as exc:
        ResearchService(FakeTransport([{"data": {"tweetResult": {"result": {"__typename": "TweetTombstone"}}}}])).get_tweet("10")
    assert exc.value.code == "post_unavailable"


def test_personal_tools_are_opt_in():
    with pytest.raises(ResearchError) as exc:
        ResearchService(FakeTransport()).personal_timeline("bookmarks")
    assert exc.value.code == "personal_tools_disabled"


def test_compatibility_search_fetch_shapes():
    fake = FakeTransport([payload([raw_post(10)]), {"data": {"tweetResult": {"result": raw_post(10, quote=raw_post(20))}}}])
    service = ResearchService(fake)
    got = service.search("q")
    assert set(got) == {"results"} and set(got["results"][0]) == {"id", "title", "url"}
    full = service.fetch("10")
    assert set(full) == {"id", "title", "url", "text", "metadata"}
    assert "post 20" in full["text"]


class Response:
    def __init__(self, data=None, status=200, headers=None, content=b"{}"):
        self.data, self.status_code, self.headers, self.content = data or {}, status, headers or {}, content
        self.closed = False

    def json(self):
        return self.data

    def close(self):
        self.closed = True

    def iter_content(self, size):
        yield self.content


class Session:
    def __init__(self, response):
        self.response, self.calls = response, []

    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.response

    def get(self, *args, **kwargs):
        return self.request(*args, **kwargs)

    def close(self):
        pass


def transport(response, headers_fn=None):
    session = Session(response)
    backend = SimpleNamespace(_session=session, _headers=headers_fn or (lambda *a: {"cookie": "PRIVATE"}))
    return XTransport(backend, query_ids={"SearchTimeline": "qid", "HomeTimeline": "qid"}, features={}, min_interval=0), session


def test_transport_refuses_mutation_even_with_upstream_client():
    client, session = transport(Response())
    with pytest.raises(ResearchError) as exc:
        client.read("CreateTweet", {})
    assert exc.value.code == "operation_not_allowed" and not session.calls


def test_transport_get_and_post_read_shapes_and_no_redirect():
    client, session = transport(Response(payload()))
    client.read("SearchTimeline", {"rawQuery": "needle"})
    args, kw = session.calls[0]
    assert args[0] == "GET" and kw["allow_redirects"] is False
    assert json.loads(kw["params"]["variables"])["rawQuery"] == "needle"
    client.read("HomeTimeline", {"count": 20})
    assert session.calls[1][0][0] == "POST"
    assert json.loads(session.calls[1][1]["data"])["variables"] == {"count": 20}


def test_transport_rate_limit_no_retry():
    client, session = transport(Response(status=429, headers={"retry-after": "42"}))
    with pytest.raises(ResearchError) as exc:
        client.read("SearchTimeline", {})
    assert exc.value.code == "rate_limited" and exc.value.details["retry_after"] == 42
    assert len(session.calls) == 1


@pytest.mark.parametrize("status", [301, 302, 400, 401, 403, 404, 500])
def test_transport_http_failures_are_explicit(status):
    client, _ = transport(Response(status=status))
    with pytest.raises(ResearchError):
        client.read("SearchTimeline", {})


def test_transport_exception_never_leaks_cookies():
    def fail(*args):
        raise RuntimeError("auth_token=SUPERSECRET; /home/private/cookie.txt")
    client, _ = transport(Response(), headers_fn=fail)
    with pytest.raises(ResearchError) as exc:
        client.read("SearchTimeline", {})
    assert "SUPERSECRET" not in json.dumps(exc.value.as_dict())
    assert "cookie.txt" not in str(exc.value)


def test_graphql_errors_and_partial_data():
    for code, expected in [(88, "rate_limited"), (89, "authentication_required"), (344, "account_or_bot_challenge")]:
        client, _ = transport(Response({"errors": [{"code": code, "message": "SECRET"}]}))
        with pytest.raises(ResearchError) as exc:
            client.read("SearchTimeline", {})
        assert exc.value.code == expected and "SECRET" not in str(exc.value)
    data = payload([raw_post(10)])
    data["errors"] = [{"code": 999, "message": "SECRET"}]
    client, _ = transport(Response(data))
    assert client.read("SearchTimeline", {})["_research_warnings"][0]["upstream_codes"] == [999]


@pytest.mark.parametrize("url", ["file:///tmp/a", "http://pbs.twimg.com/a", "https://pbs.twimg.com.evil/a", "https://127.0.0.1/a", "https://secret@pbs.twimg.com/a", "https://pbs.twimg.com:444/a"])
def test_image_ssrf_rejected_without_request(url):
    session = Session(Response())
    with pytest.raises(ResearchError):
        read_image(url, session=session)
    assert not session.calls


def test_image_body_limit_mime_and_no_credentials():
    png = b"\x89PNG\r\n\x1a\n" + b"test"
    session = Session(Response(headers={"content-type": "image/png"}, content=png))
    assert read_image("https://pbs.twimg.com/media/test.png", session=session) == (png, "image/png")
    assert session.calls[0][1]["allow_redirects"] is False and "headers" not in session.calls[0][1]
    with pytest.raises(ResearchError) as exc:
        read_image("https://pbs.twimg.com/a.png", session=session, max_bytes=2)
    assert exc.value.code == "media_too_large"
    for response in [Response(status=302, headers={"content-type": "image/png"}), Response(headers={"content-type": "text/html"}), Response(headers={"content-type": "image/png"}, content=b"not png")]:
        with pytest.raises(ResearchError):
            read_image("https://pbs.twimg.com/a", session=Session(response))


def test_public_http_requires_secret_and_exact_hostname():
    assert endpoint_settings()["path"] == "/mcp"
    for kwargs in ({"host": "0.0.0.0"}, {"public_host": "example.com"}, {"public_host": "https://example.com"}, {"path_key": "short"}):
        with pytest.raises(ResearchError):
            endpoint_settings(**kwargs)
    got = endpoint_settings(public_host="example.com", path_key="A" * 43)
    assert got["path"] == "/" + "A" * 43 + "/mcp" and "example.com" in got["allowed_hosts"]


def test_endpoint_guard_returns_404_without_secret_redirect():
    import asyncio
    events = []
    async def app(scope, receive, send):
        raise AssertionError("wrong path must never reach SDK")
    async def send(message):
        events.append(message)
    asyncio.run(EndpointGuard(app, "/SECRET/mcp")({"type": "http", "path": "/mcp"}, None, send))
    assert events[0]["status"] == 404 and "SECRET" not in str(events)


def test_scoped_queries_group_or_terms():
    fake = FakeTransport([payload(), payload(), payload()])
    service = ResearchService(fake)
    service.search_user_posts("alice", "one OR two")
    service.search_quotes("10", "one OR two")
    service.search_conversation("10", "one OR two")
    assert [v["rawQuery"] for _, v, _ in fake.calls] == [
        "from:alice (one OR two)", "url:10 (one OR two)", "conversation_id:10 (one OR two)"]


def test_parent_rate_limit_stops_thread_requests():
    fake = FakeTransport([{ "data": {"tweetResult": {"result": raw_post(20, parent=10)}}},
                          ResearchError("rate_limited", "wait")])
    got = ResearchService(fake).get_thread("20")
    assert len(fake.calls) == 2 and got["errors"][0]["code"] == "rate_limited"
    assert got["truncated"] and got["continuations"]
