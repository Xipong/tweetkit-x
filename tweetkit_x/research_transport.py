"""Allowlisted read-only X transport; reuse upstream authentication, not mutations."""
from __future__ import annotations

import json
import os
import re
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

from .research_data import ResearchError, integer

READ_OPERATIONS = frozenset({
    "SearchTimeline", "TweetResultByRestId", "TweetDetail", "UserByScreenName", "UserTweets",
    "Followers", "Following", "Favoriters", "Retweeters", "HomeTimeline", "HomeLatestTimeline",
    "Bookmarks", "Likes", "NotificationsTimeline",
})
POST_READS = frozenset({"HomeTimeline", "HomeLatestTimeline"})
PROFILE_FEATURES = {
    "hidden_profile_subscriptions_enabled": True,
    "subscriptions_verification_info_is_identity_verified_enabled": True,
    "subscriptions_verification_info_verified_since_enabled": True,
    "highlights_tweets_tab_ui_enabled": True,
    "responsive_web_twitter_article_notes_tab_enabled": True,
    "subscriptions_feature_can_gift_premium": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
}


def _overrides(env_name, boolean=False):
    path = os.environ.get(env_name)
    if not path:
        return {}
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(data, dict) or len(data) > 300:
            raise ValueError
        for key, value in data.items():
            if not re.fullmatch(r"[A-Za-z0-9_]{1,150}", key):
                raise ValueError
            if boolean:
                if not isinstance(value, bool):
                    raise ValueError
            elif key not in READ_OPERATIONS or not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", value):
                raise ValueError
        return data
    except (OSError, ValueError, TypeError):
        raise ResearchError("configuration_error", f"Invalid local {env_name} JSON file.") from None


def make_backend(timeout, impersonate=False):
    """Load cookies locally. MCP tools never accept credentials or local paths."""
    from .client import TweetKit
    from bs4 import BeautifulSoup
    from x_client_transaction import ClientTransaction
    from x_client_transaction.utils import get_ondemand_file_url

    class ReadSession(TweetKit):
        def _transaction(self):
            if self._ct is not None:
                return self._ct
            # Bounded alternative to upstream's unbounded migration helper. No
            # cookies sent to assets, and no redirect can carry an account token.
            for url in ("https://x.com/i/jf/", "https://x.com/"):
                response = self._session.get(url, timeout=self.timeout, allow_redirects=False)
                if response.status_code != 200:
                    continue
                home = BeautifulSoup(response.text, "html.parser")
                try:
                    asset = get_ondemand_file_url(home)
                except (AttributeError, ValueError, KeyError):
                    continue
                parsed = urlsplit(asset)
                if parsed.scheme != "https" or parsed.hostname != "abs.twimg.com" or parsed.port not in (None, 443) or parsed.username:
                    continue
                js = self._session.get(asset, timeout=self.timeout, allow_redirects=False)
                if js.status_code == 200:
                    self._ct = ClientTransaction(home, BeautifulSoup(js.text, "html.parser"))
                    return self._ct
            raise ResearchError("transaction_bootstrap_failed", "X changed its bootstrap page or blocked it. Update transaction-id support; do not keep retrying.")

    backend = ReadSession(timeout=timeout)
    if impersonate:
        try:
            from curl_cffi import requests as chrome_requests
        except ImportError:
            raise ResearchError("configuration_error", "Install the browser extra for curl_cffi impersonation.") from None
        backend._session.close()
        backend._session = chrome_requests.Session(impersonate="chrome")
    return backend


class XTransport:
    def __init__(self, backend=None, *, query_ids=None, features=None, timeout=15, min_interval=0.75, impersonate=False):
        self.backend = backend
        self.query_ids, self.features = query_ids, features
        self.timeout, self.min_interval, self.impersonate = timeout, min_interval, impersonate
        self.lock = threading.RLock()
        self.last_request = 0.0
        self.rate_limit = {}
        self.requests = 0

    def _ready(self):
        if self.query_ids is None:
            from . import constants as C
            self.query_ids = {k: v for k, v in C.QUERY_IDS.items() if k in READ_OPERATIONS}
            self.query_ids.update(_overrides("TWEETKIT_QUERY_IDS_FILE"))
            self.features = {**C.USER_TWEETS_FEATURES, **_overrides("TWEETKIT_FEATURES_FILE", boolean=True)}
        if self.backend is None:
            try:
                self.backend = make_backend(self.timeout, self.impersonate)
            except ResearchError:
                raise
            except Exception:
                raise ResearchError("authentication_required", "Configure the X cookie locally (TWEETKIT_COOKIE_FILE or tweetkit import). Do not paste credentials into chat.") from None

    def read(self, operation, variables, *, field_toggles=None, feature_overrides=None):
        if operation not in READ_OPERATIONS:
            raise ResearchError("operation_not_allowed", "Only allowlisted read operations are available.")
        with self.lock:
            self._ready()
            qid = self.query_ids.get(operation)
            if not qid:
                raise ResearchError("unsupported_operation", "No query ID configured for this read operation.", operation=operation)
            reset = self.rate_limit.get(operation, {})
            if reset.get("remaining") == 0 and (reset.get("reset") or 0) > time.time():
                raise ResearchError("rate_limited", "Wait for the X endpoint quota to reset.", retry_after=max(1, int(reset["reset"] - time.time())))
            pause = self.min_interval - (time.monotonic() - self.last_request)
            if pause > 0:
                time.sleep(pause)
            path = f"/i/api/graphql/{qid}/{operation}"
            method = "POST" if operation in POST_READS else "GET"
            features = {**(self.features or {}), **(feature_overrides or {})}
            if operation == "UserByScreenName":
                features.update(PROFILE_FEATURES)
            if operation == "Bookmarks":
                features["graphql_timeline_v2_bookmark_timeline"] = True
            params = {"variables": json.dumps(variables), "features": json.dumps(features)}
            if field_toggles is not None:
                params["fieldToggles"] = json.dumps(field_toggles)
            response = None
            try:
                headers = self.backend._headers(method, path)
                if self.impersonate:
                    for key in ("user-agent", "sec-ch-ua", "sec-ch-ua-platform", "sec-ch-ua-mobile"):
                        headers.pop(key, None)
                kwargs = {"params": params}
                if method == "POST":
                    headers["content-type"] = "application/json"
                    body = {"variables": variables, "features": features, "queryId": qid}
                    if field_toggles is not None:
                        body["fieldToggles"] = field_toggles
                    kwargs = {"data": json.dumps(body)}
                self.requests += 1
                self.last_request = time.monotonic()
                response = self.backend._session.request(method, "https://x.com" + path,
                    headers=headers, timeout=self.timeout, allow_redirects=False, **kwargs)
                self.rate_limit[operation] = {
                    "remaining": integer(response.headers.get("x-rate-limit-remaining")),
                    "reset": integer(response.headers.get("x-rate-limit-reset")),
                    "limit": integer(response.headers.get("x-rate-limit-limit")),
                }
                status = response.status_code
                if status == 429:
                    wait = integer(response.headers.get("retry-after"))
                    reset_at = self.rate_limit[operation]["reset"]
                    raise ResearchError("rate_limited", "X rate limit reached; do not retry immediately.",
                        retry_after=max(1, wait or int((reset_at or time.time() + 60) - time.time())))
                if status in (401, 403):
                    raise ResearchError("access_denied", "X rejected the session or restricted this endpoint.", status=status)
                if status != 200:
                    raise ResearchError("upstream_http_error", "X request failed. The endpoint/query ID may have changed.", operation=operation, status=status)
                if len(response.content) > 16 * 1024 * 1024:
                    raise ResearchError("response_too_large", "X response exceeded the 16 MiB safety bound.")
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ResearchError("schema_changed", "X returned a non-object JSON response.")
                errors = payload.get("errors") or []
                if errors:
                    codes = [integer(e.get("code")) for e in errors if isinstance(e, dict)]
                    if 88 in codes:
                        raise ResearchError("rate_limited", "X endpoint quota exhausted.", retry_after=60)
                    if any(c in (32, 89, 135, 215) for c in codes):
                        raise ResearchError("authentication_required", "Refresh the X session locally.")
                    if any(c in (326, 344) for c in codes):
                        raise ResearchError("account_or_bot_challenge", "X requires a session/account check. Stop automated retries.")
                    if not payload.get("data"):
                        raise ResearchError("upstream_graphql_error", "X rejected this query. Check query IDs, feature flags, and endpoint availability.", codes=codes)
                    payload["_research_warnings"] = [{"code": "partial_graphql_data", "upstream_codes": codes}]
                return payload
            except ResearchError:
                raise
            except Exception:
                raise ResearchError("upstream_connection_error", "Could not read X. Check connectivity, proxy, and transaction-id support; no automatic retry was performed.") from None
            finally:
                if response is not None:
                    response.close()

    def status(self):
        try:
            self._ready()
            configured = True
        except ResearchError:
            configured = False
        return {"cookie_configured": configured, "session_verified_online": False,
                "graphql_requests": self.requests, "rate_limits": self.rate_limit,
                "transport": "curl_cffi" if self.impersonate else "requests"}

    def close(self):
        if self.backend is not None:
            self.backend._session.close()


def read_image(url, *, session=None, max_bytes=5 * 1024 * 1024):
    """Only X's fixed image CDN, no account headers, no redirects, bounded body."""
    import requests
    try:
        parsed = urlsplit(url)
        valid = parsed.scheme == "https" and parsed.hostname == "pbs.twimg.com" and not parsed.username and parsed.port in (None, 443)
    except (ValueError, TypeError):
        valid = False
    if not valid:
        raise ResearchError("unsafe_media_url", "Only HTTPS images on pbs.twimg.com can be loaded.")
    owned = session is None
    session = session or requests.Session()
    response = None
    try:
        response = session.get(url, stream=True, timeout=15, allow_redirects=False)
        mime = response.headers.get("content-type", "").split(";", 1)[0].lower()
        if response.status_code != 200 or mime not in {"image/jpeg", "image/png", "image/webp"}:
            raise ResearchError("media_unavailable", "The image CDN returned an unsupported response.")
        data = bytearray()
        for chunk in response.iter_content(65536):
            data.extend(chunk)
            if len(data) > max_bytes:
                raise ResearchError("media_too_large", "Image exceeds 5 MiB; use the source URL instead.")
        signatures = {"image/jpeg": bytes(data[:3]) == b"\xff\xd8\xff",
                      "image/png": bytes(data[:8]) == b"\x89PNG\r\n\x1a\n",
                      "image/webp": bytes(data[:4]) == b"RIFF" and bytes(data[8:12]) == b"WEBP"}
        if not signatures[mime]:
            raise ResearchError("invalid_image", "Image body does not match its media type.")
        return bytes(data), mime
    except ResearchError:
        raise
    except Exception:
        raise ResearchError("media_unavailable", "Could not download the image.") from None
    finally:
        if response is not None:
            response.close()
        if owned:
            session.close()
