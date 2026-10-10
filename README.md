# X Research / tweetkit-x

Read-only research connector for ChatGPT, in Xipong's fork of
[nsozturk/tweetkit-x](https://github.com/nsozturk/tweetkit-x).

**Start here: [установка и подключение к ChatGPT](CHATGPT_SETUP_RU.md).**

Run **`tweetkit-research`**, not the legacy `tweetkit-mcp`. The new connector
exposes 21 research tools (25 with explicit personal-data opt-in), no account
mutations and no model-facing credential or local-file import tools.

Search with multiple query variants, preserve X's Top ranking, paginate without
losing page overflow, reconstruct author threads, inspect replies and references,
trace related accounts, retain full available notes/quotes/media, and return actual
MCP image content for screenshots. Supports private stdio tunnels and Streamable
HTTP; optional curl_cffi browser TLS impersonation does not launch a browser.

```bash
python -m pip install -e '.[research,browser]'
# Configure TWEETKIT_COOKIE_FILE locally; never paste session cookies into chat.
tweetkit-research --transport stdio --impersonate
```

The HTTP endpoint is private-by-default. Public deployment requires an explicitly
configured secret capability URL or an external authentication gateway; see the
setup guide before exposing anything. No built-in OAuth provider is claimed.

Tests use synthetic X responses plus real-SDK protocol tests. They do not certify
that X's current private endpoints accept your account. Coverage, quota errors,
missing posts and incomplete threads are reported explicitly, never as proof of
an exhaustive archive.

The original library, CLI and write-capable MCP remain for compatibility.
Their documentation is preserved in [README.upstream.md](README.upstream.md).
The MIT license and original author's attribution remain unchanged.
