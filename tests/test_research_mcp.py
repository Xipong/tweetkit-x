"""Real SDK/ASGI protocol tests; no X account or outbound network required."""
import json

import pytest

pytest.importorskip("mcp")
httpx = pytest.importorskip("httpx")
jsonschema = pytest.importorskip("jsonschema")

from tweetkit_x.research import ResearchService
from tweetkit_x.research_server import EndpointGuard, create_server
from test_research import FakeTransport, payload, raw_post


@pytest.fixture
def anyio_backend():
    return "asyncio"


def service(personal=False):
    item = raw_post(10)
    item["legacy"]["extended_entities"] = {"media": [{"type": "photo", "media_url_https": "https://pbs.twimg.com/media/test.png"}]}
    def route(operation, variables):
        if operation == "TweetResultByRestId":
            return {"data": {"tweetResult": {"result": item}}}
        return payload([item])
    return ResearchService(FakeTransport(route=route), personal=personal)


@pytest.mark.anyio
@pytest.mark.parametrize("personal,count", [(False, 21), (True, 25)])
async def test_real_sdk_discovery_and_schemas(personal, count):
    svc = service(personal)
    server = create_server(svc)
    tools = {t.name: t for t in await server.list_tools()}
    assert len(tools) == count and not svc.transport.calls
    assert {"search", "fetch", "search_many", "get_thread", "get_image"} <= tools.keys()
    assert not ({"post_tweet", "delete_tweet", "set_session_cookie", "import_cookie_from_file"} & tools.keys())
    for tool in tools.values():
        assert tool.annotations.readOnlyHint and tool.annotations.idempotentHint
        assert not tool.annotations.destructiveHint
        assert "args" not in tool.inputSchema.get("properties", {})
        jsonschema.Draft202012Validator.check_schema(tool.inputSchema)
    assert tools["search"].inputSchema["required"] == ["query"]
    assert tools["fetch"].inputSchema["required"] == ["id"]
    assert "results" in tools["search"].outputSchema["properties"]
    assert set(tools["fetch"].outputSchema["required"]) == {"id", "title", "url", "text", "metadata"}


@pytest.mark.anyio
async def test_real_streamable_http_roundtrip_and_protected_route(monkeypatch):
    monkeypatch.setattr("tweetkit_x.research_server.read_image", lambda url: (b"\x89PNG\r\n\x1a\nfixture", "image/png"))
    svc = service()
    key = "A" * 43
    path = "/" + key + "/mcp"
    server = create_server(svc, public_host="research.example.com", path_key=key)
    app = EndpointGuard(server.streamable_http_app(), path)
    headers = {"accept": "application/json, text/event-stream", "content-type": "application/json"}
    async with server.session_manager.run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://research.example.com", headers=headers) as client:
            wrong = await client.post("/mcp", json={})
            assert wrong.status_code == 404 and key not in wrong.text
            async def rpc(method, params, identity=1):
                response = await client.post(path, json={"jsonrpc": "2.0", "id": identity, "method": method, "params": params})
                assert response.status_code == 200, response.text
                body = response.json()
                assert "error" not in body, body
                return body["result"]
            init = await rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "research-tests", "version": "1"}})
            assert init["serverInfo"]["name"] == "X Research"
            tools = {t["name"]: t for t in (await rpc("tools/list", {}, 2))["tools"]}
            for name, arguments in [("search", {"query": "test"}), ("fetch", {"id": "10"})]:
                got = await rpc("tools/call", {"name": name, "arguments": arguments}, 3)
                assert not got.get("isError")
                jsonschema.validate(got["structuredContent"], tools[name]["outputSchema"])
                assert json.loads(got["content"][0]["text"]) == got["structuredContent"]
            bad = await rpc("tools/call", {"name": "get_tweet", "arguments": {"reference": "file:///secret"}}, 4)
            assert bad["isError"] and "invalid_id" in bad["content"][0]["text"]
            image = await rpc("tools/call", {"name": "get_image", "arguments": {"reference": "10"}}, 5)
            assert not image.get("isError")
            assert image["content"][1]["type"] == "image" and image["content"][1]["mimeType"] == "image/png"
            untrusted = await client.post(path, headers={"host": "evil.example"}, json={"jsonrpc": "2.0", "id": 7, "method": "tools/list", "params": {}})
            assert untrusted.status_code in (400, 403, 421)
