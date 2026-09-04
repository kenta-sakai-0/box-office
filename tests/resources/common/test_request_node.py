import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from box_office.resources.common.proxies import Proxy
from box_office.resources.common.RequestNode import RequestNode


def make_context():
    context = Mock()
    context.log = Mock()
    return context


def make_databricks():
    databricks = Mock()
    databricks.upload_raw_jsonl = AsyncMock()
    return databricks


class FakeResponse:
    def __init__(self, status_code=200, content=b'{"ok": true}'):
        self.status_code = status_code
        self.content = content


class FakeAsyncSession:
    """Stands in for curl_cffi.requests.AsyncSession as an async context manager."""

    def __init__(self, responses=None, **kwargs):
        self._responses = list(responses or [])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def get(self, **kwargs):
        if self._responses:
            item = self._responses.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return FakeResponse(200)


class WorkingProxyClient:
    async def get(self):
        return Proxy(url="http://working-proxy")

    async def release(self, p):
        pass

    async def releaseBroken(self, p, cooldown_seconds):
        pass


class FailingProxyClient:
    """Mimics a fully exhausted pool: every acquisition attempt raises,
    exactly like the real ProxyClient.get() does once every proxy has
    struck out (see test_proxies.py's max-strikes bug test)."""

    async def get(self):
        raise Exception("No available proxy")

    async def release(self, p):
        pass

    async def releaseBroken(self, p, cooldown_seconds):
        pass


def queued(*items):
    q: asyncio.Queue = asyncio.Queue()
    for item in items:
        q.put_nowait(item)
    return q


async def test_successful_requests_are_flushed_in_chunks():
    databricks = make_databricks()
    session = FakeAsyncSession(responses=[FakeResponse(200) for _ in range(4)])

    with patch("box_office.resources.common.RequestNode.AsyncSession", lambda **kw: session), patch(
        "box_office.resources.common.RequestNode.asyncio.sleep", new=AsyncMock()
    ):
        node = RequestNode(
            nodeID=0,
            proxyClient=WorkingProxyClient(),
            requestQ=queued({"url": "http://a"}, {"url": "http://b"}, {"url": "http://c"}, {"url": "http://d"}),
            run_id="run1",
            databricks=databricks,
            context=make_context(),
        )
        await node.run(maxRequestsFailedInRow=3, chunkSize=2, file_destination="dest")

    # 4 successful responses at chunkSize=2 -> two chunk flushes
    assert databricks.upload_raw_jsonl.await_count == 2


async def test_403_response_rotates_proxy_instead_of_raising():
    databricks = make_databricks()
    session = FakeAsyncSession(responses=[FakeResponse(403), FakeResponse(200)])
    proxy_client = Mock()
    proxy_client.get = AsyncMock(return_value=Proxy(url="http://p1"))
    proxy_client.release = AsyncMock()
    proxy_client.releaseBroken = AsyncMock()

    with patch("box_office.resources.common.RequestNode.AsyncSession", lambda **kw: session), patch(
        "box_office.resources.common.RequestNode.asyncio.sleep", new=AsyncMock()
    ):
        node = RequestNode(
            nodeID=0,
            proxyClient=proxy_client,
            requestQ=queued({"url": "http://a"}, {"url": "http://b"}),
            run_id="run1",
            databricks=databricks,
            context=make_context(),
        )
        await node.run(maxRequestsFailedInRow=3, chunkSize=10, file_destination="dest")

    proxy_client.releaseBroken.assert_awaited_once()
    # rotated to a fresh proxy after the 403 and kept going
    assert proxy_client.get.await_count == 2


async def test_non_200_status_swallows_the_actual_error_detail():
    """
    KNOWN BUG: RequestNode.run() does `else: raise Exception` (no message) for
    any non-200/403/407 response, then logs it as f'... | {e}'. str(Exception())
    is empty, so the real status code/reason is discarded before it's ever
    logged -- exactly what made the production failures on 2026-08-28..31
    impossible to diagnose from logs alone (every "Request error" line ended
    abruptly with nothing after the final "|").

    This test asserts the CORRECT behavior (the logged error should mention
    the actual status code) and is expected to FAIL until the bare
    `raise Exception` is replaced with something that preserves the status.
    """
    databricks = make_databricks()
    session = FakeAsyncSession(responses=[FakeResponse(404)])
    context = make_context()

    with patch("box_office.resources.common.RequestNode.AsyncSession", lambda **kw: session), patch(
        "box_office.resources.common.RequestNode.asyncio.sleep", new=AsyncMock()
    ):
        node = RequestNode(
            nodeID=0,
            proxyClient=WorkingProxyClient(),
            requestQ=queued({"url": "http://a"}),
            run_id="run1",
            databricks=databricks,
            context=context,
        )
        await node.run(maxRequestsFailedInRow=3, chunkSize=10, file_destination="dest")

    logged_messages = " ".join(str(call.args) for call in context.log.error.call_args_list)
    assert "404" in logged_messages, (
        "the response's actual status code never made it into the log message"
    )


async def test_one_starved_node_crashes_the_whole_gather_for_healthy_nodes():
    """
    KNOWN BUG (diagnosed from nightly seatmaps_raw failures, 2026-08-28..31).

    seatmaps_raw / showtimes_raw run every RequestNode with
    `asyncio.gather(*[node.run(...) for node in nodes])` -- no
    `return_exceptions=True`. RequestNode.run() never catches the exception
    raised by `request_proxy()` when ProxyClient.get() finds the pool
    exhausted. So one starved node's unhandled exception propagates out of
    gather() and cancels every other node mid-flight, wiping out whatever
    they hadn't flushed yet -- this is what produced the "Event loop is
    closed" cascade across dozens of nodes simultaneously in production.

    Expected to keep failing (i.e. the healthy node's work keeps getting
    lost) until run() isolates node failures, e.g. via
    `asyncio.gather(..., return_exceptions=True)`.
    """
    databricks = make_databricks()
    # chunkSize=2 so a completed healthy node would flush at least once;
    # zero flushes proves it never got that far before being cancelled.
    healthy_session = FakeAsyncSession(responses=[FakeResponse(200) for _ in range(4)])

    with patch("box_office.resources.common.RequestNode.AsyncSession", lambda **kw: healthy_session):
        healthy_node = RequestNode(
            nodeID=0,
            proxyClient=WorkingProxyClient(),
            requestQ=queued(*({"url": f"http://a/{i}"} for i in range(4))),
            run_id="run1",
            databricks=databricks,
            context=make_context(),
        )
        starved_node = RequestNode(
            nodeID=1,
            proxyClient=FailingProxyClient(),
            requestQ=queued({"url": "http://b/0"}),
            run_id="run1",
            databricks=databricks,
            context=make_context(),
        )

        with pytest.raises(Exception, match="No available proxy"):
            await asyncio.gather(
                healthy_node.run(maxRequestsFailedInRow=3, chunkSize=2, file_destination="dest"),
                starved_node.run(maxRequestsFailedInRow=3, chunkSize=2, file_destination="dest"),
            )

    assert databricks.upload_raw_jsonl.await_count == 0, (
        "the healthy node's completed work should not be lost just because "
        "a sibling node's proxy pool ran dry"
    )
