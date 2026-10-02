from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from box_office.resources.common.proxies import Proxy, ProxyClient, maxStrikes


def make_context():
    context = Mock()
    context.log = Mock()
    return context


def write_proxy_file(tmp_path, lines):
    path = tmp_path / "proxies.txt"
    path.write_text("\n".join(lines) + "\n")
    return str(path)


@pytest.fixture
def proxy_file(tmp_path):
    return write_proxy_file(
        tmp_path,
        [
            "1.1.1.1:8000:user1:pass1",
            "2.2.2.2:8000:user2:pass2",
            "3.3.3.3:8000:user3:pass3",
        ],
    )


def test_loads_proxies_from_file(proxy_file):
    client = ProxyClient(proxy_file, make_context())
    assert client._proxyQ.qsize() == 3


def test_missing_file_yields_empty_queue_instead_of_raising():
    client = ProxyClient("/nonexistent/does-not-exist.txt", make_context())
    assert client._proxyQ.qsize() == 0


async def test_get_returns_proxy_from_main_queue(proxy_file):
    client = ProxyClient(proxy_file, make_context())
    proxy = await client.get()
    assert isinstance(proxy, Proxy)
    assert client._proxyQ.qsize() == 2


async def test_get_falls_back_to_cooled_down_broken_proxy(proxy_file):
    """
    KNOWN BUG: get()'s fallback to _brokenQ is unreachable dead code. When
    _proxyQ is empty, `self._proxyQ.get_nowait()` raises asyncio.QueueEmpty,
    which is caught by the bare `except:` right there -- control jumps
    straight past the `self._brokenQ.get_nowait()` line for that loop
    iteration and never reaches it. The only way execution would reach the
    brokenQ check is if the _proxyQ read *succeeds*, but then `if p: return p`
    fires immediately and returns before the brokenQ check either. So a
    cooled-down proxy already released back via releaseBroken() can never
    actually be recovered by get() -- contradicting the method's own
    docstring ("If there's nothing available in main Q, fetch something from
    brokenQ").

    This test asserts the documented/intended behavior and is expected to
    FAIL until the try block is restructured (e.g. two separate try/excepts)
    so the brokenQ fallback is actually reachable.
    """
    client = ProxyClient(proxy_file, make_context())
    while not client._proxyQ.empty():
        client._proxyQ.get_nowait()

    cooled_down = Proxy(url="http://cooled-down", released_at=datetime.now() - timedelta(seconds=1))
    await client._brokenQ.put(cooled_down)

    with patch("box_office.resources.common.proxies.asyncio.sleep", new=AsyncMock()):
        result = await client.get(timeout=0.01)

    assert result is cooled_down


async def test_get_raises_after_timeout_when_nothing_available(proxy_file):
    client = ProxyClient(proxy_file, make_context())
    while not client._proxyQ.empty():
        client._proxyQ.get_nowait()

    with patch("box_office.resources.common.proxies.asyncio.sleep", new=AsyncMock()):
        with pytest.raises(Exception, match="No available proxy"):
            await client.get(timeout=0.01)


async def test_release_returns_proxy_to_main_queue(proxy_file):
    client = ProxyClient(proxy_file, make_context())
    p = Proxy(url="http://released-proxy")

    await client.release(p)

    assert client._proxyQ.qsize() == 4  # 3 loaded + 1 released


async def test_release_broken_increments_strikes_and_requeues_below_max(proxy_file):
    client = ProxyClient(proxy_file, make_context())
    p = Proxy(url="http://flaky-proxy", strikes=0)

    await client.releaseBroken(p, cooldown_seconds=60)

    assert p.strikes == 1
    assert p.released_at > datetime.now()
    assert client._brokenQ.qsize() == 1


async def test_release_broken_drops_proxy_silently_past_max_strikes(proxy_file):
    """
    KNOWN BUG (diagnosed from nightly seatmaps_raw failures, 2026-08-28 through 08-31).

    Once a proxy has already accumulated `maxStrikes` strikes, releaseBroken
    returns without requeueing it anywhere -- it disappears from both
    _proxyQ and _brokenQ for the rest of the run. With 85 concurrent
    RequestNodes sharing a fixed pool of ~100 proxies against a target that
    blocks aggressively, enough proxies get silently dropped this way that
    the pool empties completely, ProxyClient.get() raises "No available
    proxy", and that exception -- unhandled in RequestNode.run() -- kills
    the whole asyncio.gather() in seatmaps_raw.

    This test asserts the CORRECT behavior (a struck-out proxy should still
    be recoverable, not vanish) and is expected to FAIL until releaseBroken
    is fixed to requeue it instead of returning early.
    """
    client = ProxyClient(proxy_file, make_context())
    p = Proxy(url="http://already-struck-out", strikes=maxStrikes)

    await client.releaseBroken(p, cooldown_seconds=60)

    assert client._brokenQ.qsize() == 1, (
        "proxy was silently dropped from every queue once strikes >= maxStrikes"
    )
