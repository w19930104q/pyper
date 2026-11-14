import asyncio

import pytest

from pyper import AsyncPipeline, task, fork


class ForkTestError(Exception):
    ...


def src(n):
    for i in range(n):
        yield i


def identity(x):
    return x


def double(x):
    return x * 2


async def aslow(x):
    await asyncio.sleep(0.03 * (3 - x % 3))
    return ("aslow", x)


async def afast(x):
    return ("afast", x)


async def afail_on_two(x):
    if x == 2:
        raise ForkTestError
    return x


def fail_on_two(x):
    if x == 2:
        raise ForkTestError
    return x


async def a_identity(x):
    return x


async def consume(data):
    total = []
    async for item in data:
        total.append(item)
    return total


# --------------------------------------------------------------------------- #
# Basic regrouping / ordering
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_async_fork_regroups_in_input_order():
    p = task(src, branch=True) | fork(task(aslow, workers=3), task(afast))
    results = [v async for v in p(6)]
    assert results == [(("aslow", i), ("afast", i)) for i in range(6)]


@pytest.mark.asyncio
async def test_async_fork_three_branches():
    p = task(src, branch=True) | fork(task(a_identity), task(double), task(lambda x: x + 3))
    results = [v async for v in p(4)]
    assert results == [(i, 2 * i, i + 3) for i in range(4)]


@pytest.mark.asyncio
async def test_async_fork_feeds_downstream_task():
    p = (
        task(src, branch=True)
        | fork(task(a_identity), task(double))
        | task(lambda pair: pair[0] + pair[1])
    )
    assert [v async for v in p(5)] == [3 * i for i in range(5)]


@pytest.mark.asyncio
async def test_async_fork_with_multi_task_branches():
    p = task(src, branch=True) | fork(
        task(a_identity) | task(double),
        task(afast) | task(a_identity),
    )
    assert [v async for v in p(3)] == [(2 * i, ("afast", i)) for i in range(3)]


@pytest.mark.asyncio
async def test_async_fork_returns_async_pipeline_when_any_branch_async():
    p = fork(task(aslow), task(identity))
    assert isinstance(p, AsyncPipeline)


@pytest.mark.asyncio
async def test_async_fork_consumer():
    p = task(src, branch=True) | fork(task(a_identity), task(double)) > consume
    assert await p(3) == [(i, 2 * i) for i in range(3)]


# --------------------------------------------------------------------------- #
# Unified execution: async branch + threaded/multiprocessed sync branch
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_async_fork_mixed_threaded_branch():
    p = task(src, branch=True) | fork(task(aslow), task(double, workers=2))
    results = [v async for v in p(4)]
    assert results == [(("aslow", i), 2 * i) for i in range(4)]


@pytest.mark.asyncio
async def test_async_fork_mixed_multiprocessed_branch():
    p = task(src, branch=True) | fork(
        task(a_identity),
        task(double, workers=2, multiprocess=True),
    )
    results = [v async for v in p(4)]
    assert results == [(i, 2 * i) for i in range(4)]


# --------------------------------------------------------------------------- #
# Laziness / throttle
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_async_fork_is_lazy():
    started = []

    async def marker(x):
        started.append(x)
        return x

    p = task(src, branch=True) | fork(task(marker), task(marker))
    gen = p(3)
    await asyncio.sleep(0.05)
    assert started == []
    await gen.aclose()


@pytest.mark.asyncio
async def test_async_fork_throttle():
    p = task(src, branch=True) | fork(
        task(a_identity, throttle=1), task(a_identity), throttle=1)
    gen = p(20).__aiter__()
    await gen.__anext__()
    await asyncio.sleep(0.05)
    rest = [v async for v in gen]
    assert rest == [(i, i) for i in range(1, 20)]


# --------------------------------------------------------------------------- #
# Failure / cancellation / early termination
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_async_single_branch_failure_propagates():
    p = task(src, branch=True) | fork(task(afail_on_two), task(a_identity))
    with pytest.raises(ForkTestError):
        async for _ in p(6):
            pass


@pytest.mark.asyncio
async def test_async_sync_branch_failure_propagates():
    p = task(src, branch=True) | fork(task(a_identity), task(fail_on_two, workers=2))
    with pytest.raises(ForkTestError):
        async for _ in p(6):
            pass


@pytest.mark.asyncio
async def test_async_multiprocessed_branch_failure_propagates():
    p = task(src, branch=True) | fork(
        task(a_identity), task(fail_on_two, workers=2, multiprocess=True))
    with pytest.raises(ForkTestError):
        async for _ in p(6):
            pass


@pytest.mark.asyncio
async def test_async_early_cancellation_tears_down():
    p = task(src, branch=True) | fork(task(aslow), task(afast))
    gen = p(100).__aiter__()
    first = await gen.__anext__()
    assert first == (("aslow", 0), ("afast", 0))
    await gen.aclose()
    await asyncio.sleep(0.1)
    # Reaching here means the TaskGroup unwound without hanging.
    assert True


@pytest.mark.asyncio
async def test_async_upstream_ending_early():
    def short_source():
        yield 0
        yield 1

    p = task(short_source, branch=True) | fork(task(aslow), task(afast))
    results = [v async for v in p()]
    assert results == [(("aslow", 0), ("afast", 0)), (("aslow", 1), ("afast", 1))]


@pytest.mark.asyncio
async def test_async_empty_upstream():
    def empty_source():
        return
        yield  # pragma: no cover

    p = task(empty_source, branch=True) | fork(task(aslow), task(afast))
    assert [v async for v in p()] == []


# --------------------------------------------------------------------------- #
# Start-up validation
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_async_fork_cannot_be_first_node():
    p = fork(task(a_identity), task(a_identity))
    with pytest.raises(RuntimeError):
        async for _ in p(1):
            pass


@pytest.mark.asyncio
async def test_async_nested_fork_rejected():
    with pytest.raises(ValueError):
        fork(
            task(a_identity) | fork(task(a_identity), task(a_identity)),
            task(a_identity),
        )
