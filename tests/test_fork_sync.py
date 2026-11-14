import threading
import time

from pyper import Pipeline, task, fork


class ForkTestError(Exception):
    ...


# --------------------------------------------------------------------------- #
# Globally defined, picklable functions so multiprocessed branches can use them
# --------------------------------------------------------------------------- #

def src(n):
    for i in range(n):
        yield i


def identity(x):
    return x


def double(x):
    return x * 2


def slow_label(x):
    # Variable delay so branches complete out of input order.
    time.sleep(0.05 * (3 - x % 3))
    return ("slow", x)


def fast_label(x):
    return ("fast", x)


def fail_on_two(x):
    if x == 2:
        raise ForkTestError
    return x


def mp_double(x):
    return x * 2


def mp_slow(x):
    time.sleep(0.02)
    return x + 100


def consume(data):
    return list(data)


# --------------------------------------------------------------------------- #
# Basic regrouping / ordering
# --------------------------------------------------------------------------- #

def test_fork_regroups_results_per_input_in_order():
    p = task(src, branch=True) | fork(task(slow_label), task(fast_label))
    results = list(p(6))
    assert results == [(("slow", i), ("fast", i)) for i in range(6)]


def test_fork_three_branches_keep_branch_order():
    p = task(src, branch=True) | fork(task(identity), task(double), task(lambda x: x + 3))
    results = list(p(4))
    assert results == [(i, 2 * i, i + 3) for i in range(4)]


def test_fork_output_flows_into_downstream_task():
    p = (
        task(src, branch=True)
        | fork(task(double), task(identity))
        | task(lambda pair: pair[0] + pair[1])
    )
    assert list(p(5)) == [3 * i for i in range(5)]


def test_fork_supports_multi_task_branches():
    p = (
        task(src, branch=True)
        | fork(
            task(double) | task(double),          # x -> 4x
            task(identity) | task(lambda x: x + 1),  # x -> x+1
        )
    )
    assert list(p(4)) == [(4 * i, i + 1) for i in range(4)]


def test_fork_with_multiworker_first_branch_keeps_order():
    p = task(src, branch=True) | fork(task(slow_label, workers=4), task(fast_label))
    results = list(p(8))
    assert results == [(("slow", i), ("fast", i)) for i in range(8)]


def test_fork_with_consumer():
    p = task(src, branch=True) | fork(task(double), task(identity)) > consume
    assert p(4) == [(2 * i, i) for i in range(4)]


def test_fork_returns_pipeline_instance():
    p = fork(task(identity), task(identity))
    assert isinstance(p, Pipeline)


# --------------------------------------------------------------------------- #
# Laziness / backpressure
# --------------------------------------------------------------------------- #

def test_fork_is_lazy_until_iteration_starts():
    started = []

    def marker(x):
        started.append(x)
        return x

    p = task(src, branch=True) | fork(task(marker), task(marker))
    gen = p(3)
    time.sleep(0.05)
    assert started == []
    gen.close()


def test_fork_throttle_bounds_output_queue():
    # throttle=1 on the fork node: the collector cannot run far ahead of the consumer
    seen_max_inflight = []

    def track(x):
        seen_max_inflight.append(x)
        return x

    p = task(src, branch=True) | fork(task(identity, throttle=1), task(identity), throttle=1)
    it = iter(p(20))
    next(it)
    time.sleep(0.05)
    # The producer cannot flood: only a small number of inputs should have been dispatched.
    assert len(seen_max_inflight) <= 5
    assert list(it) == [(i, i) for i in range(1, 20)]


# --------------------------------------------------------------------------- #
# Multiprocessing
# --------------------------------------------------------------------------- #

def test_fork_with_multiprocessed_branch():
    p = task(src, branch=True) | fork(task(mp_double, workers=2, multiprocess=True), task(identity))
    assert list(p(5)) == [(2 * i, i) for i in range(5)]


def test_fork_with_multiprocessed_multi_task_branch():
    p = task(src, branch=True) | fork(
        task(mp_slow, workers=2, multiprocess=True) | task(double),
        task(identity),
    )
    assert list(p(4)) == [(2 * (i + 100), i) for i in range(4)]


# --------------------------------------------------------------------------- #
# Failure / early termination semantics
# --------------------------------------------------------------------------- #

def test_single_branch_failure_propagates():
    p = task(src, branch=True) | fork(task(fail_on_two), task(identity))
    try:
        list(p(6))
    except Exception as e:
        assert isinstance(e, ForkTestError)
    else:
        raise AssertionError("expected ForkTestError to propagate")


def test_failure_in_second_branch_propagates():
    p = task(src, branch=True) | fork(task(identity), task(fail_on_two))
    try:
        list(p(6))
    except Exception as e:
        assert isinstance(e, ForkTestError)
    else:
        raise AssertionError("expected ForkTestError to propagate")


def test_failure_in_deep_branch_task_propagates():
    p = task(src, branch=True) | fork(
        task(identity) | task(fail_on_two),
        task(identity),
    )
    try:
        list(p(6))
    except Exception as e:
        assert isinstance(e, ForkTestError)
    else:
        raise AssertionError("expected ForkTestError to propagate")


def test_multiprocessed_branch_failure_propagates():
    p = task(src, branch=True) | fork(task(fail_on_two, multiprocess=True), task(identity))
    try:
        list(p(6))
    except Exception as e:
        assert isinstance(e, ForkTestError)
    else:
        raise AssertionError("expected ForkTestError to propagate")


def test_early_consumer_cancellation_tears_down_cleanly():
    p = task(src, branch=True) | fork(task(slow_label), task(fast_label))
    gen = p(100)
    first = next(gen)
    assert first == (("slow", 0), ("fast", 0))
    gen.close()
    # Give worker threads a moment to wind down.
    time.sleep(0.3)
    # Every worker thread must have stopped; only the current (main) thread, which
    # runs the test itself, is expected to still be alive. Reaching here also proves
    # that shutdown did not deadlock.
    main = threading.current_thread()
    others = [t for t in threading.enumerate() if t is not main]
    assert all(not t.is_alive() for t in others), [t.name for t in others if t.is_alive()]


def test_upstream_ending_early_drains_partial_inputs():
    def short_source():
        yield 0
        yield 1

    p = task(short_source, branch=True) | fork(
        task(mp_double, multiprocess=True), task(identity))
    assert list(p()) == [(0, 0), (2, 1)]


def test_empty_upstream_emits_nothing_and_terminates():
    def empty_source():
        return
        yield  # pragma: no cover

    p = task(empty_source, branch=True) | fork(task(identity), task(identity))
    assert list(p()) == []


# --------------------------------------------------------------------------- #
# Start-up validation (before any worker/queue is created)
# --------------------------------------------------------------------------- #

def test_fork_requires_at_least_two_branches():
    # One branch cannot be expressed positionally (missing argument -> TypeError);
    # bypassing the signature still trips the Fork construction check (ValueError).
    try:
        fork(task(identity))
    except Exception as e:
        assert isinstance(e, (TypeError, ValueError))
    else:
        raise AssertionError


def test_fork_rejects_non_pipeline_branch():
    try:
        fork(task(identity), 42)
    except Exception as e:
        assert isinstance(e, TypeError)
    else:
        raise AssertionError


def test_fork_rejects_branch_task_with_branch_flag():
    try:
        fork(task(src, branch=True), task(identity))
    except Exception as e:
        assert isinstance(e, ValueError)
    else:
        raise AssertionError


def test_fork_rejects_branch_task_with_join_flag():
    try:
        fork(task(identity, join=True), task(identity))
    except Exception as e:
        assert isinstance(e, ValueError)
    else:
        raise AssertionError


def test_fork_rejects_nested_fork():
    try:
        fork(
            task(identity) | fork(task(identity), task(identity)),
            task(identity),
        )
    except Exception as e:
        assert isinstance(e, ValueError)
    else:
        raise AssertionError


def test_fork_cannot_be_the_first_node():
    p = fork(task(identity), task(identity))
    try:
        list(p(1))
    except Exception as e:
        assert isinstance(e, RuntimeError)
    else:
        raise AssertionError


def test_invalid_fork_throttle_rejected_at_construction():
    for value, exc in ((-1, ValueError), ("1", TypeError), (1.5, TypeError)):
        try:
            fork(task(identity), task(identity), throttle=value)
        except Exception as e:
            assert isinstance(e, exc)
        else:
            raise AssertionError


def test_invalid_branch_worker_config_rejected_before_start():
    # workers/throttle validation on branch tasks is inherited from Task and raised
    # before the pipeline starts.
    try:
        p = task(src, branch=True) | fork(task(identity, workers=0), task(identity))
        list(p(2))
    except Exception as e:
        assert isinstance(e, ValueError)
    else:
        raise AssertionError
