from __future__ import annotations

import queue as queue_module
import threading
from types import SimpleNamespace
from typing import TYPE_CHECKING, Union

from ..fork import Envelope, Fork
from ..util.sentinel import StopSentinel

if TYPE_CHECKING:
    import multiprocessing as mp
    import multiprocessing.synchronize as mpsync
    from multiprocessing.managers import SyncManager
    from ..util.worker_pool import WorkerPool
    from ..task import Task


# How long a blocked queue operation waits before re-checking the shutdown event.
# This keeps every worker interruptible when a sibling branch has failed.
_POLL_TIMEOUT = 0.1


def interruptible_get(q: Union[mp.Queue, queue_module.Queue],
                      shutdown_event: Union[mpsync.Event, threading.Event]):
    """Get from a queue, returning StopSentinel promptly once shutdown is requested."""
    while True:
        try:
            return q.get(timeout=_POLL_TIMEOUT)
        except queue_module.Empty:
            if shutdown_event.is_set():
                return StopSentinel


def interruptible_put(q: Union[mp.Queue, queue_module.Queue], item,
                      shutdown_event: Union[mpsync.Event, threading.Event]) -> bool:
    """Put on a queue, giving up (returning False) promptly once shutdown is requested."""
    while True:
        try:
            q.put(item, timeout=_POLL_TIMEOUT)
            return True
        except queue_module.Full:
            if shutdown_event.is_set():
                return False


def make_queue(task: Task, next_task, manager: SyncManager, maxsize: int = 0):
    """Mirror the queue selection of the regular stages: a multiprocessing queue is
    only used when the producing task or the consuming task are multiprocessed."""
    return manager.Queue(maxsize=maxsize) \
        if task.multiprocess or (next_task is not None and next_task.multiprocess) \
        else queue_module.Queue(maxsize=maxsize)


class ForkDispatcher:
    """Single worker sitting on the input queue of a Fork node.

    Tags every input item with a monotonically increasing sequence number (the
    position of the input in the upstream stream) and fans a copy of the tagged
    Envelope out to every branch's input queue.
    """

    def __init__(
            self,
            q_in: Union[mp.Queue, queue_module.Queue],
            fork: Fork,
            branch_queues: list,
            shutdown_event: Union[mpsync.Event, threading.Event]):
        self.q_in = q_in
        self._fork = fork
        self._branch_queues = branch_queues
        self._shutdown_event = shutdown_event

    def _worker(self):
        try:
            seq = 0
            while True:
                data = interruptible_get(self.q_in, self._shutdown_event)
                if data is StopSentinel:
                    break
                for bq in self._branch_queues:
                    if not interruptible_put(bq, Envelope(seq=seq, data=data), self._shutdown_event):
                        return
                seq += 1
        except Exception:
            self._shutdown_event.set()
            raise
        finally:
            # One stop sentinel per worker on the first task of each branch.
            for branch, bq in zip(self._fork.branches, self._branch_queues):
                for _ in range(branch.tasks[0].workers):
                    interruptible_put(bq, StopSentinel, self._shutdown_event)

    def start(self, pool: WorkerPool, /):
        pool.submit(self._worker)


class BranchDispatcher:
    """Single worker feeding the first task of a branch when that task has multiple
    workers. Without it, several workers would pull envelopes directly and could
    reorder them; the dispatcher preserves per-branch FIFO order into the work queue.

    This is a pure queue-forwarding coordinator, so it always runs in a thread even
    when the consuming task is multiprocessed. Running it in the process pool would
    make it compete with the long-lived branch workers for the same pool processes
    and deadlock, since it blocks waiting for inputs that those workers cannot
    produce until it releases a process.
    """

    def __init__(
            self,
            q_in: Union[mp.Queue, queue_module.Queue],
            q_out: Union[mp.Queue, queue_module.Queue],
            task: Task,
            shutdown_event: Union[mpsync.Event, threading.Event]):
        self.q_in = q_in
        self.q_out = q_out
        self._n_consumers = task.workers
        self._shutdown_event = shutdown_event

    def _worker(self):
        try:
            while True:
                envelope = interruptible_get(self.q_in, self._shutdown_event)
                if envelope is StopSentinel:
                    break
                if not interruptible_put(self.q_out, envelope, self._shutdown_event):
                    return
        except Exception:
            self._shutdown_event.set()
            raise
        finally:
            for _ in range(self._n_consumers):
                interruptible_put(self.q_out, StopSentinel, self._shutdown_event)

    def start(self, pool: WorkerPool, /):
        pool.submit(self._worker)


class BranchStage:
    """One task within a branch of a Fork.

    Envelopes flow through the branch untouched apart from their `data` being
    transformed by the task. The last stage of each branch additionally tags its
    results with the branch index onto the shared join queue.

    The input and output queues are created by the pipeline builder and passed in
    explicitly so adjacent stages are wired to the same queue objects.
    """

    def __init__(
            self,
            q_in: Union[mp.Queue, queue_module.Queue],
            q_out: Union[mp.Queue, queue_module.Queue, None],
            task: Task,
            is_last: bool,
            branch_index: int,
            join_q: Union[mp.Queue, queue_module.Queue],
            n_consumers: int,
            manager: SyncManager,
            shutdown_event: Union[mpsync.Event, threading.Event]):
        self.q_in = q_in
        # `q_out` is None for the last branch task, which writes to the join queue.
        self.q_out = q_out

        self._task = task
        self._is_last = is_last
        self._branch_index = branch_index
        self._join_q = join_q
        self._shutdown_event = shutdown_event
        self._n_workers = task.workers
        self._n_consumers = n_consumers
        self._workers_done = manager.Value('i', 0) if task.multiprocess else SimpleNamespace(value=0)
        self._workers_done_lock = manager.Lock() if task.multiprocess else threading.Lock()

    def _worker(self):
        try:
            while True:
                envelope = interruptible_get(self.q_in, self._shutdown_event)
                if envelope is StopSentinel:
                    break
                if self._shutdown_event.is_set():
                    break
                result = self._task.func(envelope.data)
                if self._is_last:
                    item = (envelope.seq, self._branch_index, result)
                    if not interruptible_put(self._join_q, item, self._shutdown_event):
                        return
                else:
                    if not interruptible_put(self.q_out, Envelope(seq=envelope.seq, data=result),
                                             self._shutdown_event):
                        return
        except Exception:
            self._shutdown_event.set()
            raise
        finally:
            with self._workers_done_lock:
                self._workers_done.value += 1
                if self._workers_done.value == self._n_workers:
                    if self._is_last:
                        # One branch sentinel per branch, consumed by the collector.
                        interruptible_put(
                            self._join_q, (-1, self._branch_index, StopSentinel), self._shutdown_event)
                    else:
                        for _ in range(self._n_consumers):
                            interruptible_put(self.q_out, StopSentinel, self._shutdown_event)

    def start(self, pool: WorkerPool, /):
        for _ in range(self._n_workers):
            pool.submit(self._worker)


class ForkCollector:
    """Single worker regrouping branch results per input.

    Consumes `(seq, branch_index, result)` items from the shared join queue and, as
    soon as every branch has reported for a given sequence, emits a tuple of results
    in branch order. Tuples are emitted in input (sequence) order even when branches
    complete out of order.
    """

    def __init__(
            self,
            join_q: Union[mp.Queue, queue_module.Queue],
            fork: Fork,
            next_task: Union[Task, None],
            manager: SyncManager,
            shutdown_event: Union[mpsync.Event, threading.Event]):
        self.q_out = manager.Queue(maxsize=fork.throttle) \
            if next_task is not None and next_task.multiprocess \
            else queue_module.Queue(maxsize=fork.throttle)

        self._join_q = join_q
        self._n_branches = len(fork.branches)
        self._n_consumers = 1 if next_task is None else next_task.workers
        self._shutdown_event = shutdown_event

    def _worker(self):
        groups: dict = {}
        closed_branches = set()
        next_seq = 0
        try:
            while len(closed_branches) < self._n_branches:
                item = interruptible_get(self._join_q, self._shutdown_event)
                if item is StopSentinel:
                    break
                seq, branch_index, payload = item
                if payload is StopSentinel:
                    closed_branches.add(branch_index)
                    continue
                groups.setdefault(seq, {})[branch_index] = payload
                # Emit every fully assembled group, strictly in input order.
                while next_seq in groups and len(groups[next_seq]) == self._n_branches:
                    group = groups.pop(next_seq)
                    result = tuple(group[i] for i in range(self._n_branches))
                    if not interruptible_put(self.q_out, result, self._shutdown_event):
                        return
                    next_seq += 1
        except Exception:
            self._shutdown_event.set()
            raise
        finally:
            for _ in range(self._n_consumers):
                interruptible_put(self.q_out, StopSentinel, self._shutdown_event)

    def start(self, pool: WorkerPool, /):
        pool.submit(self._worker)
