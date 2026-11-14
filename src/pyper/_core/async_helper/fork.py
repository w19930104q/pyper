from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from ..fork import Envelope, Fork
from ..util.sentinel import StopSentinel

if sys.version_info < (3, 11):  # pragma: no cover
    from ..util.task_group import TaskGroup
else:
    from asyncio import TaskGroup

if TYPE_CHECKING:
    from ..task import Task


class AsyncForkDispatcher:
    """Async counterpart of the synchronous ForkDispatcher.

    Tags inputs with sequence numbers and fans each tagged Envelope out to every
    branch's input queue. Cancellation/errors are handled by the surrounding
    TaskGroup, so no explicit shutdown event is needed.
    """

    def __init__(self, q_in: asyncio.Queue, fork: Fork, branch_queues: list):
        self.q_in = q_in
        self._fork = fork
        self._branch_queues = branch_queues

    async def _worker(self):
        seq = 0
        while True:
            data = await self.q_in.get()
            if data is StopSentinel:
                break
            for bq in self._branch_queues:
                await bq.put(Envelope(seq=seq, data=data))
            seq += 1

        for branch, bq in zip(self._fork.branches, self._branch_queues):
            for _ in range(branch.tasks[0].workers):
                await bq.put(StopSentinel)

    def start(self, tg: TaskGroup, /):
        tg.create_task(self._worker())


class AsyncBranchDispatcher:
    """Async counterpart of the synchronous BranchDispatcher: preserves per-branch
    FIFO order into a first task that runs on multiple workers."""

    def __init__(self, q_in: asyncio.Queue, q_out: asyncio.Queue, task: Task):
        self.q_in = q_in
        self.q_out = q_out
        self._n_consumers = task.workers

    async def _worker(self):
        while True:
            envelope = await self.q_in.get()
            if envelope is StopSentinel:
                break
            await self.q_out.put(envelope)

        for _ in range(self._n_consumers):
            await self.q_out.put(StopSentinel)

    def start(self, tg: TaskGroup, /):
        tg.create_task(self._worker())


class AsyncBranchStage:
    """One task within a branch of a Fork in an async pipeline.

    Envelopes flow through the branch untouched apart from their `data` being
    transformed by the task. The last stage of each branch additionally tags its
    results with the branch index onto the shared join queue.

    The input and output queues are created by the pipeline builder and passed in
    explicitly so adjacent stages are wired to the same queue objects. `q_out` is
    None for the last task, which writes to the join queue instead.
    """

    def __init__(
            self,
            q_in: asyncio.Queue,
            q_out,
            task: Task,
            is_last: bool,
            branch_index: int,
            join_q: asyncio.Queue,
            n_consumers: int):
        self.q_in = q_in
        self.q_out = q_out

        self._task = task
        self._is_last = is_last
        self._branch_index = branch_index
        self._join_q = join_q
        self._n_workers = task.workers
        self._n_consumers = n_consumers
        self._workers_done = 0

    async def _worker(self):
        while True:
            envelope = await self.q_in.get()
            if envelope is StopSentinel:
                break
            result = await self._task.func(envelope.data)
            if self._is_last:
                await self._join_q.put((envelope.seq, self._branch_index, result))
            else:
                await self.q_out.put(Envelope(seq=envelope.seq, data=result))

        self._workers_done += 1
        if self._workers_done == self._n_workers:
            if self._is_last:
                await self._join_q.put((-1, self._branch_index, StopSentinel))
            else:
                for _ in range(self._n_consumers):
                    await self.q_out.put(StopSentinel)

    def start(self, tg: TaskGroup, /):
        for _ in range(self._n_workers):
            tg.create_task(self._worker())


class AsyncForkCollector:
    """Async counterpart of the synchronous ForkCollector.

    Regroups the branch results into tuples in input order.
    """

    def __init__(self, join_q: asyncio.Queue, fork: Fork, next_task):
        self.q_out: asyncio.Queue = asyncio.Queue(maxsize=fork.throttle)

        self._join_q = join_q
        self._n_branches = len(fork.branches)
        self._n_consumers = 1 if next_task is None else next_task.workers

    async def _worker(self):
        groups: dict = {}
        closed_branches = set()
        next_seq = 0
        while len(closed_branches) < self._n_branches:
            item = await self._join_q.get()
            seq, branch_index, payload = item
            if payload is StopSentinel:
                closed_branches.add(branch_index)
                continue
            groups.setdefault(seq, {})[branch_index] = payload
            while next_seq in groups and len(groups[next_seq]) == self._n_branches:
                group = groups.pop(next_seq)
                result = tuple(group[i] for i in range(self._n_branches))
                await self.q_out.put(result)
                next_seq += 1

        for _ in range(self._n_consumers):
            await self.q_out.put(StopSentinel)

    def start(self, tg: TaskGroup, /):
        tg.create_task(self._worker())
