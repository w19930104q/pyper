from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import sys
from typing import TYPE_CHECKING

from .fork import AsyncBranchDispatcher, AsyncBranchStage, AsyncForkCollector, AsyncForkDispatcher
from .stage import AsyncProducer, AsyncProducerConsumer
from ..fork import Fork
from ..task import Task
from ..util.asynchronize import asynchronize
from ..util.sentinel import StopSentinel
from ..validation import validate_pipeline

if sys.version_info < (3, 11):  # pragma: no cover
    from ..util.task_group import TaskGroup, ExceptionGroup
else:
    from asyncio import TaskGroup

if TYPE_CHECKING:
    from ..pipeline import AsyncPipeline


class AsyncPipelineOutput:
    def __init__(self, pipeline: AsyncPipeline):
        self.pipeline = pipeline

    def _build_fork(self, tg: TaskGroup, q_in: asyncio.Queue, fork_node: Fork,
                    next_task, tp: ThreadPoolExecutor, pp: ProcessPoolExecutor) -> asyncio.Queue:
        branch_queues = []
        for branch in fork_node.branches:
            first_task = branch.tasks[0]
            maxsize = first_task.throttle if first_task.workers == 1 else 0
            branch_queues.append(asyncio.Queue(maxsize=maxsize))
        join_q = asyncio.Queue()

        AsyncForkDispatcher(q_in=q_in, fork=fork_node, branch_queues=branch_queues).start(tg)

        for branch_index, (branch, bq) in enumerate(zip(fork_node.branches, branch_queues)):
            tasks = [asynchronize(task, tp=tp, pp=pp) for task in branch.tasks]

            # Build the inter-task queues up front so adjacent stages share the same
            # queue objects, mirroring the synchronous builder.
            internal_queues = [asyncio.Queue(maxsize=task.throttle) for task in tasks[:-1]]

            first_task = tasks[0]
            if first_task.workers > 1:
                work_q = asyncio.Queue(maxsize=first_task.throttle)
                AsyncBranchDispatcher(q_in=bq, q_out=work_q, task=first_task).start(tg)
                stage_in = work_q
            else:
                stage_in = bq

            for i, task in enumerate(tasks):
                is_last = i == len(tasks) - 1
                stage_out = None if is_last else internal_queues[i]
                n_consumers = 1 if is_last else tasks[i + 1].workers
                stage = AsyncBranchStage(
                    q_in=stage_in, q_out=stage_out, task=task, is_last=is_last,
                    branch_index=branch_index, join_q=join_q, n_consumers=n_consumers)
                stage.start(tg)
                if not is_last:
                    stage_in = stage_out

        collector = AsyncForkCollector(join_q=join_q, fork=fork_node, next_task=next_task)
        collector.start(tg)
        return collector.q_out

    def _get_q_out(self, tg: TaskGroup, tp: ThreadPoolExecutor, pp: ProcessPoolExecutor, *args, **kwargs) -> asyncio.Queue:
        """Feed forward each node to the next, returning the output queue of the final node."""
        validate_pipeline(self.pipeline)
        q_out = None
        for node, next_node in zip(self.pipeline.tasks, self.pipeline.tasks[1:] + [None]):
            next_task = next_node if isinstance(next_node, Task) else None
            if isinstance(node, Fork):
                q_out = self._build_fork(tg, q_out, node, next_task, tp, pp)
                continue

            task = asynchronize(node, tp=tp, pp=pp)
            if q_out is None:
                stage = AsyncProducer(task=task, next_task=next_task)
                stage.start(tg, *args, **kwargs)
            else:
                stage = AsyncProducerConsumer(q_in=q_out, task=task, next_task=next_task)
                stage.start(tg)
            q_out = stage.q_out

        return q_out

    async def __call__(self, *args, **kwargs):
        """Iterate through the pipeline, taking the inputs to the first task, and yielding each output from the last task.

        Unify async, threaded, and multiprocessed work by:
        1. using TaskGroup to execute asynchronous tasks
        2. using ThreadPoolExecutor to execute threaded synchronous tasks
        3. using ProcessPoolExecutor to execute multiprocessed synchronous tasks
        """
        try:
            async with TaskGroup() as tg:
                with ThreadPoolExecutor() as tp, ProcessPoolExecutor() as pp:
                    q_out = self._get_q_out(tg, tp, pp, *args, **kwargs)
                    while (data := await q_out.get()) is not StopSentinel:
                        yield data
        except asyncio.CancelledError:
            # The consumer stopped iterating (aclose()) or the surrounding task was
            # cancelled. The TaskGroup has already torn every branch task down, so
            # let the cancellation propagate cleanly instead of surfacing it as an
            # ExceptionGroup.
            raise
        except BaseExceptionGroup as eg:
            # GeneratorExit raised at a `yield` on early close is grouped with the
            # resulting task cancellations. Treat a group caused solely by
            # cancellation/closure as a clean shutdown; real errors still propagate.
            cancellation = (asyncio.CancelledError, GeneratorExit)
            if all(isinstance(exc, cancellation) for exc in eg.exceptions):
                return
            raise eg.exceptions[0] from None
        except ExceptionGroup as eg:
            raise eg.exceptions[0] from None
