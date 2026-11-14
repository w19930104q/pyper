from __future__ import annotations

import queue
from typing import TYPE_CHECKING, Union

from .fork import (
    BranchDispatcher,
    BranchStage,
    ForkCollector,
    ForkDispatcher,
    make_queue,
)
from .stage import Producer, ProducerConsumer
from ..fork import Fork
from ..task import Task
from ..util.sentinel import StopSentinel
from ..validation import validate_pipeline
from ..util.worker_pool import ProcessPool, ThreadPool

if TYPE_CHECKING:
    import multiprocessing as mp
    from ..pipeline import Pipeline


class PipelineOutput:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    def _build_fork(self, q_in, fork_node: Fork, next_task, tp: ThreadPool, pp: ProcessPool):
        # A multiprocessing queue is required anywhere a multiprocessed stage touches
        # a queue: the whole BranchStage is pickled and sent to a worker process, so
        # every queue it references (input, output, the shared join queue) must be a
        # manager queue rather than a threading queue. The join queue is referenced by
        # every branch stage, so it becomes a manager queue whenever any branch task
        # is multiprocessed, not just the last one.
        any_mp = any(task.multiprocess for branch in fork_node.branches for task in branch.tasks)
        join_q = pp.manager.Queue() if any_mp else queue.Queue()

        branch_queues = []
        for branch in fork_node.branches:
            first_task = branch.tasks[0]
            # With a single worker the first task reads directly from this queue; with
            # multiple workers a BranchDispatcher bridges it into the throttled work
            # queue, in which case this queue only needs to hold the fan-out copies.
            maxsize = first_task.throttle if first_task.workers == 1 else 0
            bq = make_queue(first_task, first_task, pp.manager, maxsize=maxsize)
            branch_queues.append(bq)

        ForkDispatcher(
            q_in=q_in, fork=fork_node, branch_queues=branch_queues,
            shutdown_event=tp.shutdown_event).start(tp)

        for branch_index, (branch, bq) in enumerate(zip(fork_node.branches, branch_queues)):
            tasks = branch.tasks

            # Build the queues connecting this branch's tasks up front so producers
            # and consumers share the exact same queue objects.
            internal_queues = []
            for i, task in enumerate(tasks[:-1]):
                internal_queues.append(make_queue(task, tasks[i + 1], pp.manager, maxsize=task.throttle))

            first_task = tasks[0]
            if first_task.workers > 1:
                # Preserve per-branch FIFO order into the multi-worker first task.
                # The dispatcher is a thread-side coordinator even for multiprocessed
                # tasks, so it never competes with branch workers for process slots.
                work_q = make_queue(first_task, first_task, pp.manager, maxsize=first_task.throttle)
                BranchDispatcher(q_in=bq, q_out=work_q, task=first_task,
                                 shutdown_event=tp.shutdown_event).start(tp)
                stage_in = work_q
            else:
                stage_in = bq

            for i, task in enumerate(tasks):
                is_last = i == len(tasks) - 1
                stage_out = None if is_last else internal_queues[i]
                n_consumers = 1 if is_last else tasks[i + 1].workers
                pool = pp if task.multiprocess else tp
                stage = BranchStage(
                    q_in=stage_in, q_out=stage_out, task=task, is_last=is_last,
                    branch_index=branch_index, join_q=join_q, n_consumers=n_consumers,
                    manager=pp.manager, shutdown_event=pool.shutdown_event)
                stage.start(pool)
                if not is_last:
                    stage_in = stage_out

        collector = ForkCollector(
            join_q=join_q, fork=fork_node, next_task=next_task,
            manager=pp.manager, shutdown_event=tp.shutdown_event)
        collector.start(tp)
        return collector.q_out

    def _get_q_out(self, tp: ThreadPool, pp: ProcessPool, *args, **kwargs) -> Union[mp.Queue, queue.Queue]:
        """Feed forward each node to the next, returning the output queue of the final node."""
        validate_pipeline(self.pipeline)
        q_out = None
        for node, next_node in zip(self.pipeline.tasks, self.pipeline.tasks[1:] + [None]):
            next_task = next_node if isinstance(next_node, Task) else None
            if isinstance(node, Fork):
                q_out = self._build_fork(q_out, node, next_task, tp, pp)
                continue

            task = node
            pool = pp if task.multiprocess else tp
            if q_out is None:
                stage = Producer(task=task, next_task=next_task, manager=pp.manager, shutdown_event=pool.shutdown_event)
                stage.start(pool, *args, **kwargs)
            else:
                stage = ProducerConsumer(q_in=q_out, task=task, next_task=next_task, manager=pp.manager, shutdown_event=pool.shutdown_event)
                stage.start(pool)
            q_out = stage.q_out

        return q_out

    def __call__(self, *args, **kwargs):
        """Iterate through the pipeline, taking the inputs to the first task, and yielding each output from the last task."""
        with ThreadPool() as tp, ProcessPool() as pp:
            q_out = self._get_q_out(tp, pp, *args, **kwargs)
            try:
                while (data := q_out.get()) is not StopSentinel:
                    yield data
            except (KeyboardInterrupt, SystemExit):  # pragma: no cover
                tp.shutdown_event.set()
                pp.shutdown_event.set()
                raise
            except GeneratorExit:
                # The consumer closed the generator early: signal every worker (thread
                # and process) to stop at the next queue poll so the pipeline winds
                # down promptly rather than draining the remaining inputs.
                tp.shutdown_event.set()
                pp.shutdown_event.set()
                raise
