from __future__ import annotations

import typing as t

from .fork import Fork
from .task import Task

if t.TYPE_CHECKING:
    from .pipeline import Pipeline


def validate_pipeline(pipeline: Pipeline):
    """Validate the topology and worker configuration of a pipeline before any worker
    or queue is created, so that invalid configurations fail fast at pipeline start
    (i.e. when the pipeline is called), rather than halfway through execution.
    """
    nodes = pipeline.tasks
    if not nodes:  # pragma: no cover - pipelines always hold at least one node
        raise RuntimeError("a pipeline must contain at least one task")

    if isinstance(nodes[0], Fork):
        raise RuntimeError("the first node in a pipeline cannot be a Fork: a Fork needs an upstream task to fan out")

    first_task = nodes[0]
    if isinstance(first_task, Task):
        if first_task.workers > 1:
            raise RuntimeError(f"The first task in a pipeline ({first_task.func}) cannot have more than 1 worker")
        if first_task.join:
            raise RuntimeError(f"The first task in a pipeline ({first_task.func}) cannot join previous results")

    for index, node in enumerate(nodes):
        if isinstance(node, Fork):
            # Construction-time checks cover branch count/types and forbidden task
            # flags; re-validate here since Fork objects can be built directly.
            Fork(branches=node.branches, throttle=node.throttle)
            for branch in node.branches:
                validate_branch(branch)


def validate_branch(branch: Pipeline):
    """Validate one branch of a Fork as a standalone pipeline fragment."""
    first_task = branch.tasks[0]
    # Fork construction rejects nested Forks, so a branch's first node is always a Task.
    assert isinstance(first_task, Task)
    if first_task.join:
        raise RuntimeError(
            f"the first task of a Fork branch ({first_task.func}) cannot join previous results")
