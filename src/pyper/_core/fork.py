from __future__ import annotations

import typing as t

from .task import Task

if t.TYPE_CHECKING:
    from .pipeline import Pipeline


class Envelope:
    """An item flowing through the branches of a Fork, tagged with the sequence number
    of the original upstream input it belongs to.

    Envelopes are plain picklable objects so that they can cross process boundaries
    when branch tasks are multiprocessed.
    """

    __slots__ = ("seq", "data")

    def __init__(self, seq: int, data: t.Any):
        self.seq = seq
        self.data = data

    def __repr__(self):
        return f"Envelope(seq={self.seq}, data={self.data!r})"


class Fork:
    """A pipeline node that fans each input item out to several independent branch
    pipelines in parallel, and regroups the branch results per input.

    The output of the node is, for every input item, a tuple holding one result from
    each branch, in the order in which the branches were given. Tuples are emitted in
    the order of the original inputs, regardless of the order in which the branches
    happen to finish.

    A Fork is normally created with the :func:`fork` factory rather than directly.
    """

    __slots__ = ("branches", "throttle")

    def __init__(self, branches: t.List["Pipeline"], throttle: int = 0):
        # Local import to avoid a circular import (pipeline -> outputs -> fork -> pipeline).
        from .pipeline import Pipeline

        if not isinstance(throttle, int):
            raise TypeError("throttle must be an integer")
        if throttle < 0:
            raise ValueError("throttle cannot be less than 0")
        if len(branches) < 2:
            raise ValueError("a Fork requires at least 2 branch pipelines")
        for branch in branches:
            if not isinstance(branch, Pipeline):
                raise TypeError(
                    f"every branch of a Fork must be a Pipeline created with task(), got {branch} of type {type(branch)}")
            for node in branch.tasks:
                if isinstance(node, Fork):
                    raise ValueError("a Fork cannot contain another Fork (nested fork/join is not supported)")
                if node.join:
                    raise ValueError(
                        f"branch task {node.func} cannot join results inside a Fork: "
                        "a Fork regroups results per input, so branch tasks must be one-in/one-out tasks")
                if node.branch:
                    raise ValueError(
                        f"branch task {node.func} cannot fan out (branch=True) inside a Fork: "
                        "each branch must produce exactly one result per input so results can be regrouped")

        self.branches = branches
        self.throttle = throttle

    @property
    def is_async(self) -> bool:
        return any(any(task.is_async for task in branch.tasks) for branch in self.branches)

    def __repr__(self):
        return f"<Fork {[[task.func for task in branch.tasks] for branch in self.branches]}>"


def _make_fork(branches: t.Tuple["Pipeline", ...], throttle: int) -> "Pipeline":
    # Local import to avoid a circular import (pipeline -> outputs -> fork -> pipeline).
    from .pipeline import Pipeline
    return Pipeline([Fork(branches=list(branches), throttle=throttle)])


def fork(branch1: "Pipeline", branch2: "Pipeline", /, *branches: "Pipeline", throttle: int = 0) -> "Pipeline":
    """Create an explicit fork/join node from two or more branch pipelines.

    Each item received by the node is dispatched, unchanged, to every branch. The
    branches run concurrently and each must produce exactly one result per input.
    Once all branches have produced their result for a given input, their results are
    regrouped into a tuple in branch order, and the tuples are emitted to the next
    task in the order of the original inputs.

    Args:
        branch1, branch2, *branches: At least two `Pipeline` objects, each consisting
            of regular one-in/one-out tasks (no `branch=True`, `join=True` or nested
            `fork`). Every branch takes the same input as the node.
        throttle (int): Maximum size of the node's regrouped output queue; `0` means
            unbounded.

    Examples:
        ```python
        from pyper import task, fork

        def persist(data):
            ...  # write to the database
            return data

        def tag(data):
            ...  # annotate the data
            return data

        def alert(data):
            ...  # raise an alert if needed
            return data

        pipeline = task(collect) | fork(task(persist), task(tag), task(alert)) | task(handle)
        # `handle` receives tuples (persist_result, tag_result, alert_result),
        # one tuple per item yielded by `collect`, in input order
        ```
    """
    return _make_fork((branch1, branch2, *branches), throttle=throttle)
