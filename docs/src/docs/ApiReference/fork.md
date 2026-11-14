---
title: fork
parent: API Reference
layout: default
nav_order: 2
permalink: /docs/ApiReference/fork
---

# pyper.fork
{: .no_toc }

* TOC
{:toc}

## fork

```python
def fork(branch1: Pipeline, branch2: Pipeline, /, *branches: Pipeline, throttle: int = 0) -> Pipeline:
```

Creates an explicit **fork/join** node from two or more branch pipelines.

Each item received by the node is dispatched, unchanged, to every branch. The branches run concurrently, and each must produce exactly one result per input. Once all branches have produced their result for a given input, the results are regrouped into a tuple in branch order, and the tuples are emitted to the next task in the order of the original inputs.

The returned object is a [Pipeline](Pipeline) containing a single fork/join node, so it is composed with other pipelines using the `|` operator:

```python
from pyper import task, fork

def collect():
    yield 1
    yield 2

def persist(data):
    return ("persisted", data)

def tag(data):
    return ("tagged", data)

if __name__ == "__main__":
    pipeline = task(collect, branch=True) | fork(task(persist), task(tag))
    for output in pipeline():
        print(output)
        #> (('persisted', 1), ('tagged', 1))
        #> (('persisted', 2), ('tagged', 2))
```

See the [user guide](../UserGuide/ComposingPipelines#forking-and-joining) for the full discussion of fork/join semantics.

---

{: .text-green-200 .text-gamma}
**Parameters**

{: .text-beta}
### `branch1`, `branch2`, `*branches`

* **type:** `Pipeline` (at least two, positional)

The branch pipelines to run in parallel for every input item. Every branch takes the same input as the node and must consist of regular one-in/one-out tasks. A branch may itself contain several piped tasks.

Branches are forbidden from using the fan-out/streaming mechanisms, because each branch must return exactly one result per input so that results can be regrouped:

* no task inside a branch may set `branch=True` or `join=True`
* a branch cannot itself contain a `fork` node

Passing a non-`Pipeline` branch, or fewer than two branches, raises a `TypeError`/`ValueError` when the node is constructed.

{: .text-beta}
### `throttle`

* **type:** `int`
* **default:** `0`

The maximum size of the node's regrouped output queue. `0` means unbounded. A non-zero value applies backpressure: once the queue is full, the node stops emitting further tuples until the downstream task consumes them. Negative or non-integer values raise at construction time.

---

{: .text-green-200 .text-gamma}
**Returns**

A [Pipeline](Pipeline) holding the fork/join node. The pipeline is an `AsyncPipeline` if any branch task is asynchronous, otherwise a synchronous `Pipeline`. The node emits one tuple per input item, whose elements are the branch results in the order the branches were passed to `fork`.

{: .warning}
A `fork` node cannot be the first node of a pipeline; it needs an upstream task to fan out. Calling a pipeline whose first node is a `fork` raises a `RuntimeError` before any worker is started.

## Execution semantics

* **Lazy** — nothing runs until the returned pipeline is iterated.
* **Concurrency** — branches run concurrently using Pyper's thread/process/async machinery. Branch tasks accept the usual `workers`, `throttle` and `multiprocess` parameters, and the three execution modes can be mixed across branches.
* **Ordering** — output tuples are always emitted in input order, and tuple elements are always in branch order, regardless of completion order.
* **Failure** — an exception in any branch propagates to the consumer and shuts the remaining branches down.
* **Cancellation** — closing the output generator early tears the whole node down.
