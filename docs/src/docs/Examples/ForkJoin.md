---
title: Fork / Join
parent: Examples
layout: default
nav_order: 2
permalink: /docs/Examples/ForkJoin
---

# Fork / Join
{: .no_toc }

* TOC
{:toc}

## Problem Statement

A single collected record often needs to be processed by several independent tasks at the same time — for example written to a database, annotated, and checked against an alerting threshold. A linear pipeline (`|`) only runs one task at a time on each item, so it cannot express this.

This example uses an explicit [fork/join](../UserGuide/ComposingPipelines#forking-and-joining) to:

1. fan every collected record out to three branches — persistence, tagging, and alerting — that run concurrently
2. regroup the three branch results back into one tuple per record
3. pass the regrouped tuple, in original input order, to a downstream summarising task

The full code is in [`examples/ForkJoin/main.py`](https://github.com/pyper-dev/pyper/blob/main/examples/ForkJoin/main.py) and has no third-party dependencies.

## Pipeline

```python
from pyper import task, fork

pipeline = (
    task(collect_records, branch=True)   # lazily yield records
    | fork(
        task(persist),                   # branch 1: database
        task(tag),                       # branch 2: labelling
        task(alert),                     # branch 3: alerting
    )
    | task(summarise)                    # receives (persist, tag, alert) tuples
)
```

Each branch takes the same record and returns exactly one result. `fork` regroups those results into a tuple in branch order, and emits one tuple per record in input order regardless of which branch finishes first.

## Output

```text
{'id': 0, 'stored': True, 'label': 'even', 'alert': False}
{'id': 1, 'stored': True, 'label': 'odd', 'alert': False}
{'id': 2, 'stored': True, 'label': 'even', 'alert': False}
{'id': 3, 'stored': True, 'label': 'odd', 'alert': True}
{'id': 4, 'stored': True, 'label': 'even', 'alert': True}
{'id': 5, 'stored': True, 'label': 'odd', 'alert': True}
```

The branches can use the usual task parameters independently — for example `task(persist, workers=8)` or a `multiprocess=True` CPU-bound branch — and the fork/join node still restores the input order when it regroups the results.
