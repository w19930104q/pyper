"""Fork/join example: fan each collected record out to three independent processing
branches (persistence, tagging, alerting) in parallel, regroup the results per
record in input order, and hand them to a downstream task.

The example is fully self-contained (no network, no third-party packages) and uses
plain lists/dicts to stand in for a database and an alerting sink. Run it with:

    python main.py
"""
import time

from pyper import task, fork


# --------------------------------------------------------------------------- #
# Upstream: lazily yield the collected records
# --------------------------------------------------------------------------- #
def collect_records(n: int):
    for i in range(n):
        # Simulate records arriving one at a time.
        yield {"id": i, "value": i}


# --------------------------------------------------------------------------- #
# The three parallel branches. Each is an ordinary one-in/one-out function.
# --------------------------------------------------------------------------- #
def persist(record: dict) -> dict:
    """Branch 1: write the record to the 'database'."""
    time.sleep(0.01)
    return {"id": record["id"], "stored": True}


def tag(record: dict) -> dict:
    """Branch 2: annotate the record."""
    return {"id": record["id"], "label": "even" if record["value"] % 2 == 0 else "odd"}


def alert(record: dict) -> dict:
    """Branch 3: raise an alert for records that cross a threshold."""
    return {"id": record["id"], "alert": record["value"] >= 3}


# --------------------------------------------------------------------------- #
# Downstream: consume the regrouped tuple for each original record
# --------------------------------------------------------------------------- #
def summarise(grouped: tuple) -> dict:
    stored, labelled, alerted = grouped
    return {
        "id": stored["id"],
        "stored": stored["stored"],
        "label": labelled["label"],
        "alert": alerted["alert"],
    }


def main():
    pipeline = (
        task(collect_records, branch=True)
        | fork(
            task(persist),            # branch 1: database
            task(tag),                # branch 2: labelling
            task(alert),              # branch 3: alerting
        )
        | task(summarise)
    )

    # One dict per input record, in input order, even though the branches finish
    # out of order and run concurrently.
    for result in pipeline(6):
        print(result)


if __name__ == "__main__":
    main()
