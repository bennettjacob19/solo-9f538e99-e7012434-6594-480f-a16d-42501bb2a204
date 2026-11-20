"""Clock-skew diagnosis across archived traces.

A duplicate-charge postmortem asks: which started first — the gateway retry
or the payment service's first debit?  Wall clocks differ per host, so raw
timestamps from different services cannot be compared directly.  This module
reconstructs the order from the traces themselves:

* every service (``service.name``) gets exactly one fixed clock offset for
  the whole diagnosis;
* every *direct* CLIENT→SERVER parent-child call whose start and end times
  are complete and valid contributes two constraints: after applying the
  offsets, the SERVER interval must lie entirely inside the CLIENT interval;
* the order of the two target spans' start times is reported only when every
  feasible offset assignment yields the same order (or always equality);
  otherwise the conclusion is "indeterminate";
* when no offset assignment satisfies all constraints, the diagnosis reports
  one set of calls that cannot hold simultaneously.

The constraints are difference constraints of the form ``offset[v] <=
offset[u] + w`` and are solved with Bellman-Ford.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional

UNKNOWN_SERVICE = "unknown"

A_FIRST = "a_first"
B_FIRST = "b_first"
SIMULTANEOUS = "simultaneous"
INDETERMINATE = "indeterminate"
INFEASIBLE = "infeasible"

CONCLUSION_LABELS = {
    A_FIRST: "A started first",
    B_FIRST: "B started first",
    SIMULTANEOUS: "Both started at the same instant",
    INDETERMINATE: "Cannot be determined",
    INFEASIBLE: "Cannot be determined — constraints unsatisfiable",
}

CONCLUSION_TONES = {
    A_FIRST: "ok",
    B_FIRST: "ok",
    SIMULTANEOUS: "ok",
    INDETERMINATE: "warn",
    INFEASIBLE: "err",
}

MIN_TRACES = 2
MAX_TRACES = 8

_TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_SPAN_ID_RE = re.compile(r"^[0-9a-f]{16}$")


class DiagnosisInputError(ValueError):
    """The diagnosis request itself is malformed (HTTP 400)."""


class TraceNotArchived(LookupError):
    """A requested trace id is not in the committed archive (HTTP 404)."""

    def __init__(self, missing: list[str]) -> None:
        self.missing = list(missing)
        super().__init__("traces not archived: " + ", ".join(self.missing))


class TargetNotFound(ValueError):
    """A target span id does not exist in the selected traces (HTTP 400)."""


# ---------------------------------------------------------------------------
# Request spec
# ---------------------------------------------------------------------------


def parse_diagnosis_spec(body: Any) -> dict:
    """Validate a diagnosis request body into a normalised spec.

    Expected shape::

        {"traceIds": ["<32 hex>", ...],
         "targets": [{"traceId": ..., "spanId": ...},
                     {"traceId": ..., "spanId": ...}]}
    """

    if not isinstance(body, dict):
        raise DiagnosisInputError("request body must be a JSON object")

    raw_ids = body.get("traceIds")
    if not isinstance(raw_ids, list) or not raw_ids:
        raise DiagnosisInputError(
            f"traceIds must be a list of {MIN_TRACES} to {MAX_TRACES} archived trace ids"
        )
    trace_ids: list[str] = []
    for item in raw_ids:
        text = item.strip().lower() if isinstance(item, str) else ""
        if not _TRACE_ID_RE.match(text):
            raise DiagnosisInputError(f"invalid traceId: {item!r} (want 32 hex chars)")
        if text not in trace_ids:
            trace_ids.append(text)
    if not MIN_TRACES <= len(trace_ids) <= MAX_TRACES:
        raise DiagnosisInputError(
            f"provide between {MIN_TRACES} and {MAX_TRACES} distinct traceIds "
            f"(got {len(trace_ids)})"
        )

    raw_targets = body.get("targets")
    if not isinstance(raw_targets, list) or len(raw_targets) != 2:
        raise DiagnosisInputError(
            "targets must be a list of exactly two {traceId, spanId} objects"
        )
    targets = []
    for raw in raw_targets:
        if not isinstance(raw, dict):
            raise DiagnosisInputError("each target must be an object {traceId, spanId}")
        trace_id = str(raw.get("traceId") or "").strip().lower()
        span_id = str(raw.get("spanId") or "").strip().lower()
        if not _TRACE_ID_RE.match(trace_id):
            raise DiagnosisInputError(f"invalid target traceId: {raw.get('traceId')!r}")
        if not _SPAN_ID_RE.match(span_id):
            raise DiagnosisInputError(f"invalid target spanId: {raw.get('spanId')!r}")
        if trace_id not in trace_ids:
            raise DiagnosisInputError(
                f"target traceId {trace_id} is not among the selected traces"
            )
        targets.append({"traceId": trace_id, "spanId": span_id})
    if targets[0] == targets[1]:
        raise DiagnosisInputError("the two targets must be different spans")

    return {"traceIds": trace_ids, "targets": targets}


# ---------------------------------------------------------------------------
# Constraint extraction
# ---------------------------------------------------------------------------


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _service_of(span: dict) -> str:
    return span.get("service_name") or UNKNOWN_SERVICE


def _valid_interval(span: dict) -> bool:
    """Complete and valid start/end: both present, end not before start."""

    start = _as_int(span.get("start_time_ns"))
    end = _as_int(span.get("end_time_ns"))
    return start is not None and end is not None and start <= end


def extract_call_pairs(traces: list[dict]) -> list[dict]:
    """All direct CLIENT→SERVER parent-child calls with usable intervals.

    A call qualifies when a SERVER span's parent (same trace) is a CLIENT
    span and both spans have complete, valid start/end times.  The returned
    dicts carry the identifiers and the raw nanosecond timestamps.
    """

    calls: list[dict] = []
    for trace in traces:
        trace_id = trace.get("trace_id")
        spans = trace.get("spans") or []
        by_id = {s.get("span_id"): s for s in spans if s.get("span_id")}
        for span in spans:
            if span.get("kind") != "SERVER":
                continue
            parent = by_id.get(span.get("parent_span_id") or "")
            if parent is None or parent.get("kind") != "CLIENT":
                continue
            if not (_valid_interval(parent) and _valid_interval(span)):
                continue
            calls.append(
                {
                    "traceId": trace_id,
                    "clientSpanId": parent["span_id"],
                    "serverSpanId": span["span_id"],
                    "clientService": _service_of(parent),
                    "serverService": _service_of(span),
                    "clientName": parent.get("name") or "",
                    "serverName": span.get("name") or "",
                    "clientStartNs": int(parent["start_time_ns"]),
                    "clientEndNs": int(parent["end_time_ns"]),
                    "serverStartNs": int(span["start_time_ns"]),
                    "serverEndNs": int(span["end_time_ns"]),
                }
            )
    return calls


# ---------------------------------------------------------------------------
# Difference-constraint solver (Bellman-Ford)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Edge:
    """``offset[dst] <= offset[src] + weight`` contributed by one call."""

    src: int
    dst: int
    weight: int
    call_index: int
    kind: str  # "start" | "end"


def _negative_cycle(n_nodes: int, edges: list[_Edge]) -> Optional[list[_Edge]]:
    """Return the edges of one negative-weight cycle, or None if feasible."""

    if n_nodes == 0:
        return None
    dist = [0] * n_nodes  # super-source: every node starts at 0
    pred: list[Optional[int]] = [None] * n_nodes
    last: Optional[int] = None
    for _ in range(n_nodes):
        last = None
        for ei, edge in enumerate(edges):
            if dist[edge.src] + edge.weight < dist[edge.dst]:
                dist[edge.dst] = dist[edge.src] + edge.weight
                pred[edge.dst] = ei
                last = edge.dst
        if last is None:
            return None
    # `last` was relaxed on the n-th pass: walking predecessors n times lands
    # inside a negative cycle, then collect the cycle's edges.
    node = last
    for _ in range(n_nodes):
        node = edges[pred[node]].src  # type: ignore[index]
    cycle: list[int] = []
    start = node
    for _ in range(n_nodes + 1):
        ei = pred[node]
        if ei is None:  # pragma: no cover - defensive
            break
        cycle.append(ei)
        node = edges[ei].src
        if node == start:
            break
    cycle.reverse()
    return [edges[ei] for ei in cycle]


def _shortest_paths(n_nodes: int, edges: list[_Edge], source: int) -> list[Optional[int]]:
    """Shortest-path distances from ``source`` (None = unreachable).

    Only called once the graph is known to have no negative cycles.
    """

    dist: list[Optional[int]] = [None] * n_nodes
    dist[source] = 0
    for _ in range(max(0, n_nodes - 1)):
        changed = False
        for edge in edges:
            d = dist[edge.src]
            if d is None:
                continue
            cand = d + edge.weight
            if dist[edge.dst] is None or cand < dist[edge.dst]:
                dist[edge.dst] = cand
                changed = True
        if not changed:
            break
    return dist


# ---------------------------------------------------------------------------
# Diagnosis
# ---------------------------------------------------------------------------


def _target_json(ref: dict, span: dict) -> dict:
    return {
        "traceId": ref["traceId"],
        "spanId": ref["spanId"],
        "service": _service_of(span),
        "name": span.get("name") or "",
        "kind": span.get("kind") or "",
        "startTimeNs": _as_int(span.get("start_time_ns")),
        "endTimeNs": _as_int(span.get("end_time_ns")),
    }


def _fmt_ns(value: Optional[int], infinity: str) -> str:
    return infinity if value is None else f"{value} ns"


def build_diagnosis(traces: list[dict], spec: dict) -> dict:
    """Build the full diagnosis record from the snapshot traces and the spec.

    Raises :class:`TargetNotFound` when a target span id is not present in
    its trace.  The record is self-contained: conclusion, targets and every
    call pair that took part in the judgement, with raw nanosecond times.
    """

    spans_by_key: dict[tuple, dict] = {}
    for trace in traces:
        for span in trace.get("spans") or []:
            spans_by_key[(trace.get("trace_id"), span.get("span_id"))] = span

    target_spans = []
    for ref in spec["targets"]:
        span = spans_by_key.get((ref["traceId"], ref["spanId"]))
        if span is None:
            raise TargetNotFound(
                f"target span {ref['spanId']} not found in archived trace "
                f"{ref['traceId']}"
            )
        target_spans.append(span)

    calls = extract_call_pairs(traces)
    services = sorted(
        {c["clientService"] for c in calls}
        | {c["serverService"] for c in calls}
        | {_service_of(s) for s in target_spans}
    )
    index = {name: i for i, name in enumerate(services)}

    edges: list[_Edge] = []
    for i, call in enumerate(calls):
        client = index[call["clientService"]]
        server = index[call["serverService"]]
        # client.start + δc <= server.start + δs  ⇔  δc - δs <= serverStart - clientStart
        edges.append(
            _Edge(server, client, call["serverStartNs"] - call["clientStartNs"], i, "start")
        )
        # server.end + δs <= client.end + δc  ⇔  δs - δc <= clientEnd - serverEnd
        edges.append(
            _Edge(client, server, call["clientEndNs"] - call["serverEndNs"], i, "end")
        )

    record: dict[str, Any] = {
        "version": 1,
        "traceIds": list(spec["traceIds"]),
        "targets": {
            "a": _target_json(spec["targets"][0], target_spans[0]),
            "b": _target_json(spec["targets"][1], target_spans[1]),
        },
        "services": services,
        "calls": calls,
        "conclusion": INDETERMINATE,
        "reason": "",
        "rawStartDiffNs": None,
        "deltaBoundsNs": {"min": None, "max": None},
        "correctedStartDiffNs": {"min": None, "max": None},
        "conflict": None,
    }

    cycle = _negative_cycle(len(services), edges) if edges else None
    if cycle is not None:
        conflict_indices = sorted({e.call_index for e in cycle})
        record["conclusion"] = INFEASIBLE
        record["conflict"] = {
            "callIndices": conflict_indices,
            "edges": [
                {"callIndex": e.call_index, "constraint": e.kind, "weightNs": e.weight}
                for e in cycle
            ],
        }
        record["reason"] = (
            f"No single clock offset per service satisfies all {len(calls)} "
            f"CLIENT→SERVER containment constraint(s); the "
            f"{len(conflict_indices)} listed call(s) cannot hold simultaneously."
        )
        return record

    a_span, b_span = target_spans
    a_start = _as_int(a_span.get("start_time_ns"))
    b_start = _as_int(b_span.get("start_time_ns"))
    if a_start is None or b_start is None:
        which = "A" if a_start is None else "B"
        record["reason"] = (
            f"Target span {which} has no usable start time, so no order can be derived."
        )
        return record

    svc_a = index[_service_of(a_span)]
    svc_b = index[_service_of(b_span)]
    # δA - δB <= dist(B→A);  δA - δB >= -dist(A→B).  None means unbounded.
    max_delta = _shortest_paths(len(services), edges, svc_b)[svc_a]
    dist_ab = _shortest_paths(len(services), edges, svc_a)[svc_b]
    min_delta = -dist_ab if dist_ab is not None else None

    raw_diff = a_start - b_start
    lo = raw_diff + min_delta if min_delta is not None else None
    hi = raw_diff + max_delta if max_delta is not None else None
    record["rawStartDiffNs"] = raw_diff
    record["deltaBoundsNs"] = {"min": min_delta, "max": max_delta}
    record["correctedStartDiffNs"] = {"min": lo, "max": hi}

    if hi is not None and hi < 0:
        record["conclusion"] = A_FIRST
        record["reason"] = (
            "Under every feasible clock offset the corrected start of A precedes "
            f"the corrected start of B (corrected difference A−B is at most {hi} ns)."
        )
    elif lo is not None and lo > 0:
        record["conclusion"] = B_FIRST
        record["reason"] = (
            "Under every feasible clock offset the corrected start of B precedes "
            f"the corrected start of A (corrected difference A−B is at least {lo} ns)."
        )
    elif lo == 0 and hi == 0:
        record["conclusion"] = SIMULTANEOUS
        record["reason"] = (
            "The constraints pin the two corrected start times to the same instant "
            "under every feasible clock offset."
        )
    else:
        record["reason"] = (
            "Feasible clock offsets allow the corrected start difference A−B to "
            f"range from {_fmt_ns(lo, '-∞')} to {_fmt_ns(hi, '+∞')}, so the order "
            "is not determined."
        )
    return record


def run_diagnosis(store, spec: dict) -> dict:
    """Read the requested traces from one committed snapshot and diagnose.

    The snapshot is a single read transaction: every trace comes from the
    same committed state while archiving of new traces continues.  Nothing is
    persisted here; saving is the caller's job.
    """

    traces: list[dict] = []
    missing: list[str] = []
    with store.open_snapshot() as snapshot:
        for trace_id in spec["traceIds"]:
            trace = snapshot.read_trace(trace_id)
            if trace is None:
                missing.append(trace_id)
            else:
                traces.append(trace)
    if missing:
        raise TraceNotArchived(missing)
    return build_diagnosis(traces, spec)
