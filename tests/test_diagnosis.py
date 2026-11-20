from __future__ import annotations

import json
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402
from app.models import SpanRecord  # noqa: E402
from app.storage import ArchiveStore  # noqa: E402

INTERNAL, SERVER, CLIENT = 1, 2, 3


def tid(n: int) -> str:
    return f"{n:032x}"


def sid(n: int) -> str:
    return f"{n:016x}"


def otlp_payload(spans) -> dict:
    """Build an OTLP/HTTP JSON export; ``start``/``end`` of None omit the field."""

    groups: dict[tuple, list] = {}
    for item in spans:
        key = (item.get("service", "checkout"), item.get("scope", "test"))
        span = {
            "traceId": item["trace"],
            "spanId": item["span"],
            "name": item.get("name", "op"),
            "kind": item.get("kind", SERVER),
            "status": {"code": 2, "message": "boom"} if item.get("error") else {"code": 1},
        }
        if item.get("start") is not None:
            span["startTimeUnixNano"] = str(item["start"])
        if item.get("end") is not None:
            span["endTimeUnixNano"] = str(item["end"])
        if item.get("parent"):
            span["parentSpanId"] = item["parent"]
        groups.setdefault(key, []).append(span)

    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service}}
                    ]
                },
                "scopeSpans": [{"scope": {"name": scope}, "spans": group}],
            }
            for (service, scope), group in groups.items()
        ]
    }


class Clock:
    def __init__(self) -> None:
        self.now = 5000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock():
    return Clock()


@pytest.fixture()
def client(tmp_path, clock):
    app = create_app(
        str(tmp_path / "traces.db"),
        window_seconds=2.0,
        capacity=16,
        max_spans_per_trace=64,
        start_sampler_thread=False,
        clock=clock,
    )
    with TestClient(app) as test_client:
        test_client.app_state = app.state
        yield test_client
    app.state.store.close()


def post(client, spans):
    return client.post(
        "/v1/traces",
        content=json.dumps(otlp_payload(spans)),
        headers={"content-type": "application/json"},
    )


def archive(client, clock, spans):
    """Ingest spans and close the window so error traces hit the archive."""

    response = post(client, spans)
    assert response.status_code == 200
    clock.advance(2.0)
    client.app_state.sampler.tick()


def diagnose(client, trace_ids, target_a, target_b):
    return client.post(
        "/api/diagnoses",
        json={"traceIds": trace_ids, "targets": [target_a, target_b]},
    )


# ---------------------------------------------------------------------------
# Conclusion logic
# ---------------------------------------------------------------------------


def test_definite_order_under_all_feasible_offsets(client, clock):
    """One CLIENT→SERVER call bounds δ(payments)−δ(gateway) to ±1000 ns; the
    raw start gap of 2000 ns then makes A strictly first under every offset."""

    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": CLIENT, "service": "gateway",
             "name": "POST /pay", "start": 1000, "end": 9000},
            {"trace": tid(1), "span": sid(12), "parent": sid(11), "kind": SERVER,
             "service": "payments", "name": "charge", "start": 2000, "end": 8000,
             "error": True},
            {"trace": tid(1), "span": sid(13), "kind": INTERNAL, "service": "gateway",
             "name": "gateway retry", "start": 5000, "end": 5100},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "name": "first debit", "start": 7000, "end": 7100, "error": True},
        ],
    )

    response = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(13)},
        {"traceId": tid(2), "spanId": sid(21)},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["id"] >= 1
    assert body["conclusion"] == "a_first"
    assert body["rawStartDiffNs"] == -2000
    assert body["deltaBoundsNs"] == {"min": -1000, "max": 1000}
    assert body["correctedStartDiffNs"] == {"min": -3000, "max": -1000}

    # Evidence: the two targets and the one call pair, with raw nanoseconds.
    assert body["targets"]["a"]["spanId"] == sid(13)
    assert body["targets"]["a"]["startTimeNs"] == 5000
    assert body["targets"]["b"]["spanId"] == sid(21)
    assert body["targets"]["b"]["startTimeNs"] == 7000
    assert body["calls"] == [
        {
            "traceId": tid(1),
            "clientSpanId": sid(11),
            "serverSpanId": sid(12),
            "clientService": "gateway",
            "serverService": "payments",
            "clientName": "POST /pay",
            "serverName": "charge",
            "clientStartNs": 1000,
            "clientEndNs": 9000,
            "serverStartNs": 2000,
            "serverEndNs": 8000,
        }
    ]
    assert body["conflict"] is None

    # The same record is served by number, over JSON and HTML.
    again = client.get(f"/api/diagnoses/{body['id']}")
    assert again.status_code == 200
    assert again.json() == body
    page = client.get(f"/diagnoses/{body['id']}")
    assert page.status_code == 200
    assert "A started first" in page.text
    assert sid(11) in page.text and sid(12) in page.text
    assert "1000" in page.text and "8000" in page.text

    listing = client.get("/api/diagnoses").json()
    assert listing["total"] == 1
    assert listing["diagnoses"][0]["id"] == body["id"]
    assert listing["diagnoses"][0]["traceIds"] == [tid(1), tid(2)]


def test_indeterminate_when_offsets_unconstrained(client, clock):
    """No CLIENT→SERVER pairs: any offset is feasible, so the order of two
    spans from different services cannot be determined."""

    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": INTERNAL, "service": "gateway",
             "start": 5000, "end": 5100, "error": True},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "start": 7000, "end": 7100, "error": True},
        ],
    )
    body = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(11)},
        {"traceId": tid(2), "spanId": sid(21)},
    ).json()
    assert body["conclusion"] == "indeterminate"
    assert body["deltaBoundsNs"] == {"min": None, "max": None}
    assert body["calls"] == []
    page = client.get(f"/diagnoses/{body['id']}")
    assert page.status_code == 200
    assert "Cannot be determined" in page.text


def test_infeasible_constraints_list_conflicting_calls(client, clock):
    """Two calls demand δ(payments)−δ(gateway) in [-1000,1000] and
    [-4000,-3000] at once: no offset satisfies both."""

    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": CLIENT, "service": "gateway",
             "start": 1000, "end": 9000},
            {"trace": tid(1), "span": sid(12), "parent": sid(11), "kind": SERVER,
             "service": "payments", "start": 2000, "end": 8000},
            {"trace": tid(1), "span": sid(13), "kind": CLIENT, "service": "gateway",
             "start": 1000, "end": 9000},
            {"trace": tid(1), "span": sid(14), "parent": sid(13), "kind": SERVER,
             "service": "payments", "start": 5000, "end": 12000, "error": True},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "gateway",
             "start": 100, "end": 200, "error": True},
        ],
    )
    body = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(11)},
        {"traceId": tid(2), "spanId": sid(21)},
    ).json()
    assert body["conclusion"] == "infeasible"
    assert body["conflict"]["callIndices"] == [0, 1]
    assert len(body["calls"]) == 2
    # The conflicting calls are identifiable and carry raw nanosecond times.
    involved = {body["calls"][i]["serverSpanId"] for i in body["conflict"]["callIndices"]}
    assert involved == {sid(12), sid(14)}
    edge_pairs = {(e["callIndex"], e["constraint"]) for e in body["conflict"]["edges"]}
    assert edge_pairs == {(0, "start"), (1, "end")}

    page = client.get(f"/diagnoses/{body['id']}")
    assert "cannot hold simultaneously" in page.text


def test_simultaneous_when_offset_is_pinned(client, clock):
    """The call forces δ(payments)−δ(gateway) = 500 exactly, and the raw
    starts differ by exactly that, so the corrected starts always coincide."""

    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": CLIENT, "service": "gateway",
             "start": 1000, "end": 9000},
            {"trace": tid(1), "span": sid(12), "parent": sid(11), "kind": SERVER,
             "service": "payments", "start": 500, "end": 8500, "error": True},
            {"trace": tid(1), "span": sid(13), "kind": INTERNAL, "service": "gateway",
             "start": 5000, "end": 5100},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "start": 4500, "end": 4600, "error": True},
        ],
    )
    body = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(13)},
        {"traceId": tid(2), "spanId": sid(21)},
    ).json()
    assert body["conclusion"] == "simultaneous"
    # δA−δB = δ(gateway)−δ(payments) is pinned to −500, cancelling the +500
    # raw start difference, so the corrected starts always coincide.
    assert body["deltaBoundsNs"] == {"min": -500, "max": -500}
    assert body["correctedStartDiffNs"] == {"min": 0, "max": 0}


def test_same_service_targets_compare_raw_starts(client, clock):
    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": INTERNAL, "service": "gateway",
             "start": 5000, "end": 5100, "error": True},
            {"trace": tid(1), "span": sid(12), "kind": INTERNAL, "service": "gateway",
             "start": 6000, "end": 6100},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "start": 1, "end": 2, "error": True},
        ],
    )
    body = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(11)},
        {"traceId": tid(1), "spanId": sid(12)},
    ).json()
    assert body["conclusion"] == "a_first"
    # Swapping the targets flips the conclusion.
    body = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(12)},
        {"traceId": tid(1), "spanId": sid(11)},
    ).json()
    assert body["conclusion"] == "b_first"


def test_constraints_ignore_unusable_calls(client, clock):
    """Only direct CLIENT→SERVER parent-child calls with complete, valid
    intervals constrain the clocks; everything else is ignored."""

    archive(
        client,
        clock,
        [
            # server parent / client child: not a CLIENT→SERVER call
            {"trace": tid(1), "span": sid(11), "kind": SERVER, "service": "gateway",
             "start": 1000, "end": 9000},
            {"trace": tid(1), "span": sid(12), "parent": sid(11), "kind": CLIENT,
             "service": "payments", "start": 2000, "end": 8000},
            # CLIENT→SERVER but the server has no end time
            {"trace": tid(1), "span": sid(13), "kind": CLIENT, "service": "gateway",
             "start": 1000, "end": 9000},
            {"trace": tid(1), "span": sid(14), "parent": sid(13), "kind": SERVER,
             "service": "payments", "start": 2000, "end": None},
            # CLIENT→SERVER but the client interval is invalid (end < start)
            {"trace": tid(1), "span": sid(15), "kind": CLIENT, "service": "gateway",
             "start": 9000, "end": 1000},
            {"trace": tid(1), "span": sid(16), "parent": sid(15), "kind": SERVER,
             "service": "payments", "start": 2000, "end": 8000, "error": True},
            {"trace": tid(1), "span": sid(17), "kind": INTERNAL, "service": "gateway",
             "start": 5000, "end": 5100},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "start": 7000, "end": 7100, "error": True},
        ],
    )
    body = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(17)},
        {"traceId": tid(2), "spanId": sid(21)},
    ).json()
    assert body["calls"] == []
    assert body["conclusion"] == "indeterminate"


def test_target_without_start_time_is_indeterminate(client, clock):
    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": INTERNAL, "service": "gateway",
             "start": None, "end": None, "error": True},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "start": 7000, "end": 7100, "error": True},
        ],
    )
    response = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(11)},
        {"traceId": tid(2), "spanId": sid(21)},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["conclusion"] == "indeterminate"
    assert "no usable start time" in body["reason"]
    assert body["targets"]["a"]["startTimeNs"] is None


# ---------------------------------------------------------------------------
# Committed-state reads and persistence
# ---------------------------------------------------------------------------


def test_diagnosis_reads_only_committed_traces(client, clock):
    """A trace still inside its collection window is invisible to the
    diagnosis; once committed it works, and archiving continues afterwards."""

    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": INTERNAL, "service": "gateway",
             "start": 5000, "end": 5100, "error": True},
        ],
    )
    # Trace 2 is still collecting (window open) when the diagnosis runs.
    post(
        client,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "start": 7000, "end": 7100, "error": True},
        ],
    )
    response = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(11)},
        {"traceId": tid(2), "spanId": sid(21)},
    )
    assert response.status_code == 404
    assert tid(2) in response.json()["error"]
    assert client.get("/api/diagnoses").json()["total"] == 0

    # After the commit lands the same request succeeds...
    clock.advance(2.0)
    client.app_state.sampler.tick()
    response = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(11)},
        {"traceId": tid(2), "spanId": sid(21)},
    )
    assert response.status_code == 201

    # ...and new traces keep being archived as usual.
    archive(
        client,
        clock,
        [
            {"trace": tid(3), "span": sid(31), "kind": INTERNAL, "service": "gateway",
             "start": 1, "end": 2, "error": True},
        ],
    )
    assert client.get("/api/status").json()["archivedTraces"] == 3


def test_diagnosis_survives_archive_replacement_and_restart(tmp_path):
    clock = Clock()
    db_path = str(tmp_path / "traces.db")
    app = create_app(
        db_path,
        window_seconds=2.0,
        capacity=16,
        max_spans_per_trace=64,
        start_sampler_thread=False,
        clock=clock,
    )
    with TestClient(app) as client:
        client.app_state = app.state
        archive(
            client,
            clock,
            [
                {"trace": tid(1), "span": sid(11), "kind": CLIENT, "service": "gateway",
                 "start": 1000, "end": 9000},
                {"trace": tid(1), "span": sid(12), "parent": sid(11), "kind": SERVER,
                 "service": "payments", "start": 2000, "end": 8000, "error": True},
                {"trace": tid(1), "span": sid(13), "kind": INTERNAL, "service": "gateway",
                 "start": 5000, "end": 5100},
            ],
        )
        archive(
            client,
            clock,
            [
                {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
                 "start": 7000, "end": 7100, "error": True},
            ],
        )
        created = diagnose(
            client,
            [tid(1), tid(2)],
            {"traceId": tid(1), "spanId": sid(13)},
            {"traceId": tid(2), "spanId": sid(21)},
        ).json()
        diagnosis_id = created["id"]

        # The archived trace is replaced wholesale under the same trace id.
        replacement = SpanRecord(
            trace_id=tid(1),
            span_id=sid(99),
            parent_span_id=None,
            name="rewritten",
            kind="INTERNAL",
            status_code="ERROR",
            status_message="",
            is_error=True,
            start_time_ns="111",
            end_time_ns="222",
            duration_ns=111,
            service_name="gateway",
            event_count=0,
            link_count=0,
            resource_json="{}",
            scope_json="{}",
            attributes_json="{}",
            raw_json="{}",
        )
        app.state.store.write_trace(tid(1), [replacement])
        assert [s["span_id"] for s in app.state.store.get_trace(tid(1))["spans"]] == [sid(99)]

        # The diagnosis still shows the original conclusion and evidence.
        kept = client.get(f"/api/diagnoses/{diagnosis_id}").json()
        assert kept == created
        assert kept["calls"][0]["clientSpanId"] == sid(11)

    # After a full restart the diagnosis id still opens the original record.
    app2 = create_app(db_path, start_sampler_thread=False, clock=clock)
    with TestClient(app2) as client2:
        reopened = client2.get(f"/api/diagnoses/{diagnosis_id}")
        assert reopened.status_code == 200
        assert reopened.json() == created
        page = client2.get(f"/diagnoses/{diagnosis_id}")
        assert page.status_code == 200
        assert "A started first" in page.text
        assert sid(11) in page.text  # original evidence, not the replacement
    app2.state.store.close()


def test_snapshot_reads_one_committed_state_while_writes_continue(tmp_path):
    """A snapshot opened before a concurrent rewrite keeps seeing the old
    committed version of every trace, and the writer is not blocked."""

    store = ArchiveStore(str(tmp_path / "traces.db"))

    def record(trace_id, span_id, name):
        return SpanRecord(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=None,
            name=name,
            kind="SERVER",
            status_code="ERROR",
            status_message="",
            is_error=True,
            start_time_ns="1",
            end_time_ns="2",
            duration_ns=1,
            service_name="svc",
            event_count=0,
            link_count=0,
            resource_json="{}",
            scope_json="{}",
            attributes_json="{}",
            raw_json="{}",
        )

    store.write_trace(tid(1), [record(tid(1), sid(1), "t1-v1")])
    store.write_trace(tid(2), [record(tid(2), sid(2), "t2-v1")])

    snapshot = store.open_snapshot()
    assert snapshot.read_trace(tid(1))["spans"][0]["name"] == "t1-v1"
    # The writer commits a replacement while the snapshot is open.
    store.write_trace(tid(2), [record(tid(2), sid(3), "t2-v2")])
    # The snapshot still reads the committed state it started from.
    assert snapshot.read_trace(tid(2))["spans"][0]["name"] == "t2-v1"
    snapshot.close()

    fresh = store.open_snapshot()
    assert fresh.read_trace(tid(2))["spans"][0]["name"] == "t2-v2"
    fresh.close()
    store.close()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_diagnosis_input_validation(client, clock):
    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": INTERNAL, "service": "gateway",
             "start": 5000, "end": 5100, "error": True},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "start": 7000, "end": 7100, "error": True},
        ],
    )
    target_a = {"traceId": tid(1), "spanId": sid(11)}
    target_b = {"traceId": tid(2), "spanId": sid(21)}

    def bad(payload, fragment):
        response = client.post("/api/diagnoses", json=payload)
        assert response.status_code == 400, payload
        assert fragment in response.json()["error"]

    bad({"traceIds": [tid(1)], "targets": [target_a, target_b]}, "between 2 and 8")
    bad(
        {"traceIds": [tid(i) for i in range(1, 11)], "targets": [target_a, target_b]},
        "between 2 and 8",
    )
    bad({"traceIds": [tid(1), tid(1)], "targets": [target_a, target_b]}, "between 2 and 8")
    bad({"traceIds": ["nope", tid(2)], "targets": [target_a, target_b]}, "invalid traceId")
    bad(
        {"traceIds": [tid(1), tid(2)], "targets": [target_a]},
        "exactly two",
    )
    bad(
        {"traceIds": [tid(1), tid(2)],
         "targets": [{"traceId": tid(3), "spanId": sid(1)}, target_b]},
        "not among the selected",
    )
    bad(
        {"traceIds": [tid(1), tid(2)],
         "targets": [{"traceId": tid(1), "spanId": "xyz"}, target_b]},
        "invalid target spanId",
    )
    bad(
        {"traceIds": [tid(1), tid(2)], "targets": [target_a, target_a]},
        "different spans",
    )
    bad(
        {"traceIds": [tid(1), tid(2)],
         "targets": [{"traceId": tid(1), "spanId": sid(99)}, target_b]},
        "not found",
    )
    # Nothing was saved for the rejected requests.
    assert client.get("/api/diagnoses").json()["total"] == 0

    response = client.post("/api/diagnoses", content=b"{not json")
    assert response.status_code == 400


def test_unknown_diagnosis_id_is_404(client):
    assert client.get("/api/diagnoses/4242").status_code == 404
    assert client.get("/diagnoses/4242").status_code == 404
    assert "does not exist" in client.get("/diagnoses/4242").text


def test_diagnosis_number_returned_only_after_successful_save(client, clock, monkeypatch):
    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": INTERNAL, "service": "gateway",
             "start": 5000, "end": 5100, "error": True},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "start": 7000, "end": 7100, "error": True},
        ],
    )

    def boom(record):
        raise RuntimeError("disk full")

    monkeypatch.setattr(client.app_state.store, "save_diagnosis", boom)
    response = diagnose(
        client,
        [tid(1), tid(2)],
        {"traceId": tid(1), "spanId": sid(11)},
        {"traceId": tid(2), "spanId": sid(21)},
    )
    assert response.status_code == 500
    assert "id" not in response.json()
    # Nothing was persisted, so no diagnosis number exists afterwards either.
    assert client.get("/api/diagnoses").json()["total"] == 0


def test_spec_parsing_boundaries():
    from app.diagnosis import DiagnosisInputError, parse_diagnosis_spec

    def spec(n):
        ids = [tid(i) for i in range(1, n + 1)]
        return {
            "traceIds": ids,
            "targets": [
                {"traceId": ids[0] if ids else tid(1), "spanId": sid(1)},
                {"traceId": ids[-1] if ids else tid(2), "spanId": sid(2)},
            ],
        }

    for count in (2, 8):
        parsed = parse_diagnosis_spec(spec(count))
        assert len(parsed["traceIds"]) == count
    for count in (0, 1, 9):
        with pytest.raises(DiagnosisInputError):
            parse_diagnosis_spec(spec(count))
    # Duplicates collapse before the count check.
    body = spec(2)
    body["traceIds"] = body["traceIds"] + body["traceIds"]
    assert len(parse_diagnosis_spec(body)["traceIds"]) == 2
    # Ids are normalised to lowercase.
    body = spec(2)
    body["traceIds"][0] = body["traceIds"][0].upper()
    body["targets"][0]["traceId"] = body["targets"][0]["traceId"].upper()
    parsed = parse_diagnosis_spec(body)
    assert parsed["traceIds"][0] == tid(1)
    assert parsed["targets"][0]["traceId"] == tid(1)


# ---------------------------------------------------------------------------
# HTML flow
# ---------------------------------------------------------------------------


def test_html_diagnosis_flow(client, clock):
    archive(
        client,
        clock,
        [
            {"trace": tid(1), "span": sid(11), "kind": CLIENT, "service": "gateway",
             "start": 1000, "end": 9000},
            {"trace": tid(1), "span": sid(12), "parent": sid(11), "kind": SERVER,
             "service": "payments", "start": 2000, "end": 8000, "error": True},
            {"trace": tid(1), "span": sid(13), "kind": INTERNAL, "service": "gateway",
             "start": 5000, "end": 5100},
        ],
    )
    archive(
        client,
        clock,
        [
            {"trace": tid(2), "span": sid(21), "kind": INTERNAL, "service": "payments",
             "start": 7000, "end": 7100, "error": True},
        ],
    )

    index = client.get("/")
    assert "Run diagnosis" in index.text

    response = client.post(
        "/diagnoses",
        data={
            "trace_ids": f"{tid(1)}\n{tid(2)}",
            "target_a_trace": tid(1),
            "target_a_span": sid(13),
            "target_b_trace": tid(2),
            "target_b_span": sid(21),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    location = response.headers["location"]
    assert location.startswith("/diagnoses/")

    page = client.get(location)
    assert page.status_code == 200
    assert "A started first" in page.text
    assert sid(13) in page.text and sid(21) in page.text
    assert "5000" in page.text and "7000" in page.text  # raw target starts
    assert sid(11) in page.text and sid(12) in page.text  # evidence call pair

    # The new diagnosis is listed on the overview.
    assert location in client.get("/").text

    # A bad form submission re-renders the overview with the error.
    response = client.post(
        "/diagnoses",
        data={
            "trace_ids": tid(1),
            "target_a_trace": tid(1),
            "target_a_span": sid(13),
            "target_b_trace": tid(2),
            "target_b_span": sid(21),
        },
    )
    assert response.status_code == 400
    assert "between 2 and 8" in response.text
