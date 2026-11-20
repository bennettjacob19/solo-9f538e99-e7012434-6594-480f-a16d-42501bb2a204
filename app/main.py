"""FastAPI tail-sampling collector.

Run with ``uvicorn app.main:app``.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import Callable, Optional
from urllib.parse import parse_qs

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from .diagnosis import (
    CONCLUSION_LABELS,
    CONCLUSION_TONES,
    DiagnosisInputError,
    TargetNotFound,
    TraceNotArchived,
    parse_diagnosis_spec,
    run_diagnosis,
)
from .otlp import OtlpParseError, export_response, parse_trace_request
from .sampler import (
    DEFAULT_CAPACITY,
    DEFAULT_MAX_SPANS,
    DEFAULT_WINDOW_SECONDS,
    TraceSampler,
)
from .storage import ArchiveStore

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("tailsampler")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "..", "data", "traces.db")
MAX_BODY_BYTES = int(os.environ.get("MAX_BODY_BYTES", str(8 * 1024 * 1024)))

# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _pretty_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _format_duration(ns) -> str:
    if ns is None:
        return "—"
    try:
        ns = int(ns)
    except (TypeError, ValueError):
        return "—"
    if ns < 1_000:
        return f"{ns} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.1f} µs"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    return f"{ns / 1_000_000_000:.3f} s"


def _format_time(ns, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    if ns is None:
        return "—"
    try:
        stamp = datetime.datetime.fromtimestamp(
            int(ns) / 1_000_000_000, tz=datetime.timezone.utc
        )
    except (TypeError, ValueError, OSError, OverflowError):
        return "—"
    # Only add sub-second precision when the format asks for it.
    return stamp.strftime(fmt) + (" UTC" if "%f" not in fmt else "")


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, ""))
    except ValueError:
        return default
    return value if value > 0 else default


def _env_int(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, ""))
    except ValueError:
        return default
    return value if value > 0 else default


def _fmt_bound(value, infinity: str) -> str:
    """Render an offset/diff bound; None means unbounded in that direction."""

    return infinity if value is None else f"{value} ns"


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app(
    db_path: Optional[str] = None,
    *,
    window_seconds: Optional[float] = None,
    capacity: Optional[int] = None,
    max_spans_per_trace: Optional[int] = None,
    start_sampler_thread: bool = True,
    clock: Optional[Callable[[], float]] = None,
) -> FastAPI:
    db_path = db_path or os.environ.get("DB_PATH", DEFAULT_DB_PATH)
    store = ArchiveStore(db_path)
    sampler = TraceSampler(
        store,
        window_seconds=window_seconds
        if window_seconds is not None
        else _env_float("SAMPLE_WINDOW_SECONDS", DEFAULT_WINDOW_SECONDS),
        capacity=capacity
        if capacity is not None
        else _env_int("MAX_ACTIVE_TRACES", DEFAULT_CAPACITY),
        max_spans_per_trace=max_spans_per_trace
        if max_spans_per_trace is not None
        else _env_int("MAX_SPANS_PER_TRACE", DEFAULT_MAX_SPANS),
        clock=clock,
    )
    templates = Jinja2Templates(directory=TEMPLATE_DIR)
    templates.env.filters["pretty_json"] = _pretty_json
    templates.env.filters["duration"] = _format_duration
    templates.env.filters["utc_time"] = _format_time
    templates.env.filters["conclusion_label"] = lambda c: CONCLUSION_LABELS.get(c, c)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        if start_sampler_thread:
            sampler.start()
        log.info(
            "tail sampler ready db=%s window=%.2fs capacity=%d max_spans=%d",
            db_path,
            sampler.window_seconds,
            sampler.capacity,
            sampler.max_spans_per_trace,
        )
        try:
            yield
        finally:
            sampler.stop()
            store.close()

    app = FastAPI(title="OTLP Tail Sampler", lifespan=lifespan)
    app.state.store = store
    app.state.sampler = sampler

    def render(request: Request, name: str, context: dict, status_code: int = 200):
        return templates.TemplateResponse(
            request, name, context, status_code=status_code
        )

    def page_context(request: Request, **extra) -> dict:
        context = {"status": sampler.snapshot(), "request": request}
        context.update(extra)
        return context

    def flush_due_writes() -> None:
        """Run due archive writes inline so a page reflects just-finished work."""
        sampler.tick()

    def overview(
        request: Request, status_code: int = 200, **extra
    ) -> HTMLResponse:
        traces, total = store.list_traces(limit=50, offset=0)
        diagnoses, _ = store.list_diagnoses(limit=20)
        context = page_context(
            request,
            traces=traces,
            total=total,
            limit=50,
            offset=0,
            diagnoses=diagnoses,
            diagnosis_error=None,
        )
        context.update(extra)
        return render(request, "index.html", context, status_code=status_code)

    # -- OTLP ingest --------------------------------------------------------
    @app.post("/v1/traces")
    async def ingest_traces(request: Request) -> Response:
        content_type = request.headers.get("content-type", "").lower()
        if "protobuf" in content_type:
            return JSONResponse(
                status_code=415,
                content=export_response(
                    0,
                    "only OTLP/HTTP JSON is supported; "
                    "send Content-Type: application/json",
                ),
            )
        body = await request.body()
        if len(body) > MAX_BODY_BYTES:
            return JSONResponse(
                status_code=413,
                content=export_response(0, "request body too large"),
            )
        try:
            spans, invalid = parse_trace_request(body)
        except OtlpParseError as exc:
            # Malformed export: nothing was accepted, all spans implied lost.
            return JSONResponse(
                status_code=400, content=export_response(0, str(exc))
            )

        result = sampler.ingest(spans, invalid=invalid)
        if result.rejected:
            detail = ", ".join(
                f"{reason}={count}" for reason, count in sorted(result.reasons.items())
            )
            message = f"rejected {result.rejected} span(s): {detail}"
            log.info("export partial success: %s", message)
            return JSONResponse(
                status_code=200,
                content=export_response(
                    result.rejected, message, accepted=result.accepted
                ),
            )
        return JSONResponse(status_code=200, content=export_response(0))

    # -- HTML ---------------------------------------------------------------
    @app.get("/", response_class=HTMLResponse)
    async def index(
        request: Request, limit: int = Query(50, ge=1, le=200), offset: int = 0
    ):
        offset = max(0, offset)
        traces, total = store.list_traces(limit=limit, offset=offset)
        diagnoses, _ = store.list_diagnoses(limit=20)
        return render(
            request,
            "index.html",
            page_context(
                request,
                traces=traces,
                total=total,
                limit=limit,
                offset=offset,
                diagnoses=diagnoses,
                diagnosis_error=None,
            ),
        )

    @app.get("/traces/{trace_id}", response_class=HTMLResponse)
    async def trace_detail(request: Request, trace_id: str):
        trace = store.get_trace(trace_id)
        if trace is None:
            return render(
                request,
                "missing.html",
                page_context(request, trace_id=trace_id),
                status_code=404,
            )
        return render(request, "trace.html", page_context(request, trace=trace))

    # -- Diagnosis ----------------------------------------------------------
    def _diagnose_and_save(spec: dict) -> dict:
        """Run the diagnosis on one committed snapshot, then persist it.

        The diagnosis number is produced only when the save succeeds.
        """

        record = run_diagnosis(store, spec)
        diagnosis_id = store.save_diagnosis(record)
        saved = store.get_diagnosis(diagnosis_id)
        if saved is None:  # pragma: no cover - defensive
            raise RuntimeError("diagnosis was not persisted")
        return saved

    def _diagnosis_view(record: dict) -> dict:
        bounds = record.get("deltaBoundsNs") or {}
        corrected = record.get("correctedStartDiffNs") or {}
        view = {
            "label": CONCLUSION_LABELS.get(
                record.get("conclusion"), record.get("conclusion")
            ),
            "tone": CONCLUSION_TONES.get(record.get("conclusion"), ""),
            "min_delta": _fmt_bound(bounds.get("min"), "−∞"),
            "max_delta": _fmt_bound(bounds.get("max"), "+∞"),
            "min_corr": _fmt_bound(corrected.get("min"), "−∞"),
            "max_corr": _fmt_bound(corrected.get("max"), "+∞"),
            "conflict_indices": [],
            "conflict_calls": [],
        }
        conflict = record.get("conflict")
        if conflict:
            indices = [
                i
                for i in conflict.get("callIndices") or []
                if 0 <= i < len(record.get("calls") or [])
            ]
            view["conflict_indices"] = indices
            view["conflict_calls"] = [
                dict(record["calls"][i], index=i) for i in indices
            ]
        return view

    @app.post("/diagnoses", response_class=HTMLResponse)
    async def create_diagnosis_page(request: Request):
        form = parse_qs((await request.body()).decode("utf-8", "replace"))

        def field(name: str) -> str:
            return form.get(name, [""])[0]

        tokens = re.split(r"[\s,]+", field("trace_ids").strip())
        spec_body = {
            "traceIds": [token for token in tokens if token],
            "targets": [
                {"traceId": field("target_a_trace"), "spanId": field("target_a_span")},
                {"traceId": field("target_b_trace"), "spanId": field("target_b_span")},
            ],
        }
        try:
            spec = parse_diagnosis_spec(spec_body)
        except DiagnosisInputError as exc:
            return overview(request, status_code=400, diagnosis_error=str(exc))
        try:
            saved = _diagnose_and_save(spec)
        except TraceNotArchived as exc:
            return overview(request, status_code=404, diagnosis_error=str(exc))
        except TargetNotFound as exc:
            return overview(request, status_code=400, diagnosis_error=str(exc))
        except Exception:
            log.exception("diagnosis failed")
            return overview(
                request,
                status_code=500,
                diagnosis_error="diagnosis could not be computed or saved",
            )
        return RedirectResponse(f"/diagnoses/{saved['id']}", status_code=303)

    @app.get("/diagnoses/{diagnosis_id}", response_class=HTMLResponse)
    async def diagnosis_page(request: Request, diagnosis_id: int):
        record = store.get_diagnosis(diagnosis_id)
        if record is None:
            return render(
                request,
                "diagnosis_missing.html",
                page_context(request, diagnosis_id=diagnosis_id),
                status_code=404,
            )
        return render(
            request,
            "diagnosis.html",
            page_context(request, d=record, view=_diagnosis_view(record)),
        )

    @app.get("/api/diagnoses")
    async def api_diagnoses(limit: int = 50, offset: int = 0) -> dict:
        items, total = store.list_diagnoses(limit=limit, offset=offset)
        return {"total": total, "diagnoses": items}

    @app.post("/api/diagnoses")
    async def api_create_diagnosis(request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(
                status_code=400, content={"error": "request body must be JSON"}
            )
        try:
            spec = parse_diagnosis_spec(body)
        except DiagnosisInputError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        try:
            saved = _diagnose_and_save(spec)
        except TraceNotArchived as exc:
            return JSONResponse(status_code=404, content={"error": str(exc)})
        except TargetNotFound as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        except Exception:
            log.exception("diagnosis failed")
            return JSONResponse(
                status_code=500,
                content={"error": "diagnosis could not be computed or saved"},
            )
        # The diagnosis number is returned only after the save committed.
        return JSONResponse(status_code=201, content=saved)

    @app.get("/api/diagnoses/{diagnosis_id}")
    async def api_diagnosis(diagnosis_id: int):
        record = store.get_diagnosis(diagnosis_id)
        if record is None:
            return JSONResponse(
                status_code=404, content={"error": "no such diagnosis"}
            )
        return record

    @app.post("/pause", response_class=HTMLResponse)
    async def page_pause(request: Request):
        sampler.pause()
        return overview(request)

    @app.post("/resume", response_class=HTMLResponse)
    async def page_resume(request: Request):
        sampler.resume()
        flush_due_writes()
        return overview(request)

    # -- JSON API -----------------------------------------------------------
    @app.get("/api/status")
    async def api_status() -> dict:
        snapshot = sampler.snapshot()
        snapshot["archivedTraces"] = store.count()
        return snapshot

    @app.get("/api/traces")
    async def api_traces(limit: int = 100, offset: int = 0) -> dict:
        traces, total = store.list_traces(limit=limit, offset=offset)
        return {"total": total, "traces": traces}

    @app.get("/api/traces/{trace_id}")
    async def api_trace(trace_id: str):
        trace = store.get_trace(trace_id)
        if trace is None:
            return JSONResponse(status_code=404, content={"error": "not archived"})
        return trace

    @app.post("/api/archive/pause")
    async def api_pause() -> dict:
        sampler.pause()
        return {"paused": True}

    @app.post("/api/archive/resume")
    async def api_resume() -> dict:
        sampler.resume()
        flush_due_writes()
        return {"paused": False}

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    return app


app = create_app()
