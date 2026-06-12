"""single_gateway — Heimdall_API_기능_명세 §3.3.5.1 내부 Telemetry Query API.

서비스 클러스터(데모) 내부에 설치되어, AIOps telemetry-query-gateway가 PrivateLink로
호출하는 단일 진입점. provider로 내부 LGTM(Prometheus/Loki/Tempo)에 read-only query를
라우팅한다. 기존 3-prefix(/prometheus·/loki·/tempo) nginx read-gateway를 대체한다.

probe 경로는 등록 verify(§3.3.5)가 single_gateway 및 provider routing 도달성을 확인하는
용도로, verify가 토큰을 보내지 않으므로 무인증이다(read-only health/route).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse

from app import providers
from app.schemas import TelemetryQueryRequest, TelemetryQueryResponse

app = FastAPI(title="Heimdall single_gateway", version="0.2.0")

VALID_SIGNALS = {"metrics", "logs", "traces", "kubernetes_events"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _require_auth(authorization: str | None) -> None:
    token = os.environ.get("HEIMDALL_READ_TOKEN", "")
    if not token:
        # 토큰 미설정이면 인증 비활성(개발). 운영에선 secret으로 주입한다.
        return
    if authorization != f"Bearer {token}":
        raise HTTPException(status_code=403, detail={
            "code": "INTERNAL_CALL_FORBIDDEN",
            "message": "invalid or missing bearer token",
        })


@app.get("/healthz", include_in_schema=False)
def healthz() -> PlainTextResponse:
    return PlainTextResponse("ok\n")


@app.get("/", include_in_schema=False)
def root() -> dict:
    # verify의 single_gateway probe(handshake) 응답. 무인증.
    return {"data": {"service": "single_gateway", "status": "pass"}}


def _probe_handler(request: Request) -> Response:
    try:
        r = providers.probe(request.url.path)
    except providers.ProviderError as exc:
        raise HTTPException(status_code=502, detail={
            "code": "TELEMETRY_PROVIDER_QUERY_FAILED",
            "message": str(exc)[:200],
        })
    return Response(
        content=r.content,
        status_code=r.status_code,
        media_type=r.headers.get("content-type", "text/plain"),
    )


for _path in providers.PROBE_ROUTES:
    app.add_api_route(_path, _probe_handler, methods=["GET"], include_in_schema=False)


@app.post("/internal/telemetry/query", summary="내부 Telemetry Query API (§3.3.5.1)")
def telemetry_query(
    req: TelemetryQueryRequest,
    authorization: str | None = Header(default=None),
) -> dict:
    _require_auth(authorization)

    if req.signal not in VALID_SIGNALS:
        raise HTTPException(status_code=400, detail={
            "code": "TELEMETRY_QUERY_INVALID", "message": f"unknown signal: {req.signal}"})
    if req.provider not in providers.SUPPORTED:
        raise HTTPException(status_code=404, detail={
            "code": "TELEMETRY_CONNECTION_NOT_FOUND",
            "message": f"no connection for provider: {req.provider}"})
    if not req.query.strip():
        raise HTTPException(status_code=400, detail={
            "code": "TELEMETRY_QUERY_INVALID", "message": "empty query"})
    start, end = req.time_window.from_, req.time_window.to
    if start >= end:
        raise HTTPException(status_code=400, detail={
            "code": "TELEMETRY_QUERY_INVALID", "message": "time_window.from must be before to"})

    started = _now()
    try:
        result = providers.run_query(req.provider, req.query, start, end, req.limit)
    except providers.ProviderError as exc:
        raise HTTPException(status_code=502, detail={
            "code": "TELEMETRY_PROVIDER_QUERY_FAILED", "message": str(exc)[:200]})
    completed = _now()

    resp = TelemetryQueryResponse(
        provider=req.provider,
        status="pass",
        result_excerpt=result["result_excerpt"],
        row_count=result["row_count"],
        rows=result["rows"],
        started_at=started,
        completed_at=completed,
    )
    return {"data": resp.model_dump(mode="json")}
