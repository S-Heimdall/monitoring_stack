"""provider 라우팅 — 서비스 클러스터 내부 LGTM으로 read-only query를 전달한다.

single_gateway는 Heimdall DB를 보지 않는다. upstream(Prometheus/Loki/Tempo) 주소는
환경변수로 주입되며(차트 values.gateway.upstreams), provider별 native query API로 변환해
호출한 뒤 결과를 §3.3.5.1 응답 형태(row_count/result_excerpt/rows)로 정규화한다.

rows는 provider-faithful한 raw 행이다(metric 라벨+값, 로그 라인, trace 요약).
RCA-semantic 변환(threshold/unit/downstream 등)은 게이트웨이 책임이 아니라 소비자
번역 계층이 담당한다. 소비자 Evidence MCP가 rows를 필수로 요구하므로 노출한다.
"""
from __future__ import annotations

import os
from datetime import datetime

import httpx

SUPPORTED = {"prometheus", "mimir", "loki", "tempo", "k8s_events"}
HTTP_TIMEOUT = float(os.environ.get("QUERY_TIMEOUT_SECONDS", "20"))


class ProviderError(Exception):
    """upstream 조회 실패 — 502 TELEMETRY_PROVIDER_QUERY_FAILED로 매핑."""


def _upstreams() -> dict[str, str]:
    # k8s 이벤트는 Loki에 적재되므로 loki upstream을 공유한다.
    loki = os.environ.get("UPSTREAM_LOKI", "heimdall-loki:3100")
    prom = os.environ.get("UPSTREAM_PROMETHEUS", "heimdall-kps-prometheus:9090")
    return {
        "prometheus": prom,
        "mimir": prom,
        "loki": loki,
        "k8s_events": loki,
        "tempo": os.environ.get("UPSTREAM_TEMPO", "heimdall-tempo:3200"),
    }


def _base(provider: str) -> str:
    return f"http://{_upstreams()[provider]}"


def _get(url: str, params: dict) -> httpx.Response:
    # 실제 upstream 호출 지점(테스트 monkeypatch 대상).
    return httpx.get(url, params=params, timeout=HTTP_TIMEOUT)


def _call(url: str, params: dict) -> dict:
    try:
        r = _get(url, params)
    except httpx.HTTPError as exc:
        raise ProviderError(str(exc)) from exc
    if r.status_code >= 400:
        raise ProviderError(f"upstream {r.status_code}: {r.text[:200]}")
    try:
        return r.json()
    except ValueError as exc:
        raise ProviderError(f"non-json upstream response: {exc}") from exc


def _step_seconds(start: datetime, end: datetime) -> int:
    span = max(1, int((end - start).total_seconds()))
    return max(15, span // 200)


def _labels(metric: dict) -> str:
    items = {k: v for k, v in metric.items() if k != "__name__"}
    if not items:
        return ""
    inner = ", ".join(f'{k}="{v}"' for k, v in list(items.items())[:4])
    return "{" + inner + "}"


def _num(value):
    # 숫자 문자열은 float으로, 그 외는 원형 보존.
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def _cap(limit: int | None, default: int) -> int:
    return limit if limit and limit > 0 else default


def _prometheus(provider: str, query: str, start: datetime, end: datetime, limit: int | None) -> dict:
    body = _call(f"{_base(provider)}/api/v1/query_range", {
        "query": query,
        "start": start.timestamp(),
        "end": end.timestamp(),
        "step": f"{_step_seconds(start, end)}s",
    })
    series = (body.get("data") or {}).get("result") or []
    excerpt = None
    if series:
        m = series[0].get("metric", {})
        name = m.get("__name__") or query
        pts = series[0].get("values") or []
        last = pts[-1][1] if pts else "?"
        excerpt = f"{name}{_labels(m)} = {last}"
    rows = []
    for s in series[: _cap(limit, 50)]:
        m = s.get("metric", {})
        pts = s.get("values") or []
        last_ts, last_val = pts[-1] if pts else (None, None)
        rows.append({
            "metric_name": m.get("__name__"),
            "service": m.get("service_name") or m.get("service"),
            "observed_value": _num(last_val),
            "labels": m,
            "timestamp": _num(last_ts),
        })
    return {"row_count": len(series), "result_excerpt": excerpt, "rows": rows}


def _loki(provider: str, query: str, start: datetime, end: datetime, limit: int | None) -> dict:
    body = _call(f"{_base(provider)}/loki/api/v1/query_range", {
        "query": query,
        "start": int(start.timestamp() * 1_000_000_000),
        "end": int(end.timestamp() * 1_000_000_000),
        "limit": limit or 100,
        "direction": "backward",
    })
    streams = (body.get("data") or {}).get("result") or []
    row_count = sum(len(s.get("values") or []) for s in streams)
    excerpt = None
    for s in streams:
        vals = s.get("values") or []
        if vals:
            excerpt = str(vals[0][1])[:200]
            break
    cap = _cap(limit, 100)
    rows = []
    for s in streams:
        st = s.get("stream", {})
        for entry in s.get("values") or []:
            if len(rows) >= cap:
                break
            ts, line = entry[0], entry[1]
            rows.append({
                "message": line,
                "service": st.get("service_name") or st.get("service"),
                "level": st.get("level") or st.get("detected_level"),
                "labels": st,
                "timestamp": ts,
            })
        if len(rows) >= cap:
            break
    return {"row_count": row_count, "result_excerpt": excerpt, "rows": rows}


def _tempo(query: str, start: datetime, end: datetime, limit: int | None) -> dict:
    body = _call(f"{_base('tempo')}/api/search", {
        "q": query,
        "start": int(start.timestamp()),
        "end": int(end.timestamp()),
        "limit": limit or 20,
    })
    traces = body.get("traces") or []
    excerpt = None
    if traces:
        t = traces[0]
        excerpt = f"{t.get('rootServiceName', '?')} {t.get('traceID', '')}".strip()[:200]
    rows = [{
        "span": t.get("rootTraceName"),
        "service": t.get("rootServiceName"),
        "trace_id": t.get("traceID"),
        "duration_ms": t.get("durationMs"),
    } for t in traces[: _cap(limit, 20)]]
    return {"row_count": len(traces), "result_excerpt": excerpt, "rows": rows}


def run_query(provider: str, query: str, start: datetime, end: datetime, limit: int | None) -> dict:
    """provider native query 실행 후 {row_count, result_excerpt, rows} 반환."""
    if provider in ("prometheus", "mimir"):
        return _prometheus(provider, query, start, end, limit)
    if provider in ("loki", "k8s_events"):
        return _loki(provider, query, start, end, limit)
    if provider == "tempo":
        return _tempo(query, start, end, limit)
    raise ProviderError(f"unsupported provider: {provider}")


# ── 등록 verify(§3.3.5) probe ─────────────────────────────────────────
# monitoring-stack/verify가 single_gateway base에 때리는 provider 도달성 경로.
# 무인증으로 upstream health/route에 라우팅해 routing 가능 여부만 증명한다.
PROBE_ROUTES = {
    "/-/healthy": ("prometheus", "/-/healthy"),
    "/loki/api/v1/labels": ("loki", "/loki/api/v1/labels"),
    "/api/echo": ("tempo", "/api/echo"),
    "/api/v1/events": ("k8s_events", "/loki/api/v1/labels"),
}


def probe(path: str) -> httpx.Response:
    provider, upstream_path = PROBE_ROUTES[path]
    try:
        return _get(f"{_base(provider)}{upstream_path}", {})
    except httpx.HTTPError as exc:
        raise ProviderError(str(exc)) from exc
