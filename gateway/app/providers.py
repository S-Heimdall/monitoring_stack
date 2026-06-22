"""provider 라우팅 — 서비스 클러스터 내부 LGTM으로 read-only query를 전달한다.

single_gateway는 Heimdall DB를 보지 않는다. upstream(Prometheus/Loki/Tempo) 주소는
환경변수로 주입되며(차트 values.gateway.upstreams), provider별 native query API로 변환해
호출한 뒤 결과를 §3.3.5.1 응답 형태(row_count/result_excerpt/rows)로 정규화한다.

rows는 provider-faithful한 raw 행이다(metric 라벨+값, 로그 라인, trace 요약).
RCA-semantic 변환(threshold/unit/downstream 등)은 게이트웨이 책임이 아니라 소비자
번역 계층이 담당한다. 소비자 Evidence MCP가 rows를 필수로 요구하므로 노출한다.
"""
from __future__ import annotations

import json
import os
import re
import shlex
from datetime import datetime

import httpx

SUPPORTED = {"prometheus", "mimir", "loki", "tempo", "k8s_events", "kubernetes_log_signal", "kubernetes"}
HTTP_TIMEOUT = float(os.environ.get("QUERY_TIMEOUT_SECONDS", "20"))


class ProviderError(Exception):
    """upstream 조회 실패 — 502 TELEMETRY_PROVIDER_QUERY_FAILED로 매핑."""


class ProviderTimeoutError(ProviderError):
    """upstream timeout — telemetry error detail에서 provider_timeout으로 구분."""


LABEL_ALIASES = {
    "service_name": "service",
    "service": "service",
    "app": "service",
    "k8s_namespace_name": "namespace",
    "namespace": "namespace",
    "k8s_pod_name": "pod",
    "pod": "pod",
    "k8s_container_name": "container",
    "container": "container",
}
MISMATCH_PRONE_LABELS = {"service", "app", "namespace", "pod", "container"}


def _upstreams() -> dict[str, str]:
    # k8s_events는 Alloy kubernetes_events stream만 강제 조회한다. 일반 Loki 기반
    # Kubernetes-label 로그 signal은 kubernetes_log_signal provider로 분리한다.
    # Real Kubernetes API evidence는 provider="kubernetes"로 별도 조회한다.
    loki = os.environ.get("UPSTREAM_LOKI", "heimdall-loki:3100")
    prom = os.environ.get("UPSTREAM_PROMETHEUS", "heimdall-kps-prometheus:9090")
    return {
        "prometheus": prom,
        "mimir": prom,
        "loki": loki,
        "k8s_events": loki,
        "kubernetes_log_signal": loki,
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
    except httpx.TimeoutException as exc:
        raise ProviderTimeoutError(str(exc)) from exc
    except httpx.HTTPError as exc:
        raise ProviderError(str(exc)) from exc
    if r.status_code >= 400:
        raise ProviderError(f"upstream {r.status_code}: {r.text[:200]}")
    try:
        body = r.json()
    except ValueError as exc:
        raise ProviderError(f"non-json upstream response: {exc}") from exc
    if body.get("status") == "error":
        raise ProviderError(f"{body.get('errorType', 'error')}: {body.get('error', '')}"[:200])
    return body


def _kubernetes_get(path: str) -> dict:
    host = os.environ.get("KUBERNETES_SERVICE_HOST")
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    token_path = os.environ.get(
        "KUBERNETES_SERVICEACCOUNT_TOKEN",
        "/var/run/secrets/kubernetes.io/serviceaccount/token",
    )
    ca_path = os.environ.get(
        "KUBERNETES_SERVICEACCOUNT_CA",
        "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    )
    if not host:
        raise ProviderError("kubernetes API service env is not available")
    try:
        with open(token_path, encoding="utf-8") as fp:
            token = fp.read().strip()
    except OSError as exc:
        raise ProviderError(f"kubernetes service account token unavailable: {exc}") from exc
    verify: str | bool = ca_path if os.path.exists(ca_path) else True
    try:
        r = httpx.get(
            f"https://{host}:{port}{path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=HTTP_TIMEOUT,
            verify=verify,
        )
    except httpx.TimeoutException as exc:
        raise ProviderTimeoutError(str(exc)) from exc
    except httpx.HTTPError as exc:
        raise ProviderError(str(exc)) from exc
    if r.status_code >= 400:
        raise ProviderError(f"kubernetes API {r.status_code}: {r.text[:200]}")
    try:
        return r.json()
    except ValueError as exc:
        raise ProviderError(f"non-json kubernetes response: {exc}") from exc


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


def _service(labels: dict) -> str | None:
    return labels.get("service_name") or labels.get("service") or labels.get("app")


def _namespace(labels: dict) -> str | None:
    return labels.get("k8s_namespace_name") or labels.get("namespace")


def _pod(labels: dict) -> str | None:
    return labels.get("k8s_pod_name") or labels.get("pod")


def _container(labels: dict) -> str | None:
    return labels.get("k8s_container_name") or labels.get("container")


def _cap(limit: int | None, default: int) -> int:
    return limit if limit and limit > 0 else default


def normalize_query_labels(query: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for label, value in re.findall(r'([a-zA-Z_:][\w:]*)\s*(?:=|=~|!=|!~)\s*"([^"]*)"', query):
        labels.setdefault(LABEL_ALIASES.get(label, label), value)
    return labels


def _raw_query_labels(query: str) -> set[str]:
    return {
        label
        for label, _ in re.findall(r'([a-zA-Z_:][\w:]*)\s*(?:=|=~|!=|!~)\s*"([^"]*)"', query)
    }


def _label_mismatch_suspected(query: str) -> bool:
    return bool(_raw_query_labels(query) & MISMATCH_PRONE_LABELS)


def classify_result(provider: str, query: str, result: dict) -> tuple[str, str | None]:
    row_count = int(result.get("row_count") or 0)
    series_count = int(result.get("_series_count") or row_count)
    rows = result.get("rows") or []
    has_sampled_row = any(
        row.get("sample_count", 1) > 0 and row.get("observed_value", row.get("message", row.get("trace_id"))) is not None
        for row in rows
    )
    if row_count > 0 and has_sampled_row:
        return "ok", None
    if series_count > 0:
        return "empty", "no_rows_in_window"
    if _label_mismatch_suspected(query):
        return "empty", "label_mismatch_suspected"
    return "empty", "no_series_matched"


def error_empty_reason(exc: Exception) -> str:
    if isinstance(exc, ProviderTimeoutError):
        return "provider_timeout"
    message = str(exc).lower()
    if any(token in message for token in ("bad_data", "parse", "unsupported", "invalid parameter")):
        return "unsupported_query"
    return "unsupported_query"


def _prometheus(provider: str, query: str, start: datetime, end: datetime, limit: int | None) -> dict:
    step_seconds = _step_seconds(start, end)
    body = _call(f"{_base(provider)}/api/v1/query_range", {
        "query": query,
        "start": start.timestamp(),
        "end": end.timestamp(),
        "step": f"{step_seconds}s",
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
        # range samples 전체로 요약한다 — 마지막 1점만 보면 "지속됐는가"(sustained)를
        # 판단할 수 없어 RCA falsification이 single point라 inconclusive로 빠진다(HEIM-249).
        nums = [n for n in (_num(v) for _, v in pts) if isinstance(n, (int, float))]
        row = {
            "metric_name": m.get("__name__"),
            "service": _service(m),
            "namespace": _namespace(m),
            "pod": _pod(m),
            "container": _container(m),
            "labels": m,
            "sample_count": len(pts),
            "series_start": _num(pts[0][0]) if pts else None,
            "series_end": _num(pts[-1][0]) if pts else None,
            "timestamp": _num(pts[-1][0]) if pts else None,
        }
        if nums:
            # observed_value=last는 기존 소비자 호환용으로 유지하고, max/min/avg를 함께 노출한다.
            row["observed_value"] = nums[-1]
            row["last_value"] = nums[-1]
            row["max_value"] = max(nums)
            row["min_value"] = min(nums)
            row["avg_value"] = round(sum(nums) / len(nums), 6)
            row["latest"] = nums[-1]
            row["last"] = nums[-1]
            row["max"] = row["max_value"]
            row["min"] = row["min_value"]
            row["avg"] = row["avg_value"]
        else:
            row["observed_value"] = _num(pts[-1][1]) if pts else None
        rows.append(row)
    return {
        "row_count": len(series),
        "result_excerpt": excerpt,
        "rows": rows,
        "_series_count": len(series),
        "_step_seconds": step_seconds,
    }


def _loki(provider: str, query: str, start: datetime, end: datetime, limit: int | None) -> dict:
    body = _call(f"{_base(provider)}/loki/api/v1/query_range", {
        "query": query,
        "start": int(start.timestamp() * 1_000_000_000),
        "end": int(end.timestamp() * 1_000_000_000),
        "limit": limit or 100,
        "direction": "backward",
    })
    data = body.get("data") or {}
    result = data.get("result") or []
    # count_over_time 등 LogQL metric 쿼리는 matrix로 온다. 과거엔 이를 로그 스트림으로 오인해
    # message에 카운트 숫자를 넣고 시계열 버킷마다 행을 만들어 파편화됐다(HEIM-254). matrix는
    # prometheus처럼 series당 1행 요약, streams는 실제 로그 라인만 돌려준다.
    if data.get("resultType") == "matrix":
        return _loki_matrix(query, result, limit)
    return _loki_streams(result, limit)


def _event_logql(query: str) -> str:
    """Force k8s_events to Alloy's kubernetes event stream, never pod logs."""
    stripped = query.strip()
    if not stripped.startswith("{") or "}" not in stripped:
        return '{job="kubernetes-events"}'

    selector, rest = stripped[1:].split("}", 1)
    matchers: list[tuple[str, str, str]] = []
    for label, op, value in re.findall(r'([a-zA-Z_:][\w:]*)\s*(=~|=|!=|!~)\s*"([^"]*)"', selector):
        normalized = "namespace" if label in {"k8s_namespace_name", "namespace"} else label
        if normalized == "job":
            continue
        matchers.append((normalized, op, value))

    ordered = ['job="kubernetes-events"']
    ordered.extend(f'{label}{op}"{value}"' for label, op, value in matchers)
    return "{" + ",".join(ordered) + "}" + rest


def _parse_event_logfmt(line: str) -> dict:
    try:
        parts = shlex.split(line)
    except ValueError:
        parts = line.split()
    fields = {}
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        fields[key] = value
    return fields


def _k8s_event_streams(streams: list, limit: int | None) -> dict:
    base = _loki_streams(streams, limit)
    rows = []
    for row in base["rows"]:
        fields = _parse_event_logfmt(row.get("message") or "")
        involved_kind = fields.get("kind")
        involved_name = fields.get("name")
        event_message = fields.get("msg")
        row.update({
            "reason": fields.get("reason"),
            "type": fields.get("type"),
            "count": _num(fields.get("count")),
            "involved_kind": involved_kind,
            "involved_name": involved_name,
            "event_message": event_message,
            "pod": involved_name if involved_kind == "Pod" else row.get("pod"),
            "container": None,
            "real_kubernetes_event": True,
            "source_kind": "kubernetes_event",
            "evidence_source": "kubernetes_event",
        })
        rows.append(row)
    base["rows"] = rows
    if rows:
        first = rows[0]
        reason = first.get("reason") or "KubernetesEvent"
        name = first.get("involved_name") or first.get("namespace") or ""
        message = first.get("event_message") or first.get("message") or ""
        base["result_excerpt"] = f"{reason} {name}: {message}".strip()[:200]
    return base


def _k8s_events(query: str, start: datetime, end: datetime, limit: int | None) -> dict:
    body = _call(f"{_base('k8s_events')}/loki/api/v1/query_range", {
        "query": _event_logql(query),
        "start": int(start.timestamp() * 1_000_000_000),
        "end": int(end.timestamp() * 1_000_000_000),
        "limit": limit or 100,
        "direction": "backward",
    })
    data = body.get("data") or {}
    return _k8s_event_streams(data.get("result") or [], limit)


def _mark_kubernetes_log_signal(result: dict) -> dict:
    for row in result.get("rows") or []:
        row["evidence_source"] = "kubernetes_log_signal"
        row["real_kubernetes_event"] = False
        row["source_kind"] = "loki_log"
    return result


def _loki_matrix(query: str, series: list, limit: int | None) -> dict:
    excerpt = None
    if series:
        pts = series[0].get("values") or []
        last = pts[-1][1] if pts else "?"
        excerpt = f"{query[:120]} = {last}"
    rows = []
    for s in series[: _cap(limit, 50)]:
        m = s.get("metric", {})
        pts = s.get("values") or []
        nums = [n for n in (_num(v) for _, v in pts) if isinstance(n, (int, float))]
        row = {
            "metric_name": "log_match_count",
            "service": _service(m),
            "namespace": _namespace(m),
            "pod": _pod(m),
            "container": _container(m),
            "labels": m,
            "sample_count": len(pts),
            "series_start": _num(pts[0][0]) if pts else None,
            "series_end": _num(pts[-1][0]) if pts else None,
            "timestamp": _num(pts[-1][0]) if pts else None,
        }
        if nums:
            row["observed_value"] = nums[-1]
            row["last_value"] = nums[-1]
            row["max_value"] = max(nums)
            row["min_value"] = min(nums)
            row["avg_value"] = round(sum(nums) / len(nums), 6)
            row["latest"] = nums[-1]
            row["last"] = nums[-1]
            row["max"] = row["max_value"]
            row["min"] = row["min_value"]
            row["avg"] = row["avg_value"]
        else:
            row["observed_value"] = None
        rows.append(row)
    return {
        "row_count": len(series),
        "result_excerpt": excerpt,
        "rows": rows,
        "_series_count": len(series),
    }


def _loki_streams(streams: list, limit: int | None) -> dict:
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
                "service": _service(st),
                "namespace": _namespace(st),
                "pod": _pod(st),
                "container": _container(st),
                "level": st.get("level") or st.get("detected_level"),
                "labels": st,
                "timestamp": ts,
            })
        if len(rows) >= cap:
            break
    return {
        "row_count": row_count,
        "result_excerpt": excerpt,
        "rows": rows,
        "_series_count": len(streams),
    }


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
    rows = []
    for t in traces[: _cap(limit, 20)]:
        trace_id = t.get("traceID")
        detail_rows = _tempo_trace_rows(trace_id) if trace_id else []
        if detail_rows:
            rows.extend(detail_rows)
        else:
            rows.append({
                "span": t.get("rootTraceName"),
                "operation": t.get("rootTraceName"),
                "service": t.get("rootServiceName"),
                "namespace": t.get("rootServiceNamespace"),
                "trace_id": trace_id,
                "span_id": None,
                "parent_span_id": None,
                "duration_ms": t.get("durationMs"),
                "status": None,
                "critical_path_rank": None,
                "labels": t,
            })
    return {
        "row_count": len(rows),
        "result_excerpt": excerpt,
        "rows": rows,
        "_series_count": len(traces),
    }


def _tempo_trace_rows(trace_id: str) -> list[dict]:
    try:
        body = _call(f"{_base('tempo')}/api/traces/{trace_id}", {})
    except ProviderError:
        return []
    rows: list[dict] = []
    for resource_span in body.get("resourceSpans") or []:
        resource_attrs = _otel_attrs((resource_span.get("resource") or {}).get("attributes") or [])
        service = resource_attrs.get("service.name")
        namespace = resource_attrs.get("service.namespace") or resource_attrs.get("k8s.namespace.name")
        for scope_span in resource_span.get("scopeSpans") or []:
            for span in scope_span.get("spans") or []:
                attrs = {**resource_attrs, **_otel_attrs(span.get("attributes") or [])}
                duration_ms = _span_duration_ms(span)
                rows.append({
                    "trace_id": span.get("traceId") or trace_id,
                    "span_id": span.get("spanId"),
                    "parent_span_id": span.get("parentSpanId") or None,
                    "service": attrs.get("service.name") or service,
                    "namespace": attrs.get("service.namespace") or attrs.get("k8s.namespace.name") or namespace,
                    "operation": span.get("name"),
                    "span": span.get("name"),
                    "duration_ms": duration_ms,
                    "status": (span.get("status") or {}).get("code"),
                    "labels": attrs,
                })
    for rank, row in enumerate(
        sorted(rows, key=lambda item: item.get("duration_ms") or 0, reverse=True),
        start=1,
    ):
        row["critical_path_rank"] = rank
    return rows


def _otel_attrs(attrs: list[dict]) -> dict:
    values = {}
    for attr in attrs:
        key = attr.get("key")
        raw = attr.get("value") or {}
        if not key:
            continue
        values[key] = (
            raw.get("stringValue")
            if "stringValue" in raw
            else raw.get("intValue")
            if "intValue" in raw
            else raw.get("doubleValue")
            if "doubleValue" in raw
            else raw.get("boolValue")
        )
    return values


def _span_duration_ms(span: dict) -> float | None:
    try:
        start_ns = int(span.get("startTimeUnixNano") or 0)
        end_ns = int(span.get("endTimeUnixNano") or 0)
    except (TypeError, ValueError):
        return None
    if not start_ns or not end_ns or end_ns < start_ns:
        return None
    return round((end_ns - start_ns) / 1_000_000, 3)


def _kubernetes(query: str, limit: int | None) -> dict:
    namespace = _kubernetes_namespace(query)
    paths = [
        f"/api/v1/namespaces/{namespace}/events",
        f"/api/v1/namespaces/{namespace}/pods",
        f"/apis/apps/v1/namespaces/{namespace}/deployments",
        f"/apis/apps/v1/namespaces/{namespace}/replicasets",
        f"/apis/autoscaling/v2/namespaces/{namespace}/horizontalpodautoscalers",
    ]
    rows: list[dict] = []
    for path in paths:
        rows.extend(_kubernetes_rows(_kubernetes_get(path).get("items") or []))
    rows = rows[: _cap(limit, 200)]
    excerpt = None
    if rows:
        first = rows[0]
        excerpt = f"{first.get('kind')} {first.get('namespace')}/{first.get('name')}".strip()
    return {
        "row_count": len(rows),
        "result_excerpt": excerpt,
        "rows": rows,
        "_series_count": len(rows),
    }


def _kubernetes_namespace(query: str) -> str:
    try:
        parsed = json.loads(query)
    except ValueError:
        parsed = {}
    if isinstance(parsed, dict):
        for key in ("namespace", "k8s_namespace_name"):
            value = parsed.get(key)
            if value:
                return str(value)
    labels = normalize_query_labels(query)
    return labels.get("namespace") or os.environ.get("DEFAULT_KUBERNETES_NAMESPACE", "default")


def _kubernetes_rows(items: list[dict]) -> list[dict]:
    rows = []
    for item in items:
        meta = item.get("metadata") or {}
        status = item.get("status") or {}
        spec = item.get("spec") or {}
        kind = item.get("kind")
        row = {
            "kind": kind,
            "name": meta.get("name"),
            "namespace": meta.get("namespace"),
            "labels": meta.get("labels") or {},
            "source_kind": "kubernetes_api",
            "real_kubernetes_event": kind == "Event",
        }
        if kind == "Event":
            row.update({
                "reason": item.get("reason"),
                "message": item.get("message"),
                "involved_object": item.get("involvedObject"),
                "event_time": item.get("eventTime") or item.get("lastTimestamp") or item.get("firstTimestamp"),
            })
        elif kind == "Pod":
            restarts = sum((c.get("restartCount") or 0) for c in status.get("containerStatuses") or [])
            row.update({"phase": status.get("phase"), "restart_count": restarts})
        elif kind in ("Deployment", "ReplicaSet"):
            row.update({
                "replicas": spec.get("replicas"),
                "ready_replicas": status.get("readyReplicas"),
                "available_replicas": status.get("availableReplicas"),
                "conditions": status.get("conditions") or [],
            })
        elif kind == "HorizontalPodAutoscaler":
            row.update({
                "current_replicas": status.get("currentReplicas"),
                "desired_replicas": status.get("desiredReplicas"),
                "max_replicas": spec.get("maxReplicas"),
                "conditions": status.get("conditions") or [],
            })
        rows.append(row)
    return rows


def run_query(provider: str, query: str, start: datetime, end: datetime, limit: int | None) -> dict:
    """provider native query 실행 후 {row_count, result_excerpt, rows} 반환."""
    if provider in ("prometheus", "mimir"):
        return _prometheus(provider, query, start, end, limit)
    if provider == "loki":
        return _loki(provider, query, start, end, limit)
    if provider == "k8s_events":
        return _k8s_events(query, start, end, limit)
    if provider == "kubernetes_log_signal":
        result = _loki(provider, query, start, end, limit)
        return _mark_kubernetes_log_signal(result)
    if provider == "tempo":
        return _tempo(query, start, end, limit)
    if provider == "kubernetes":
        return _kubernetes(query, limit)
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
