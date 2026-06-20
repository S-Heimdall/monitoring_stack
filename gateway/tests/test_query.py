"""POST /internal/telemetry/query — §3.3.5.1 계약 검증. upstream은 monkeypatch로 차단."""
import httpx
from fastapi.testclient import TestClient

from app import providers
from app.main import app

client = TestClient(app)
WINDOW = {"from": "2026-06-10T00:00:00Z", "to": "2026-06-10T00:10:00Z"}


def _patch_upstream(monkeypatch, payload, status=200):
    def fake_get(url, params):
        return httpx.Response(status, json=payload)
    monkeypatch.setattr(providers, "_get", fake_get)


def _patch_upstream_by_url(monkeypatch, routes):
    def fake_get(url, params):
        for needle, payload in routes.items():
            if needle in url:
                return httpx.Response(200, json=payload)
        return httpx.Response(404, text=f"no route for {url}")

    monkeypatch.setattr(providers, "_get", fake_get)


def test_prometheus_query(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"__name__": "up", "job": "checkout", "service_name": "checkout",
                    "k8s_namespace_name": "otel-demo", "pod": "checkout-abc", "container": "checkout"},
         "values": [[1, "1"], [2, "1"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["provider"] == "prometheus"
    assert d["status"] == "pass"
    assert d["data_status"] == "ok"
    assert d["empty_reason"] is None
    assert d["row_count"] == 1
    assert "up" in d["result_excerpt"]
    # rows: provider-faithful 행 노출(소비자 Evidence MCP 필수).
    assert len(d["rows"]) == 1
    row = d["rows"][0]
    assert row["metric_name"] == "up"
    assert row["service"] == "checkout"
    assert row["namespace"] == "otel-demo"
    assert row["pod"] == "checkout-abc"
    assert row["container"] == "checkout"
    assert row["observed_value"] == 1.0
    # AIOps측 필드는 single_gateway에서 null
    assert d["telemetry_connection_id"] is None
    assert d["result_ref"] is None
    assert d["query_time_range"]["from"] == WINDOW["from"]
    assert d["query_time_range"]["to"] == WINDOW["to"]
    assert d["query_time_range"]["step_seconds"] == 15
    assert d["normalized_labels"] == {}
    assert d["started_at"] and d["completed_at"]


def test_prometheus_range_summary(monkeypatch):
    # range samples를 sample_count/max/min/avg로 요약해 "지속됐는가"를 판단 가능케 한다(HEIM-249).
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"__name__": "throttle", "service_name": "payment"},
         "values": [[1, "0.2"], [2, "0.9"], [3, "1.0"], [4, "0.8"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "throttle", "time_window": WINDOW})
    row = r.json()["data"]["rows"][0]
    assert row["sample_count"] == 4
    assert row["observed_value"] == 0.8  # backward-compat: last value
    assert row["last_value"] == 0.8
    assert row["max_value"] == 1.0
    assert row["min_value"] == 0.2
    assert abs(row["avg_value"] - 0.725) < 1e-6
    assert row["series_start"] == 1.0 and row["series_end"] == 4.0


def test_prometheus_empty_no_series_contract(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": []}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["provider"] == "prometheus"
    assert d["row_count"] == 0
    assert d["rows"] == []
    assert d["data_status"] == "empty"
    assert d["empty_reason"] == "no_series_matched"
    assert d["query_time_range"]["step_seconds"] == 15


def test_prometheus_empty_label_mismatch_contract(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": []}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics",
        "provider": "prometheus",
        "query": 'http_server_duration{app="checkout",namespace="otel-demo"}',
        "time_window": WINDOW,
    })
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["data_status"] == "empty"
    assert d["empty_reason"] == "label_mismatch_suspected"
    assert d["normalized_labels"] == {"service": "checkout", "namespace": "otel-demo"}


def test_prometheus_empty_no_rows_in_window_contract(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"__name__": "up", "service_name": "checkout"}, "values": []},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["row_count"] == 1
    assert len(d["rows"]) == 1
    assert d["rows"][0]["sample_count"] == 0
    assert d["data_status"] == "empty"
    assert d["empty_reason"] == "no_rows_in_window"


def test_loki_query(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"app": "checkout", "service_name": "checkout", "k8s_namespace_name": "otel-demo",
                    "k8s_pod_name": "checkout-abc", "k8s_container_name": "checkout", "level": "warning"},
         "values": [["1", "payment timeout"], ["2", "retry"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "logs", "provider": "loki", "query": '{app="checkout"}', "time_window": WINDOW, "limit": 50})
    d = r.json()["data"]
    assert d["row_count"] == 2
    assert d["result_excerpt"] == "payment timeout"
    # rows: 스트림 값 하나당 행 하나.
    assert len(d["rows"]) == 2
    assert d["rows"][0]["message"] == "payment timeout"
    assert d["rows"][0]["service"] == "checkout"
    assert d["rows"][0]["namespace"] == "otel-demo"
    assert d["rows"][0]["pod"] == "checkout-abc"
    assert d["rows"][0]["container"] == "checkout"
    assert d["rows"][0]["level"] == "warning"


def test_loki_rows_capped_by_limit(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"app": "checkout"}, "values": [["1", "a"], ["2", "b"], ["3", "c"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "logs", "provider": "loki", "query": '{app="checkout"}', "time_window": WINDOW, "limit": 2})
    d = r.json()["data"]
    assert d["row_count"] == 3  # 전체 집계는 그대로
    assert len(d["rows"]) == 2  # 노출 행은 limit 상한


def test_loki_count_over_time_summarized_to_one_row(monkeypatch):
    # HEIM-254: count_over_time 등 metric 쿼리는 matrix로 온다 — 버킷마다 행을 만들지 않고
    # series당 1행으로 요약하고, 카운트를 가짜 로그 message로 만들지 않는다.
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"service_name": "checkout", "k8s_namespace_name": "otel-demo"},
         "values": [["1", "10"], ["2", "148"], ["3", "148"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "logs", "provider": "loki",
        "query": 'sum(count_over_time({service_name="checkout"} |~ "error" [5m]))',
        "time_window": WINDOW})
    d = r.json()["data"]
    assert len(d["rows"]) == 1
    row = d["rows"][0]
    assert row["metric_name"] == "log_match_count"
    assert row["observed_value"] == 148.0
    assert row["max_value"] == 148.0
    assert row["sample_count"] == 3
    assert "message" not in row


def test_tempo_query(monkeypatch):
    _patch_upstream_by_url(monkeypatch, {
        "/api/search": {"traces": [
            {"traceID": "abc123", "rootServiceName": "checkout", "rootTraceName": "POST /checkout",
             "rootServiceNamespace": "otel-demo", "durationMs": 3200},
        ]},
        "/api/traces/abc123": {"resourceSpans": [
            {"resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "service.namespace", "value": {"stringValue": "otel-demo"}},
            ]}, "scopeSpans": [{"spans": [
                {
                    "traceId": "abc123",
                    "spanId": "span-root",
                    "name": "POST /checkout",
                    "startTimeUnixNano": "1000000000",
                    "endTimeUnixNano": "4200000000",
                    "status": {"code": "STATUS_CODE_ERROR"},
                },
                {
                    "traceId": "abc123",
                    "spanId": "span-child",
                    "parentSpanId": "span-root",
                    "name": "payment call",
                    "startTimeUnixNano": "1500000000",
                    "endTimeUnixNano": "4100000000",
                    "status": {"code": "STATUS_CODE_OK"},
                },
            ]}]}
        ]},
    })
    r = client.post("/internal/telemetry/query", json={
        "signal": "traces", "provider": "tempo", "query": "{}", "time_window": WINDOW})
    d = r.json()["data"]
    assert d["row_count"] == 2
    assert "checkout" in d["result_excerpt"]
    assert len(d["rows"]) == 2
    assert d["rows"][0]["service"] == "checkout"
    assert d["rows"][0]["namespace"] == "otel-demo"
    assert d["rows"][0]["operation"] == "POST /checkout"
    assert d["rows"][0]["trace_id"] == "abc123"
    assert d["rows"][0]["span_id"] == "span-root"
    assert d["rows"][0]["parent_span_id"] is None
    assert d["rows"][0]["duration_ms"] == 3200.0
    assert d["rows"][0]["status"] == "STATUS_CODE_ERROR"
    assert d["rows"][0]["critical_path_rank"] == 1


def test_kubernetes_events_routes_to_loki(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"reason": "ScalingReplicaSet", "namespace": "otel-demo", "pod": "payment-abc",
                    "container": "payment"}, "values": [["1", "Scaled up"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "kubernetes_events", "provider": "k8s_events",
        "query": '{job="kubernetes-events"}', "time_window": WINDOW})
    assert r.status_code == 200
    row = r.json()["data"]["rows"][0]
    assert row["namespace"] == "otel-demo"
    assert row["pod"] == "payment-abc"
    assert row["container"] == "payment"
    assert row["evidence_source"] == "kubernetes_log_signal"
    assert row["real_kubernetes_event"] is False
    assert row["source_kind"] == "loki_log"


def test_kubernetes_provider_returns_structured_cluster_state(monkeypatch):
    def fake_kubernetes_get(path):
        if path == "/api/v1/namespaces/otel-demo/events":
            return {"items": [
                {
                    "kind": "Event",
                    "metadata": {"name": "payment.123", "namespace": "otel-demo"},
                    "reason": "FailedScheduling",
                    "message": "0/3 nodes available",
                    "lastTimestamp": "2026-06-10T00:02:00Z",
                    "involvedObject": {"kind": "Pod", "name": "payment-abc"},
                }
            ]}
        if path == "/api/v1/namespaces/otel-demo/pods":
            return {"items": [
                {
                    "kind": "Pod",
                    "metadata": {"name": "payment-abc", "namespace": "otel-demo", "labels": {"app": "payment"}},
                    "status": {"phase": "Running", "containerStatuses": [{"restartCount": 2}]},
                }
            ]}
        if path == "/apis/apps/v1/namespaces/otel-demo/deployments":
            return {"items": [
                {
                    "kind": "Deployment",
                    "metadata": {"name": "payment", "namespace": "otel-demo"},
                    "spec": {"replicas": 3},
                    "status": {"readyReplicas": 2, "conditions": [{"type": "Available", "status": "False"}]},
                }
            ]}
        if path == "/apis/apps/v1/namespaces/otel-demo/replicasets":
            return {"items": []}
        if path == "/apis/autoscaling/v2/namespaces/otel-demo/horizontalpodautoscalers":
            return {"items": [
                {
                    "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": "payment", "namespace": "otel-demo"},
                    "spec": {"maxReplicas": 10},
                    "status": {"currentReplicas": 3, "desiredReplicas": 6, "conditions": [{"type": "ScalingLimited", "status": "True"}]},
                }
            ]}
        raise AssertionError(path)

    monkeypatch.setattr(providers, "_kubernetes_get", fake_kubernetes_get)
    r = client.post("/internal/telemetry/query", json={
        "signal": "kubernetes_events",
        "provider": "kubernetes",
        "query": '{namespace="otel-demo"}',
        "time_window": WINDOW,
    })
    assert r.status_code == 200
    rows = r.json()["data"]["rows"]
    assert {row["kind"] for row in rows} >= {"Event", "Pod", "Deployment", "HorizontalPodAutoscaler"}
    hpa = next(row for row in rows if row["kind"] == "HorizontalPodAutoscaler")
    assert hpa["current_replicas"] == 3
    assert hpa["desired_replicas"] == 6
    assert hpa["max_replicas"] == 10
    event = next(row for row in rows if row["kind"] == "Event")
    assert event["reason"] == "FailedScheduling"
    assert event["involved_object"] == {"kind": "Pod", "name": "payment-abc"}


def test_kubernetes_events_empty_no_rows_in_window_contract(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"reason": "ScalingReplicaSet", "namespace": "otel-demo"}, "values": []},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "kubernetes_events", "provider": "k8s_events",
        "query": '{job="kubernetes-events"}', "time_window": WINDOW})
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["provider"] == "k8s_events"
    assert d["row_count"] == 0
    assert d["rows"] == []
    assert d["data_status"] == "empty"
    assert d["empty_reason"] == "no_rows_in_window"


def test_unknown_provider_404(monkeypatch):
    r = client.post("/internal/telemetry/query", json={
        "signal": "traces", "provider": "jaeger", "query": "{}", "time_window": WINDOW})
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "TELEMETRY_CONNECTION_NOT_FOUND"


def test_empty_query_400():
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "  ", "time_window": WINDOW})
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "TELEMETRY_QUERY_INVALID"


def test_bad_time_window_400():
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "up",
        "time_window": {"from": "2026-06-10T01:00:00Z", "to": "2026-06-10T00:00:00Z"}})
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "TELEMETRY_QUERY_INVALID"


def test_upstream_failure_502(monkeypatch):
    monkeypatch.setattr(providers, "_get", lambda url, params: httpx.Response(500, text="boom"))
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 502
    assert r.json()["detail"]["code"] == "TELEMETRY_PROVIDER_QUERY_FAILED"
    assert r.json()["detail"]["data_status"] == "error"


def test_upstream_timeout_504(monkeypatch):
    def timeout(url, params):
        raise httpx.ReadTimeout("timed out")
    monkeypatch.setattr(providers, "_get", timeout)
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 504
    assert r.json()["detail"]["data_status"] == "error"
    assert r.json()["detail"]["empty_reason"] == "provider_timeout"


def test_auth_required_when_token_set(monkeypatch):
    monkeypatch.setenv("HEIMDALL_READ_TOKEN", "s3cret")
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 403
    assert r.json()["detail"]["code"] == "INTERNAL_CALL_FORBIDDEN"


def test_auth_pass_with_token(monkeypatch):
    monkeypatch.setenv("HEIMDALL_READ_TOKEN", "s3cret")
    _patch_upstream(monkeypatch, {"status": "success", "data": {"result": []}})
    r = client.post("/internal/telemetry/query",
                    headers={"Authorization": "Bearer s3cret"},
                    json={"signal": "metrics", "provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 200
    assert r.json()["data"]["row_count"] == 0


# HEIM-235: 모든 에러가 안정 스키마 detail{code,message,data_status,empty_reason,provider}로 통일된다.
def test_invalid_signal_uses_stable_schema():
    r = client.post("/internal/telemetry/query", json={
        "signal": "bogus", "provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert detail["code"] == "TELEMETRY_QUERY_INVALID"
    assert detail["data_status"] == "error"
    assert detail["empty_reason"] == "unsupported_query"


def test_request_validation_normalized_to_stable_schema():
    # 필수 필드 누락 → FastAPI 기본 422 리스트가 아니라 안정 스키마 400.
    r = client.post("/internal/telemetry/query", json={"provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert detail["code"] == "TELEMETRY_QUERY_INVALID"
    assert detail["data_status"] == "error"
    assert isinstance(detail["message"], str)


# ── Set 4: post-action verification PromQL window contract ─────────────────────────────
# before-window(장애 구간)과 after-window(조치 후 구간) query가 rows와 data_status를
# 안정적으로 반환하는지 검증한다. verification_checker의 before/after 비교 판정이 의존한다.

BEFORE_WINDOW = {"from": "2026-06-10T00:00:00Z", "to": "2026-06-10T00:05:00Z"}
AFTER_WINDOW  = {"from": "2026-06-10T00:10:00Z", "to": "2026-06-10T00:15:00Z"}


def test_verification_before_window_returns_rows_and_observed_value(monkeypatch):
    # 장애 구간(before) — 에러율 높음
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"__name__": "http_errors_total", "service_name": "checkout"},
         "values": [[1, "0.80"], [2, "0.85"], [3, "0.90"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus",
        "query": 'rate(http_errors_total{service_name="checkout"}[5m])',
        "time_window": BEFORE_WINDOW,
    })
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["data_status"] == "ok"
    assert d["empty_reason"] is None
    assert d["row_count"] == 1
    row = d["rows"][0]
    assert row["observed_value"] == 0.9   # last value (backward-compat)
    assert row["max_value"] == 0.9
    assert row["min_value"] == 0.8
    assert row["sample_count"] == 3


def test_verification_after_window_returns_rows_and_observed_value(monkeypatch):
    # 조치 후 구간(after) — 에러율 회복
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"__name__": "http_errors_total", "service_name": "checkout"},
         "values": [[1, "0.10"], [2, "0.08"], [3, "0.05"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus",
        "query": 'rate(http_errors_total{service_name="checkout"}[5m])',
        "time_window": AFTER_WINDOW,
    })
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["data_status"] == "ok"
    assert d["empty_reason"] is None
    row = d["rows"][0]
    assert row["observed_value"] == 0.05
    assert row["max_value"] == 0.10
    assert row["sample_count"] == 3


def test_verification_threshold_query_passes_stable_rows(monkeypatch):
    # threshold 기반 판정: observed_value <= threshold 이면 PASS
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"__name__": "http_request_duration_p99", "service_name": "payment"},
         "values": [[1, "320"], [2, "310"], [3, "300"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus",
        "query": 'histogram_quantile(0.99, rate(http_request_duration_bucket{service_name="payment"}[5m]))',
        "time_window": AFTER_WINDOW,
    })
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["data_status"] == "ok"
    row = d["rows"][0]
    # verification_checker가 observed_value를 threshold(예: 500ms)와 비교 가능한 형태여야 한다.
    assert isinstance(row["observed_value"], float)
    assert row["observed_value"] == 300.0
    assert row["max_value"] == 320.0


def test_verification_after_window_no_data_returns_stable_empty_contract(monkeypatch):
    # 조치 후 metric이 아직 수집되지 않은 경우 — INSUFFICIENT_VERIFICATION_DATA 경로
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": []}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus",
        "query": 'rate(http_errors_total{service_name="checkout"}[5m])',
        "time_window": AFTER_WINDOW,
    })
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["data_status"] == "empty"
    assert d["empty_reason"] == "no_series_matched"
    assert d["row_count"] == 0
    assert d["rows"] == []


def test_verification_before_after_label_mismatch_signals_empty(monkeypatch):
    # label 불일치 — before/after 모두 빈 결과 + 원인 명시
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": []}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus",
        "query": 'rate(http_errors_total{service="checkout",namespace="otel-demo"}[5m])',
        "time_window": BEFORE_WINDOW,
    })
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["data_status"] == "empty"
    assert d["empty_reason"] == "label_mismatch_suspected"


def test_verification_kubernetes_event_after_rollout_returns_rows(monkeypatch):
    # Kubernetes rollout 조치 후 이벤트 확인 — after-window에서 ScalingReplicaSet 이벤트 수집
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"reason": "ScalingReplicaSet", "namespace": "otel-demo",
                    "pod": "checkout-deployment-xyz", "container": "checkout"},
         "values": [["1718000100000000000", "Scaled up replica set checkout-deployment-xyz to 3"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "kubernetes_events", "provider": "k8s_events",
        "query": '{reason="ScalingReplicaSet",namespace="otel-demo"}',
        "time_window": AFTER_WINDOW,
    })
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["data_status"] == "ok"
    assert d["row_count"] == 1
    row = d["rows"][0]
    assert row["namespace"] == "otel-demo"
    assert "ScalingReplicaSet" in row["message"] or row["pod"] == "checkout-deployment-xyz"
