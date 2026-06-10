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


def test_prometheus_query(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"__name__": "up", "job": "checkout"}, "values": [[1, "1"], [2, "1"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "metrics", "provider": "prometheus", "query": "up", "time_window": WINDOW})
    assert r.status_code == 200
    d = r.json()["data"]
    assert d["provider"] == "prometheus"
    assert d["status"] == "pass"
    assert d["row_count"] == 1
    assert "up" in d["result_excerpt"]
    # AIOps측 필드는 single_gateway에서 null
    assert d["telemetry_connection_id"] is None
    assert d["result_ref"] is None
    assert d["started_at"] and d["completed_at"]


def test_loki_query(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"app": "checkout"}, "values": [["1", "payment timeout"], ["2", "retry"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "logs", "provider": "loki", "query": '{app="checkout"}', "time_window": WINDOW, "limit": 50})
    d = r.json()["data"]
    assert d["row_count"] == 2
    assert d["result_excerpt"] == "payment timeout"


def test_tempo_query(monkeypatch):
    _patch_upstream(monkeypatch, {"traces": [{"traceID": "abc123", "rootServiceName": "checkout"}]})
    r = client.post("/internal/telemetry/query", json={
        "signal": "traces", "provider": "tempo", "query": "{}", "time_window": WINDOW})
    d = r.json()["data"]
    assert d["row_count"] == 1
    assert "checkout" in d["result_excerpt"]


def test_kubernetes_events_routes_to_loki(monkeypatch):
    _patch_upstream(monkeypatch, {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"reason": "ScalingReplicaSet"}, "values": [["1", "Scaled up"]]},
    ]}})
    r = client.post("/internal/telemetry/query", json={
        "signal": "kubernetes_events", "provider": "k8s_events",
        "query": '{job="kubernetes-events"}', "time_window": WINDOW})
    assert r.status_code == 200
    assert r.json()["data"]["row_count"] == 1


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
