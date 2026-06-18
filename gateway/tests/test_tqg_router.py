import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock

import httpx
from fastapi.testclient import TestClient

from tqg import main
from tqg.main import app

client = TestClient(app)
WINDOW = {"from": "2026-06-10T00:00:00Z", "to": "2026-06-10T00:10:00Z"}


class FakeAsyncClient:
    requests = []
    response = httpx.Response(200, json={"data": {"provider": "prometheus", "row_count": 1, "rows": []}})

    def __init__(self, timeout):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def post(self, url, json, headers):
        self.requests.append({"url": url, "json": json, "headers": headers, "timeout": self.timeout})
        return self.response


class ConcurrentFakeAsyncClient:
    requests = []
    lock = Lock()
    barrier = Barrier(2)

    def __init__(self, timeout):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def post(self, url, json, headers):
        self.barrier.wait(timeout=2)
        with self.lock:
            self.requests.append({"url": url, "json": json, "headers": headers, "timeout": self.timeout})
        return httpx.Response(
            200,
            json={
                "data": {
                    "provider": json["provider"],
                    "status": "pass",
                    "row_count": 0,
                    "rows": [],
                    "data_status": "empty",
                    "empty_reason": "no_series_matched",
                    "query": json["query"],
                }
            },
        )


class TimeoutAsyncClient(FakeAsyncClient):
    async def post(self, url, json, headers):
        raise httpx.ReadTimeout("single gateway timed out")


def _set_connections(monkeypatch):
    monkeypatch.setenv(
        "TELEMETRY_CONNECTIONS",
        json.dumps([
            {
                "cluster_id": "clu_otel001",
                "connection_id": "tel_otel_demo",
                "base_url": "http://single-gateway.local",
                "token_env": "OTEL_DEMO_READ_TOKEN",
            }
        ]),
    )
    monkeypatch.setenv("OTEL_DEMO_READ_TOKEN", "secret-token")


def _set_two_connections(monkeypatch):
    monkeypatch.setenv(
        "TELEMETRY_CONNECTIONS",
        json.dumps([
            {
                "cluster_id": "clu_otel001",
                "connection_id": "tel_otel_demo",
                "base_url": "http://single-gateway-a.local",
                "token_env": "OTEL_DEMO_READ_TOKEN",
            },
            {
                "cluster_id": "clu_prod002",
                "connection_id": "tel_prod",
                "base_url": "http://single-gateway-b.local",
                "token_env": "PROD_READ_TOKEN",
            },
        ]),
    )
    monkeypatch.setenv("OTEL_DEMO_READ_TOKEN", "demo-token")
    monkeypatch.setenv("PROD_READ_TOKEN", "prod-token")


def test_root_identifies_tqg_not_single_gateway():
    assert client.get("/").json()["data"] == {
        "service": "telemetry-query-gateway",
        "status": "pass",
    }


def test_routes_query_to_single_gateway_with_configured_token(monkeypatch):
    _set_connections(monkeypatch)
    FakeAsyncClient.requests = []
    FakeAsyncClient.response = httpx.Response(
        200,
        json={"data": {"provider": "prometheus", "row_count": 1, "rows": [{"metric_name": "up"}]}},
    )
    monkeypatch.setattr(main.httpx, "AsyncClient", FakeAsyncClient)

    body = {
        "cluster_id": "clu_otel001",
        "signal": "metrics",
        "provider": "prometheus",
        "query": "up",
        "time_window": WINDOW,
    }
    response = client.post("/internal/telemetry/query", json=body)

    assert response.status_code == 200
    assert response.json()["data"]["telemetry_connection_id"] == "tel_otel_demo"
    assert FakeAsyncClient.requests == [
        {
            "url": "http://single-gateway.local/internal/telemetry/query",
            "json": body,
            "headers": {"Authorization": "Bearer secret-token"},
            "timeout": 20.0,
        }
    ]


def test_service_graph_dependency_queries_preserve_empty_contract(monkeypatch):
    _set_connections(monkeypatch)
    FakeAsyncClient.requests = []
    FakeAsyncClient.response = httpx.Response(
        200,
        json={
            "data": {
                "provider": "prometheus",
                "status": "pass",
                "row_count": 0,
                "rows": [],
                "data_status": "empty",
                "empty_reason": "no_series_matched",
            }
        },
    )
    monkeypatch.setattr(main.httpx, "AsyncClient", FakeAsyncClient)

    queries = {
        "dep_inbound": 'sum(rate(traces_service_graph_request_total{server="checkout"}[5m])) by (client, server)',
        "dep_outbound": 'sum(rate(traces_service_graph_request_total{client="checkout"}[5m])) by (client, server)',
    }

    for query_name, query in queries.items():
        body = {
            "cluster_id": "clu_otel001",
            "signal": "metrics",
            "provider": "prometheus",
            "query": query,
            "time_window": WINDOW,
        }
        response = client.post("/internal/telemetry/query", json=body)

        assert response.status_code == 200, query_name
        data = response.json()["data"]
        assert data["telemetry_connection_id"] == "tel_otel_demo"
        assert data["row_count"] == 0
        assert data["rows"] == []
        assert data["data_status"] == "empty"
        assert data["empty_reason"] == "no_series_matched"

    assert [request["json"]["query"] for request in FakeAsyncClient.requests] == list(queries.values())


def test_concurrent_queries_keep_connection_token_and_empty_contract(monkeypatch):
    _set_two_connections(monkeypatch)
    ConcurrentFakeAsyncClient.requests = []
    ConcurrentFakeAsyncClient.barrier = Barrier(2)
    monkeypatch.setattr(main.httpx, "AsyncClient", ConcurrentFakeAsyncClient)

    bodies = [
        {
            "cluster_id": "clu_otel001",
            "signal": "metrics",
            "provider": "prometheus",
            "query": "up{service_name=\"checkout\"}",
            "time_window": WINDOW,
        },
        {
            "cluster_id": "clu_prod002",
            "signal": "logs",
            "provider": "loki",
            "query": "{service_name=\"payment\"}",
            "time_window": WINDOW,
        },
    ]

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda body: client.post("/internal/telemetry/query", json=body), bodies))

    assert [response.status_code for response in responses] == [200, 200]
    by_cluster = {response.json()["data"]["query"]: response.json()["data"] for response in responses}
    assert by_cluster['up{service_name="checkout"}']["telemetry_connection_id"] == "tel_otel_demo"
    assert by_cluster['{service_name="payment"}']["telemetry_connection_id"] == "tel_prod"
    assert all(data["row_count"] == 0 for data in by_cluster.values())
    assert all(data["rows"] == [] for data in by_cluster.values())
    assert all(data["data_status"] == "empty" for data in by_cluster.values())
    assert all(data["empty_reason"] == "no_series_matched" for data in by_cluster.values())

    requests_by_cluster = {request["json"]["cluster_id"]: request for request in ConcurrentFakeAsyncClient.requests}
    assert requests_by_cluster["clu_otel001"]["url"] == "http://single-gateway-a.local/internal/telemetry/query"
    assert requests_by_cluster["clu_otel001"]["headers"] == {"Authorization": "Bearer demo-token"}
    assert requests_by_cluster["clu_prod002"]["url"] == "http://single-gateway-b.local/internal/telemetry/query"
    assert requests_by_cluster["clu_prod002"]["headers"] == {"Authorization": "Bearer prod-token"}


def test_missing_connection_returns_404(monkeypatch):
    _set_connections(monkeypatch)

    response = client.post(
        "/internal/telemetry/query",
        json={
            "cluster_id": "missing",
            "signal": "metrics",
            "provider": "prometheus",
            "query": "up",
            "time_window": WINDOW,
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "TELEMETRY_CONNECTION_NOT_FOUND"


def test_upstream_error_is_preserved(monkeypatch):
    _set_connections(monkeypatch)
    FakeAsyncClient.requests = []
    FakeAsyncClient.response = httpx.Response(
        502,
        json={"detail": {"code": "TELEMETRY_PROVIDER_QUERY_FAILED", "message": "boom"}},
    )
    monkeypatch.setattr(main.httpx, "AsyncClient", FakeAsyncClient)

    response = client.post(
        "/internal/telemetry/query",
        json={
            "cluster_id": "clu_otel001",
            "signal": "metrics",
            "provider": "prometheus",
            "query": "up",
            "time_window": WINDOW,
        },
    )

    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "TELEMETRY_PROVIDER_QUERY_FAILED"


def test_upstream_provider_error_reason_is_preserved(monkeypatch):
    _set_connections(monkeypatch)
    FakeAsyncClient.requests = []
    provider_detail = {
        "code": "TELEMETRY_PROVIDER_QUERY_FAILED",
        "message": "bad_data: parse error",
        "data_status": "error",
        "empty_reason": "unsupported_query",
        "provider": "prometheus",
    }
    FakeAsyncClient.response = httpx.Response(502, json={"detail": provider_detail})
    monkeypatch.setattr(main.httpx, "AsyncClient", FakeAsyncClient)

    response = client.post(
        "/internal/telemetry/query",
        json={
            "cluster_id": "clu_otel001",
            "signal": "metrics",
            "provider": "prometheus",
            "query": "bad query",
            "time_window": WINDOW,
        },
    )

    assert response.status_code == 502
    assert response.json()["detail"] == provider_detail


def test_upstream_timeout_reason_is_provider_timeout(monkeypatch):
    _set_connections(monkeypatch)
    monkeypatch.setattr(main.httpx, "AsyncClient", TimeoutAsyncClient)

    response = client.post(
        "/internal/telemetry/query",
        json={
            "cluster_id": "clu_otel001",
            "signal": "metrics",
            "provider": "prometheus",
            "query": "up",
            "time_window": WINDOW,
        },
    )

    assert response.status_code == 504
    detail = response.json()["detail"]
    assert detail["code"] == "TELEMETRY_PROVIDER_QUERY_FAILED"
    assert detail["data_status"] == "error"
    assert detail["empty_reason"] == "provider_timeout"
    assert detail["provider"] == "prometheus"
    assert detail["telemetry_connection_id"] == "tel_otel_demo"
    assert detail["cluster_id"] == "clu_otel001"
