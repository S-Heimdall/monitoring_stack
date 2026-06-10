"""verify(§3.3.5) probe 표면 — 무인증으로 single_gateway 및 provider route 도달성 확인."""
import httpx
from fastapi.testclient import TestClient

from app import providers
from app.main import app

client = TestClient(app)


def test_root_probe_handshake():
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["data"]["status"] == "pass"


def test_healthz():
    assert client.get("/healthz").status_code == 200


def test_verify_probe_routes_reach_upstream(monkeypatch):
    monkeypatch.setattr(providers, "_get", lambda url, params: httpx.Response(200, text="healthy"))
    for path in ["/-/healthy", "/loki/api/v1/labels", "/api/echo", "/api/v1/events"]:
        assert client.get(path).status_code == 200, path


def test_probe_routes_are_unauthenticated(monkeypatch):
    # 토큰이 설정돼 있어도 verify는 토큰을 안 보내므로 probe는 무인증이어야 한다.
    monkeypatch.setenv("HEIMDALL_READ_TOKEN", "s3cret")
    monkeypatch.setattr(providers, "_get", lambda url, params: httpx.Response(200, text="ok"))
    assert client.get("/-/healthy").status_code == 200
    assert client.get("/").status_code == 200


def test_probe_upstream_down_502(monkeypatch):
    def boom(url, params):
        raise httpx.ConnectError("refused")
    monkeypatch.setattr(providers, "_get", boom)
    assert client.get("/api/echo").status_code == 502
