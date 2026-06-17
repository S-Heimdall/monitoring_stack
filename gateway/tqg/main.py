"""Telemetry Query Gateway router for the Heimdall AIOps cluster.

This service runs in the Heimdall namespace. It validates the high-level
telemetry query request, selects the service-cluster single_gateway from
TELEMETRY_CONNECTIONS, injects the configured read token, and forwards the
request to that single_gateway.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

logger = logging.getLogger("heimdall.telemetry_query_gateway")


@dataclass(frozen=True)
class TelemetryConnection:
    connection_id: str | None
    cluster_id: str | None
    base_url: str
    token_env: str | None


class ConnectionConfigError(RuntimeError):
    pass


class ConnectionNotFoundError(RuntimeError):
    pass


app = FastAPI(title="Heimdall telemetry-query-gateway", version="0.1.0")


def _error_detail(
    code: str,
    message: str,
    *,
    provider: str | None = None,
    connection_id: str | None = None,
    cluster_id: str | None = None,
    empty_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "message": message,
        "data_status": "error",
        "empty_reason": empty_reason,
        "provider": provider,
        "telemetry_connection_id": connection_id,
        "cluster_id": cluster_id,
    }


def _load_connections() -> list[TelemetryConnection]:
    raw = os.environ.get("TELEMETRY_CONNECTIONS", "").strip()
    if not raw:
        raise ConnectionConfigError("TELEMETRY_CONNECTIONS is empty")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConnectionConfigError(f"TELEMETRY_CONNECTIONS is not valid JSON: {exc}") from exc
    if not isinstance(payload, list):
        raise ConnectionConfigError("TELEMETRY_CONNECTIONS must be a JSON array")

    connections: list[TelemetryConnection] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ConnectionConfigError(f"connection[{index}] must be an object")
        base_url = str(item.get("base_url") or item.get("query_base_url") or "").strip()
        if not base_url:
            raise ConnectionConfigError(f"connection[{index}] is missing base_url")
        connections.append(
            TelemetryConnection(
                connection_id=item.get("connection_id") or item.get("telemetry_connection_id"),
                cluster_id=item.get("cluster_id"),
                base_url=base_url.rstrip("/"),
                token_env=item.get("token_env"),
            )
        )
    if not connections:
        raise ConnectionConfigError("TELEMETRY_CONNECTIONS has no entries")
    return connections


def _select_connection(body: dict[str, Any]) -> TelemetryConnection:
    connections = _load_connections()
    cluster_id = body.get("cluster_id")
    connection_id = body.get("telemetry_connection_id") or body.get("connection_id")

    if connection_id:
        for connection in connections:
            if connection.connection_id == connection_id:
                return connection
        raise ConnectionNotFoundError(f"no telemetry connection for connection_id={connection_id}")

    if cluster_id:
        for connection in connections:
            if connection.cluster_id == cluster_id:
                return connection
        raise ConnectionNotFoundError(f"no telemetry connection for cluster_id={cluster_id}")

    if len(connections) == 1:
        return connections[0]
    raise ConnectionNotFoundError("cluster_id or telemetry_connection_id is required")


def _upstream_headers(connection: TelemetryConnection) -> dict[str, str]:
    headers: dict[str, str] = {}
    if connection.token_env:
        token = os.environ.get(connection.token_env, "")
        if token:
            headers["Authorization"] = f"Bearer {token}"
    return headers


def _timeout_seconds() -> float:
    return float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "20"))


@app.get("/healthz", include_in_schema=False)
def healthz() -> PlainTextResponse:
    return PlainTextResponse("ok\n")


@app.get("/", include_in_schema=False)
def root() -> dict[str, Any]:
    return {"data": {"service": "telemetry-query-gateway", "status": "pass"}}


@app.post("/internal/telemetry/query", summary="Route telemetry query to service-cluster single_gateway")
async def telemetry_query(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail("TELEMETRY_QUERY_INVALID", f"invalid JSON body: {exc}"),
        ) from exc

    if not isinstance(body, dict):
        raise HTTPException(
            status_code=400,
            detail=_error_detail("TELEMETRY_QUERY_INVALID", "request body must be an object"),
        )

    provider = body.get("provider")
    cluster_id = body.get("cluster_id")
    connection: TelemetryConnection | None = None
    try:
        connection = _select_connection(body)
    except ConnectionConfigError as exc:
        logger.error(
            "telemetry_connection_config_invalid provider=%s cluster_id=%s error=%s",
            provider,
            cluster_id,
            exc,
        )
        raise HTTPException(
            status_code=500,
            detail=_error_detail(
                "TELEMETRY_GATEWAY_CONFIG_INVALID",
                str(exc),
                provider=provider,
                cluster_id=cluster_id,
            ),
        ) from exc
    except ConnectionNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail=_error_detail(
                "TELEMETRY_CONNECTION_NOT_FOUND",
                str(exc),
                provider=provider,
                cluster_id=cluster_id,
            ),
        ) from exc

    upstream_url = urljoin(f"{connection.base_url}/", "internal/telemetry/query")
    timeout = _timeout_seconds()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                upstream_url,
                json=body,
                headers=_upstream_headers(connection),
            )
    except httpx.TimeoutException as exc:
        logger.warning(
            "telemetry_upstream_timeout provider=%s cluster_id=%s connection_id=%s base_url=%s error=%s",
            provider,
            connection.cluster_id,
            connection.connection_id,
            connection.base_url,
            exc,
        )
        raise HTTPException(
            status_code=504,
            detail=_error_detail(
                "TELEMETRY_PROVIDER_QUERY_FAILED",
                str(exc)[:200],
                provider=provider,
                connection_id=connection.connection_id,
                cluster_id=connection.cluster_id,
                empty_reason="provider_timeout",
            ),
        ) from exc
    except httpx.HTTPError as exc:
        logger.warning(
            "telemetry_upstream_http_error provider=%s cluster_id=%s connection_id=%s base_url=%s error=%s",
            provider,
            connection.cluster_id,
            connection.connection_id,
            connection.base_url,
            exc,
        )
        raise HTTPException(
            status_code=502,
            detail=_error_detail(
                "TELEMETRY_PROVIDER_QUERY_FAILED",
                str(exc)[:200],
                provider=provider,
                connection_id=connection.connection_id,
                cluster_id=connection.cluster_id,
                empty_reason="provider_unreachable",
            ),
        ) from exc

    try:
        payload = response.json()
    except ValueError as exc:
        logger.warning(
            "telemetry_upstream_non_json provider=%s cluster_id=%s connection_id=%s status=%s body=%s",
            provider,
            connection.cluster_id,
            connection.connection_id,
            response.status_code,
            response.text[:200],
        )
        raise HTTPException(
            status_code=502,
            detail=_error_detail(
                "TELEMETRY_PROVIDER_QUERY_FAILED",
                f"non-json upstream response: {exc}",
                provider=provider,
                connection_id=connection.connection_id,
                cluster_id=connection.cluster_id,
                empty_reason="provider_non_json",
            ),
        ) from exc

    if response.status_code >= 400:
        logger.warning(
            "telemetry_upstream_failed provider=%s cluster_id=%s connection_id=%s status=%s detail=%s",
            provider,
            connection.cluster_id,
            connection.connection_id,
            response.status_code,
            payload.get("detail") if isinstance(payload, dict) else None,
        )
        return JSONResponse(status_code=response.status_code, content=payload)

    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload["data"].setdefault("telemetry_connection_id", connection.connection_id)

    return JSONResponse(status_code=response.status_code, content=payload)
