"""요청/응답 스키마 — Heimdall_API_기능_명세 §3.3.5.1 내부 Telemetry Query API.

single_gateway는 AIOps telemetry-query-gateway가 호출하는 같은 본문/응답 형태를 받는다.
query_intent_id/project_id/cluster_id 및 응답의 telemetry_connection_id/result_ref 는
AIOps측이 채우는 필드라 single_gateway에서는 선택/패스스루이며 응답에서 null로 둔다.
"""
from __future__ import annotations

from datetime import datetime

from typing import Any

from pydantic import BaseModel, Field


class TimeWindow(BaseModel):
    from_: datetime = Field(alias="from")
    to: datetime

    model_config = {"populate_by_name": True}


class TelemetryQueryRequest(BaseModel):
    signal: str  # metrics | logs | traces | kubernetes_events
    provider: str  # prometheus | mimir | loki | tempo | jaeger | k8s_events
    query: str
    time_window: TimeWindow
    limit: int | None = None
    budget_class: str = "normal"
    # AIOps측 telemetry-query-gateway 필드 — single_gateway에선 선택/패스스루
    query_intent_id: str | None = None
    project_id: str | None = None
    cluster_id: str | None = None


class TelemetryQueryResponse(BaseModel):
    telemetry_connection_id: str | None = None
    provider: str
    status: str  # CheckStatus: pass | warn | fail | pending
    result_ref: str | None = None
    result_excerpt: str | None = None
    row_count: int | None = None
    data_status: str | None = None  # ok | empty | error
    empty_reason: str | None = None
    query_time_range: dict[str, Any] | None = None
    normalized_labels: dict[str, str] = Field(default_factory=dict)
    # provider-faithful raw 행. 소비자 Evidence MCP가 필수로 요구한다.
    rows: list[dict[str, Any]] = Field(default_factory=list)
    started_at: datetime
    completed_at: datetime
