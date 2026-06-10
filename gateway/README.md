# single_gateway

서비스 클러스터(데모) 내부에 설치되는 telemetry 조회 진입점. AIOps `telemetry-query-gateway`가
PrivateLink로 호출하면, provider로 내부 LGTM(Prometheus/Loki/Tempo)에 read-only query를 라우팅한다.
Heimdall_API_기능_명세 §3.3.5.1(`POST /internal/telemetry/query`)을 구현하며, 기존 3-prefix
(`/prometheus`·`/loki`·`/tempo`) nginx read-gateway를 대체한다.

## 엔드포인트

### POST /internal/telemetry/query  (Bearer 인증)
명세 §3.3.5.1. provider는 body 필드다.

요청:
```json
{
  "signal": "logs",
  "provider": "loki",
  "query": "{app=\"checkout\"} |= \"timeout\"",
  "time_window": {"from": "2026-06-10T00:00:00Z", "to": "2026-06-10T00:10:00Z"},
  "limit": 100,
  "budget_class": "normal"
}
```
- `signal`: metrics | logs | traces | kubernetes_events
- `provider`: prometheus | mimir | loki | tempo | k8s_events
- `query_intent_id` / `project_id` / `cluster_id`는 AIOps측 필드라 여기선 선택/패스스루.

응답:
```json
{"data": {
  "telemetry_connection_id": null,
  "provider": "loki",
  "status": "pass",
  "result_ref": null,
  "result_excerpt": "payment timeout after 3000ms",
  "row_count": 12,
  "started_at": "...", "completed_at": "..."
}}
```
`telemetry_connection_id` / `result_ref`는 AIOps측이 채우므로 single_gateway에선 null.

에러: 400 `TELEMETRY_QUERY_INVALID` / 403 `INTERNAL_CALL_FORBIDDEN` /
404 `TELEMETRY_CONNECTION_NOT_FOUND` / 502 `TELEMETRY_PROVIDER_QUERY_FAILED`.

### probe (무인증)
등록 verify(§3.3.5)가 single_gateway 및 provider routing 도달성을 확인하는 경로. verify가
토큰을 보내지 않으므로 무인증이며 read-only health/route로만 라우팅한다.
- `GET /` — single_gateway handshake
- `GET /-/healthy` → Prometheus
- `GET /loki/api/v1/labels` → Loki
- `GET /api/echo` → Tempo
- `GET /api/v1/events` → Loki(k8s 이벤트 적재소)
- `GET /healthz` — k8s probe

## 설정 (환경변수)
- `HEIMDALL_READ_TOKEN` — Bearer 토큰. 비우면 query 인증 비활성(개발).
- `UPSTREAM_PROMETHEUS` / `UPSTREAM_LOKI` / `UPSTREAM_TEMPO` — 내부 LGTM host:port.
- `QUERY_TIMEOUT_SECONDS` — upstream 타임아웃(기본 20).

차트(`charts/heimdall-monitoring`)가 `values.gateway.upstreams`로 주입한다.

## 개발
```bash
pip install -e '.[test]'
PYTHONPATH=. pytest -q
uvicorn app.main:app --port 8080
```
