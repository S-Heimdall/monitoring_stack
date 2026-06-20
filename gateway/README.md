# single_gateway

서비스 클러스터(데모) 내부에 설치되는 telemetry 조회 진입점. AIOps `telemetry-query-gateway`가
PrivateLink로 호출하면, provider로 내부 LGTM(Prometheus/Loki/Tempo)와 Kubernetes API에 read-only query를 라우팅한다.
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
- `provider`: prometheus | mimir | loki | tempo | kubernetes | kubernetes_log_signal | k8s_events
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
  "rows": [
    {
      "service": "checkout",
      "namespace": "otel-demo",
      "pod": "checkout-abc",
      "container": "checkout",
      "labels": {"service_name": "checkout", "k8s_namespace_name": "otel-demo"}
    }
  ],
  "started_at": "...", "completed_at": "..."
}}
```
`telemetry_connection_id` / `result_ref`는 AIOps측이 채우므로 single_gateway에선 null.

### Agent evidence label contract
`rows`는 provider 원본 라벨을 `labels`에 보존하면서 Agent evidence pack이 공통으로 읽을 수 있는 필드를 함께 노출한다.

| provider | query label 기준 | row 공통 필드 |
| --- | --- | --- |
| `prometheus`/`mimir` service RED | `service_name`, `k8s_namespace_name` | `service`, `namespace`, `labels`, `sample_count`, `observed_value`, `max_value` |
| `prometheus`/`mimir` infra/container | `namespace`, `pod`, `container` | `namespace`, `pod`, `container`, `labels`, range summary |
| `loki` | `service_name`, `k8s_namespace_name`, `k8s_pod_name`, `k8s_container_name` | `message`, `service`, `namespace`, `pod`, `container`, `level`, `labels` |
| `tempo` | TraceQL search + trace detail | `trace_id`, `span_id`, `parent_span_id`, `service`, `operation`, `duration_ms`, `status`, `critical_path_rank`, `labels` |
| `kubernetes` | Kubernetes API Events/Pods/Deployments/ReplicaSets/HPA | `kind`, `name`, `namespace`, `reason`, `message`, `involved_object`, `event_time`, `replicas`, `current_replicas`, `desired_replicas`, `max_replicas`, `conditions` |
| `kubernetes_log_signal` / `k8s_events` | Loki에 적재된 Kubernetes-label 로그 signal | `message`, `namespace`, `pod`, `container`, `labels`, `evidence_source=kubernetes_log_signal`, `real_kubernetes_event=false` |

`k8s_events`는 backward-compatible alias이며 real Kubernetes Event가 아니다. RCA에서 Kubernetes Event/상태로
판단해야 하는 값은 `provider=kubernetes`의 Kubernetes API row만 사용한다.

Agent RCA가 checkout 증상과 payment 원인을 연결할 수 있도록 gateway는 provider별 row/excerpt를 비워서 성공 처리하지 않고, upstream이 반환한 결과를 위 공통 필드와 raw `labels`로 같이 전달한다.

에러: 400 `TELEMETRY_QUERY_INVALID` / 403 `INTERNAL_CALL_FORBIDDEN` /
404 `TELEMETRY_CONNECTION_NOT_FOUND` / 502 `TELEMETRY_PROVIDER_QUERY_FAILED`.

### probe (무인증)
등록 verify(§3.3.5)가 single_gateway 및 provider routing 도달성을 확인하는 경로. verify가
토큰을 보내지 않으므로 무인증이며 read-only health/route로만 라우팅한다.
- `GET /` — single_gateway handshake
- `GET /-/healthy` → Prometheus
- `GET /loki/api/v1/labels` → Loki
- `GET /api/echo` → Tempo
- `GET /api/v1/events` → Loki(k8s log-signal compatibility route)
- `GET /healthz` — k8s probe

## 설정 (환경변수)
- `HEIMDALL_READ_TOKEN` — Bearer 토큰. 비우면 query 인증 비활성(개발).
- `UPSTREAM_PROMETHEUS` / `UPSTREAM_LOKI` / `UPSTREAM_TEMPO` — 내부 LGTM host:port.
- Kubernetes API는 pod service account(`KUBERNETES_SERVICE_HOST`, token, CA)를 사용한다.
- `QUERY_TIMEOUT_SECONDS` — upstream 타임아웃(기본 20).

차트(`charts/heimdall-monitoring`)가 `values.gateway.upstreams`로 주입한다.

## 개발
```bash
pip install -e '.[test]'
PYTHONPATH=. pytest -q
uvicorn app.main:app --port 8080
```
