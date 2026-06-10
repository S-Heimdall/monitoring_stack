# HEIM-49 dogfooding 설치 runbook

데모 클러스터(`skala3-cloud1-finalproj-team6`)의 ad-hoc 모니터링을 이 차트로 교체해
single_gateway 경로를 실증하고, kps/alloy 버전 드리프트를 수렴한다.

> 이 작업은 **라이브 모니터링 교체**다. 실행 전 아래 주의와 사전 캡처를 반드시 읽는다.

## 현행 → 목표

| | 현행(ad-hoc 4 릴리스) | 목표(이 차트 1 릴리스) |
|---|---|---|
| Prometheus | `kps` kube-prometheus-stack **85.2.0**, svc `kps-kube-prometheus-stack-prometheus` | **86.2.0**, svc `heimdall-kps-prometheus` |
| Alloy | `alloy` **1.8.1** | **1.8.2** |
| Loki | `loki` 7.0.0, svc `loki` | 7.0.0, svc `heimdall-loki` |
| Tempo | `tempo` 1.24.4, svc `tempo` | 1.24.4, svc `heimdall-tempo` |
| 게이트웨이 | 없음(엔드포인트 3분리) | read-gateway 단일 진입점 `heimdall-monitoring-gateway` |

## 주의(위험 요소)

- **서비스 DNS 변경**: `kps-kube-prometheus-stack-prometheus` → `heimdall-kps-prometheus` 등. 옛 이름을
  참조하는 것(Grafana 데이터소스, alert generator_url 등)이 바뀐다. AM→Heimdall webhook은 vpce 주소라 영향 없음.
- **CRD 버전업**: kps 85.2.0→86.2.0. helm은 CRD를 자동 업그레이드하지 않으므로 1단계에서 수동 적용.
- **텔레메트리 초기화**: 새 릴리스 = 새 PVC. Prometheus/Loki/Tempo 히스토리는 리셋된다(데모라 허용).
- **모니터링 갭**: 기존 제거~신규 설치 사이 수 분간 수집/알람 중단. 그 사이 발화 중이던 알람은
  설치 후 fresh로 재발화 → FK 수정본대로 새 incident 생성됨.
- **토큰 연속성(중요)**: AM→Heimdall webhook 토큰은 **기존 값 그대로** 넣는다. 이 토큰이 ingest의
  `ALERT_WEBHOOK_TOKEN` 및 `alert_source_connections.webhook_token`(정렬해 둔 값)과 같아야
  alert 수신·FK 해석이 끊기지 않는다. 새로 생성하지 말 것.

전제: kubectl/helm 컨텍스트 = 데모 `skala3-cloud1-finalproj-team6`, `gp3-standard` StorageClass + EBS CSI.

## 0. 사전 캡처(롤백·연속성용)

```bash
DEMO=arn:aws:eks:ap-northeast-2:881490135253:cluster/skala3-cloud1-finalproj-team6
# 기존 AM webhook 토큰(= 그대로 재사용). 변수에만 담고 출력하지 말 것.
WT=$(kubectl --context "$DEMO" -n monitoring get secret heimdall-webhook-token -o jsonpath='{.data.token}' | base64 -d)
# 현재 릴리스/값 백업(롤백용)
helm --kube-context "$DEMO" -n monitoring list
helm --kube-context "$DEMO" -n monitoring get values kps   > /tmp/kps-values.bak.yaml
helm --kube-context "$DEMO" -n monitoring get values alloy > /tmp/alloy-values.bak.yaml
```

vpce webhook URL(고정):
`http://vpce-01dbf67c55bb7b6c1-konprmmj.vpce-svc-045bed1b84449bd54.ap-northeast-2.vpce.amazonaws.com:8000/api/v1/alerts/webhook/alertmanager`

## 1. CRD 86.2.0 적용(수동)

```bash
KPS_CRD=https://raw.githubusercontent.com/prometheus-community/helm-charts/kube-prometheus-stack-86.2.0/charts/kube-prometheus-stack/charts/crds/crds
for c in alertmanagerconfigs alertmanagers podmonitors probes prometheusagents \
         prometheuses prometheusrules scrapeconfigs servicemonitors thanosrulers; do
  kubectl --context "$DEMO" apply --server-side -f "$KPS_CRD/crd-${c}.yaml"
done
```

## 2. 기존 ad-hoc 릴리스 제거

```bash
# 제거 순서 무관. kps 제거 시 Prometheus/Alertmanager CR이 내려가 알람이 잠시 멈춘다.
helm --kube-context "$DEMO" -n monitoring uninstall kps alloy loki tempo
# (선택) 옛 PVC 정리 — 새 릴리스는 새 PVC를 만든다. 데이터 보존 불필요하면:
# kubectl --context "$DEMO" -n monitoring delete pvc -l app.kubernetes.io/instance=kps
```

## 3. 차트 설치

```bash
GW=$(openssl rand -hex 32)          # read-gateway Bearer(신규) — 에이전트 팀과 공유
helm --kube-context "$DEMO" upgrade --install heimdall-monitoring \
  ./charts/heimdall-monitoring -n monitoring --create-namespace \
  --set kps.prometheus.prometheusSpec.externalLabels.cluster=skala3-cloud1-finalproj-team6 \
  --set gateway.token="$GW" \
  --set global.heimdall.webhookUrl='http://vpce-01dbf67c55bb7b6c1-konprmmj.vpce-svc-045bed1b84449bd54.ap-northeast-2.vpce.amazonaws.com:8000/api/v1/alerts/webhook/alertmanager' \
  --set global.heimdall.webhookToken="$WT"
echo "read-gateway 토큰(GW)을 안전히 보관·공유: $GW"
```

`heimdall_project=otel-demo`는 차트 기본값이라 생략 가능. (Helm repo 공개 후엔 `./charts/...` 대신
`heimdall/heimdall-monitoring` 사용.)

## 4. 검증

```bash
kubectl --context "$DEMO" -n monitoring rollout status deploy/heimdall-monitoring-gateway
kubectl --context "$DEMO" -n monitoring get pods
kubectl --context "$DEMO" -n monitoring get svc | grep -E 'heimdall-(kps-prometheus|loki|tempo)|gateway'
# AM webhook이 vpce로 가는지(설정 확인)
kubectl --context "$DEMO" -n monitoring get secret alertmanager-heimdall-kps-alertmanager-generated \
  -o jsonpath='{.data.alertmanager\.yaml\.gz}' | base64 -d | gunzip | grep -A1 npo-webhook
```

기능 검증(전 사이클): `otel-demo/scripts/loadgen.sh scenario checkout 80 75`로 장애 주입 →
AIOps DB에서 새 alert/incident에 `project_id=proj_e28fe8184eab`,`cluster_id=clu_c3f556988981`
적재 확인(이전 검증과 동일) → 정리 `scenario stop`.

게이트웨이 단일 쿼리(클러스터 내부에서):
```bash
kubectl --context "$DEMO" -n monitoring run gwtest --rm -it --image=curlimages/curl --restart=Never -- \
  -sG http://heimdall-monitoring-gateway/prometheus/api/v1/query \
  -H "Authorization: Bearer $GW" --data-urlencode 'query=up' | head -c 300
```

## 5. 게이트웨이 PrivateLink(consumer 방향, 후속)

read-gateway는 internal NLB로 뜬다. AIOps의 에이전트가 cross-VPC로 PromQL/LogQL/TraceQL을
pull하려면 alert-ingest와 **반대 방향** PrivateLink(데모 gateway NLB → Endpoint Service →
AIOps Interface Endpoint)가 필요하다. bootstrap의 single_gateway(main.tf) 블록 / 등록 UI가 생성.

## 6. 롤백

```bash
helm --kube-context "$DEMO" -n monitoring uninstall heimdall-monitoring
# 0단계 백업값으로 기존 스택 재설치(OTel_Demo/monitoring/helm/*.yaml 기준)
helm --kube-context "$DEMO" -n monitoring upgrade --install kps \
  prometheus-community/kube-prometheus-stack --version 85.2.0 -f /tmp/kps-values.bak.yaml
# alloy/loki/tempo도 동일하게 백업값으로 재설치
```
