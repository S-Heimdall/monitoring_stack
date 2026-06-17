# Heimdall Monitoring Stack

서비스/고객 EKS 클러스터(모니터링 대상)에 LGTM 관측 스택과 단일 read-gateway 를 한 번에 설치하는 Helm 패키지.
`OTel_Demo/monitoring` 에서 검증한 설정을 그대로 차트화한 것이라, `helm install` 하나로 동일한 스택이 뜬다.

## 설치되는 것

| 컴포넌트 | 역할 |
|---|---|
| kube-prometheus-stack | 메트릭 + kube-state-metrics + node-exporter + Alertmanager + Operator + Grafana |
| Loki (SingleBinary) | 로그, EBS 영속 |
| Tempo (SingleBinary) | 트레이스 + service-graph metrics-generator, EBS 영속 |
| Grafana Alloy (DaemonSet) | 수집 hub — 컨테이너 로그/K8s 이벤트 → Loki, OTLP(4317/4318) → 메트릭/로그/트레이스 분배 |
| read-gateway (nginx) | 외부(NPO)가 PromQL/LogQL/TraceQL 을 pull 조회하는 단일 진입점(Bearer 인증 + 읽기전용 allowlist) |
| npo-alerts | PrometheusRule 13룰(scope/npo_category 라벨), Alertmanager 가 npo_category 알람만 webhook 으로 라우팅 |
| Grafana datasources/dashboard | Prometheus/Loki/Tempo/Alertmanager 데이터소스 + NPO Overview 대시보드 |

데이터 흐름: 앱 → otel-collector → Alloy → 메트릭=Prometheus / 로그=Loki / 트레이스=Tempo.
alert 는 Alertmanager → 원격 Heimdall webhook(push), telemetry 는 read-gateway(pull).

## 사전 조건

- EBS 영속을 쓰므로 `gp3-standard` StorageClass + EBS CSI 드라이버. 다른 클래스면 아래 곳들 override:
  `kps.prometheus.prometheusSpec.storageSpec...storageClassName`, `kps.alertmanager.alertmanagerSpec.storage...storageClassName`,
  `loki.singleBinary.persistence.storageClass`, `tempo.persistence.storageClassName`.
- Prometheus Operator: 기본 동봉(`kps.prometheusOperator.enabled=true`). 클러스터에 이미 있으면 `=false`.
- helm 3.8+ (또는 helm 4). CRD(ServiceMonitor/PrometheusRule)는 kube-prometheus-stack 이 설치.

## 설치 (Helm Repo)

GitHub Pages 가 활성화되면 Pages URL 을 Helm 저장소로 사용한다.

```bash
helm repo add heimdall https://s-heimdall.github.io/monitoring_stack
helm repo update

helm upgrade --install heimdall-monitoring heimdall/heimdall-monitoring \
  --namespace monitoring --create-namespace \
  --set kps.prometheus.prometheusSpec.externalLabels.cluster=<cluster-name> \
  --set kps.prometheus.prometheusSpec.externalLabels.heimdall_project=<project> \
  --set gateway.token=$(openssl rand -hex 32) \
  --set global.heimdall.webhookUrl=https://<npo-host>/api/v1/alerts/webhook/alertmanager \
  --set global.heimdall.webhookToken=<원격 Heimdall 과 공유할 토큰>
```

- 토큰은 `--set` 으로만 주입하고 git 에 커밋하지 않는다. 차트가 `heimdall-read-gateway-token`·
  `heimdall-webhook-token` Secret 으로 만들어 게이트웨이/Alertmanager 에 마운트한다.
- webhook URL 은 `global.heimdall.webhookUrl` 파라미터 한 곳에서 설정한다(kps 가 AM config 를 tpl 로
  렌더). 경로는 NPO 스펙상 `/api/v1/alerts/webhook/alertmanager` 로 고정이고 **host 만** NPO 실주소로
  바꾸면 된다. 미설정 시 placeholder(`heimdall.REPLACE_ME.example.com`)라 전송은 실패만 한다.

## 주요 values

| 키 | 기본값 | 설명 |
|---|---|---|
| `targetNamespaces` | `[otel-demo]` | 관측 대상 앱 ns(Alloy 수집 + alert namespace 매처). 릴리스 ns 는 자동 포함 |
| `kps.prometheus.prometheusSpec.externalLabels.cluster` | `""` | 소스 클러스터 식별(외부 alert 에 부착) — 설치 시 필수 |
| `gateway.enabled` / `gateway.token` | `true` / `""` | read-gateway 사용 여부 / Bearer 토큰 |
| `gateway.service.annotations` | internal NLB | 게이트웨이 LB 노출 방식 |
| `global.heimdall.webhookUrl` | `.../api/v1/alerts/webhook/alertmanager`(host=REPLACE_ME) | AM→Heimdall webhook 목적지. host 만 NPO 실주소로 |
| `global.heimdall.webhookToken` | `""` | AM→Heimdall webhook Bearer 토큰(양쪽 동일 값) |
| 각 `*.persistence` storageClass | `gp3-standard` | LGTM 영속 볼륨 클래스 |
| `kps.prometheusOperator.enabled` | `true` | operator 동봉(공유 클러스터면 false) |

## read-gateway 사용

```
GET  https://<gateway-LB>/prometheus/api/v1/query        # PromQL (읽기 전용 allowlist)
GET  https://<gateway-LB>/loki/loki/api/v1/query_range   # LogQL
GET  https://<gateway-LB>/tempo/api/search               # TraceQL
헤더: Authorization: Bearer <gateway.token>
```
토큰 없음/틀림 → 401, 쓰기·관리 경로 → 403.

## 소스 식별 라벨

외부로 나가는 신호(alert, remote-write)에 `cluster`·`heimdall_project` externalLabel 이 붙는다.
원격 Heimdall 은 이 라벨로 출처 클러스터/프로젝트를 매핑한다.

## PrivateLink

PrivateLink 는 Heimdall 프로젝트 등록 UI 가 런타임 값(서비스 VPC, AIOps VPC, allowed principal,
이 Helm install 이 만든 NLB)으로 생성한다. UI 가 두 Terraform 블록을 보여준다.
- 서비스 VPC: read-gateway internal NLB 로부터 `aws_vpc_endpoint_service`
- AIOps VPC: 서비스 endpoint 이름에 연결하는 `aws_vpc_endpoint`

## 렌더/검증

```bash
helm dependency build charts/heimdall-monitoring   # Chart.lock 기준 의존성 받기 (또는 update)
helm lint charts/heimdall-monitoring
helm template heimdall-monitoring charts/heimdall-monitoring -n monitoring \
  --set kps.prometheus.prometheusSpec.externalLabels.cluster=test
```

## 출처

이 차트는 `OTel_Demo/monitoring`(helm values + manifests)에서 실측·검증한 구성을 단일 설치 패키지로
정리한 것이다. 알람 정의·게이트웨이 동작·수집 파이프라인의 근거는 그 레포의 docs 에 있다.
