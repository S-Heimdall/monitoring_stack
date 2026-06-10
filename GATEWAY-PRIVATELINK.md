# read-gateway 단독 배포 + telemetry PrivateLink (HEIM-49 / HEIM-47 선행)

목적: 데모 클러스터에 **read-gateway만** 띄워(기존 LGTM 스택 교체 없이) internal NLB를 만들고,
그 NLB를 백엔드로 **telemetry 방향 PrivateLink**(AIOps 에이전트 → 데모 게이트웨이)를 붙인다.
alert 방향 PrivateLink(HEIM-78, 데모 AM → AIOps ingest)의 **대칭(반대 방향)**이다.

> 이건 LGTM 마이그레이션(DOGFOODING-RUNBOOK.md)과 독립이다. 게이트웨이만 추가하므로
> 현행 모니터링/alert 흐름에 영향 없음. upstream을 현행 서비스명으로 가리킨다.

## 1. read-gateway 단독 설치 (LGTM 서브차트 off)

```bash
DEMO=arn:aws:eks:ap-northeast-2:881490135253:cluster/skala3-cloud1-finalproj-team6
GW=$(openssl rand -hex 32)            # 게이트웨이 Bearer — AIOps 에이전트와 공유
helm --kube-context "$DEMO" upgrade --install heimdall-gateway \
  ./charts/heimdall-monitoring -n monitoring \
  --set lgtm.prometheus.enabled=false --set lgtm.loki.enabled=false \
  --set lgtm.tempo.enabled=false --set lgtm.alloy.enabled=false \
  --set global.heimdall.webhookEnabled=false \
  --set gateway.token="$GW" \
  --set gateway.upstreams.prometheus=kps-kube-prometheus-stack-prometheus:9090 \
  --set gateway.upstreams.loki=loki:3100 \
  --set gateway.upstreams.tempo=tempo:3200
echo "게이트웨이 토큰(GW) 안전 보관·공유: $GW"
kubectl --context "$DEMO" -n monitoring rollout status deploy/heimdall-monitoring-gateway
```

렌더 검증(helm template)으로 이 설정은 게이트웨이 리소스 4개(ConfigMap/Deployment/Secret/Service)만
만들고 upstream이 현행 서비스명을 가리키는 것을 확인했다. LGTM/AM은 건드리지 않는다.

설치 후 클러스터 내부 동작 확인:
```bash
kubectl --context "$DEMO" -n monitoring run gwtest --rm -it --image=curlimages/curl --restart=Never -- \
  -sG http://heimdall-monitoring-gateway/prometheus/api/v1/query \
  -H "Authorization: Bearer $GW" --data-urlencode 'query=up' | head -c 300
```

## 2. 게이트웨이 NLB ARN 확보

```bash
# AWS LB Controller가 만든 internal NLB. provisioning에 1~2분.
kubectl --context "$DEMO" -n monitoring get svc heimdall-monitoring-gateway \
  -o jsonpath='{.metadata.annotations.service\.k8s\.aws/load-balancer-arn}'; echo
# 없으면 콘솔/elbv2로 heimdall-monitoring-gateway 대상 NLB ARN 확인
```

## 3. 데모 VPC = provider (Endpoint Service)

alert-ingest 때 AIOps 쪽에서 했던 것과 동일, 방향만 데모쪽. (AWS API는 사용자가 실행)

```bash
GW_NLB_ARN=<2번 ARN>
# Endpoint Service 생성(승인 불요) + AIOps 계정 allowed principal
aws ec2 create-vpc-endpoint-service-configuration --region ap-northeast-2 \
  --no-acceptance-required --network-load-balancer-arns "$GW_NLB_ARN" \
  --query 'ServiceConfiguration.ServiceName' --output text   # → com.amazonaws.vpce....vpce-svc-xxxx
aws ec2 modify-vpc-endpoint-service-permissions --region ap-northeast-2 \
  --service-id <위 vpce-svc-id> \
  --add-allowed-principals arn:aws:iam::881490135253:root
```

## 4. AIOps VPC = consumer (Interface Endpoint)

bootstrap에 소비자 측 코드(`aws_vpc_endpoint.single_gateway`)가 이미 있다. 변수만 채워 apply.

```hcl
# bootstrap/terraform.tfvars (또는 -var)
service_endpoint_service_name = "com.amazonaws.vpce.ap-northeast-2.vpce-svc-xxxx"  # 3번 출력
```
```bash
cd bootstrap && terraform apply   # single_gateway Interface Endpoint + SG 생성
terraform output single_gateway_interface_endpoint_dns
```
> bootstrap 로컬 state가 비어 있으면 plain apply 금지(EKS까지 새로 만들려 함). alert-ingest 때처럼
> AWS CLI로 직접 `create-vpc-endpoint`(Interface, 데모 vpce-svc, AIOps subnet/SG)하는 우회가 안전.

## 5. 검증 (전 사이클)

AIOps 파드에서 vpce DNS로 게이트웨이 pull:
```bash
AIOPS=arn:aws:eks:ap-northeast-2:881490135253:cluster/heimdall-aiops-dev
kubectl --context "$AIOPS" -n heimdall run gwtest --rm -it --image=curlimages/curl --restart=Never -- \
  -sG http://<single_gateway vpce DNS>/prometheus/api/v1/query \
  -H "Authorization: Bearer $GW" --data-urlencode 'query=up' | head -c 300
# /loki/loki/api/v1/query_range, /tempo/api/search 도 동일 패턴
```

이러면 incident(scope.cluster_id로 대상 식별) → 에이전트가 이 게이트웨이로 로그/트레이스/메트릭 pull
→ RCA 까지의 telemetry 경로가 PrivateLink로 닫힌다.

## 후속 (HEIM-47)

현재 게이트웨이는 3-prefix(`/prometheus`·`/loki`·`/tempo`) nginx다. HEIM-47은 이를 단일
`POST /query/{provider}`(gateway.py)로 통일하는 것 — API 형태 리팩터이며 이 PrivateLink 경로와 독립.
PrivateLink가 붙은 뒤 엔드포인트 형태만 교체하면 된다.
