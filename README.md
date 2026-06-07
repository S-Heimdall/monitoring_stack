# Heimdall Monitoring Stack

Helm package installed into the service cluster. It exposes a `single_gateway` that proxies telemetry query traffic from the AIOps cluster to in-cluster providers.

## Render

```bash
helm lint charts/heimdall-monitoring
helm template heimdall-monitoring charts/heimdall-monitoring --namespace monitoring
```
