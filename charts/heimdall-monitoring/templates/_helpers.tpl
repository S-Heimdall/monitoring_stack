{{- define "heimdall-monitoring.name" -}}
heimdall-monitoring
{{- end -}}

{{- define "heimdall-monitoring.labels" -}}
app.kubernetes.io/name: {{ include "heimdall-monitoring.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
{{- end -}}
