{{- define "springboot-app.name" -}}
springboot-app
{{- end -}}

{{- define "springboot-app.fullname" -}}
{{ .Release.Name }}-springboot-app
{{- end -}}
