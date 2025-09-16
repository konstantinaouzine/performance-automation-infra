# ansible_grafana

Ansible role and playbook for deploying Grafana to Kubernetes via Helm.

## Usage

1. Запусти:
   ```bash
   ansible-playbook -i inventory.ini playbook_grafana.yml
   ```
2. После деплоя:
   - Grafana будет доступна на сервисе `grafana` в namespace `monitoring` (порт 3000).
   - Логин: admin / grafanaadmin

## Настройки
- Values-файл: `roles/deploy_grafana/files/grafana-values.yaml`
- Можно добавить ingress, dashboards, datasources по необходимости.
