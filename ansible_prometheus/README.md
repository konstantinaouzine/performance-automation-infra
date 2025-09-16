# ansible_prometheus

Ansible role and playbook for installing Prometheus via Helm in Kubernetes.

## Structure
- `roles/deploy_prometheus/` — main role for Prometheus installation
- `inventories/hosts.ini` — inventory file
- `deploy_prometheus.yml` — playbook entry point

## Usage
```bash
ansible-playbook -i inventories/hosts.ini deploy_prometheus.yml
```
