# terraform/010-workload

This is Terraform root 1. You apply it first. It creates the base that the apps in `terraform/020-app` need.

## Contents

- Namespaces `hsec` and `intel-gpu-plugin`.
- The Intel GPU plugin, as a DaemonSet with the image `intel/intel-gpu-plugin:0.37.1`.
- All passwords, with `random_password`, and all Kubernetes Secrets.
- The network policies of the namespace `hsec`.
- The modules `hsec-db-rustfs`, `hsec-q-redpanda`, `hsec-q-mqtt`, `hsec-db-clickhouse`, and `hsec-db-iceberg`.

## Before you apply

- Make sure that the cluster meets the [Prerequisites](../../docs/system-design.md#prerequisites).
- Make sure that your kubeconfig can reach the cluster as a cluster admin.

## Apply

1. Copy `terraform.tfvars.example` to `terraform.tfvars`.
2. Fill in the values. You can also set them as `TF_VAR_*` environment variables.
3. Run `terraform init`.
4. Run `terraform apply`.

Do not commit `terraform.tfvars`, because it holds secret values. [Secrets](../../docs/specs.md#secrets) lists the `.gitignore` lines that exclude it.

## Variables

- Cluster: `kubeconfig_path`, `kube_context`, `namespace`, `node_name`.
- Storage and time: `ssd_storage_class`, `hdd_storage_class`, `timezone`.
- Grafana: `grafana_url`, `grafana_namespace`.
- Sensitive: `discord_webhook_url`, `frigate_rtsp_password`, `grafana_auth`.

## State

Terraform stores the state in a Kubernetes Secret in `kube-system`, with the suffix `hsec-010-workload`. The state holds all passwords in plain text. Only cluster admins can read Secrets in `kube-system`.

## Next step

Build and push the two custom images. Run the ClickHouse and the Iceberg migrations. Then apply `terraform/020-app`. See its [README](../020-app/README.md).

## Test

Run `terraform validate` and `terraform test`. The tests cover the module calls, the Secrets, and the network policies.

## Details

See [Roots](../../docs/specs.md#roots), [Secrets](../../docs/specs.md#secrets), [Variables](../../docs/specs.md#variables), [Intel GPU plugin](../../docs/specs.md#intel-gpu-plugin), and [Network policies](../../docs/specs.md#network-policies) in the specs.
