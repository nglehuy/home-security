# terraform/010-workload

This is Terraform root 1. You apply it first. It creates the base that the apps in `terraform/020-app` need.

## Contents

- The namespace `hsec`.
- All passwords, with `random_password`, and all Kubernetes Secrets.
- The network policies of the namespace `hsec`.
- The modules `hsec-db-rustfs`, `hsec-q-redpanda`, `hsec-q-mqtt`, `hsec-db-clickhouse`, and `hsec-db-iceberg`.
- The schema migrations, in `migrations.tf`. See [Migrations](#migrations).

## Migrations

Two Jobs run the files in `hsec-db-clickhouse/migrations` and `hsec-db-iceberg/migrations`. Each Job gets its files from a ConfigMap.

| Job | Image | Runs after | What it does |
| --- | --- | --- | --- |
| `clickhouse-migrations-<hash>` | `migrate/migrate:v4.20.1` | ClickHouse | `migrate up` as `hsec_admin`. golang-migrate records the version in `hsec.schema_migrations`. |
| `iceberg-migrations-<hash>` | `apache/spark:4.0.4-scala2.13-java17-ubuntu` | Postgres and RustFS | `spark-sql` on each `.up.sql` file, in order. `--packages` downloads the Iceberg, AWS, and Postgres jars from Maven Central. |

- The hash in each Job name comes from its files. If a file changes, the hash changes, and the Job runs again. Otherwise it does not run again.
- Nothing records which Iceberg files ran. So each Iceberg file must change nothing on a second run.
- The ClickHouse driver loads the time zone of the server, and the `migrate` image has no time zone data. So the Job mounts `/usr/share/zoneinfo` of the node, read-only.
- Terraform waits until each Job completes. If a Job fails, the apply fails. Read its log with `kubectl -n hsec logs job/<name>`.

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

- Cluster: `kubeconfig_path`, `kube_context`, `namespace`.
- Storage and time: `ssd_storage_class`, `hdd_storage_class`, `timezone`.
- Web UIs, optional: `domain`, default `hsec.home.arpa`. RustFS gets the host `rustfs.<domain>`. Use the same value in `terraform/020-app`. See [Web UIs](../../docs/specs.md#web-uis).
- Grafana, optional: `grafana_namespace`. If you set it, a network policy lets the Grafana pods reach ClickHouse on port 8123.
- Sensitive: `discord_webhook_url`, `frigate_rtsp_password`.

The Grafana data source and dashboards are not part of this root yet.

## State

Terraform stores the state in a Kubernetes Secret in `kube-system`, with the suffix `hsec-010-workload`. The state holds all passwords in plain text. Only cluster admins can read Secrets in `kube-system`.

## Next step

Build and push the two custom images. Then apply `terraform/020-app`. See its [README](../020-app/README.md).

## Test

Run `terraform validate` and `terraform test`. The tests cover the module calls, the Secrets, the network policies, the migration Jobs, and the check of `domain`.

## Details

See [Roots](../../docs/specs.md#roots), [Secrets](../../docs/specs.md#secrets), [Variables](../../docs/specs.md#variables), and [Network policies](../../docs/specs.md#network-policies) in the specs.
