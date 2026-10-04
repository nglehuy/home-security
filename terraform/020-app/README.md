# terraform/020-app

This is Terraform root 2. You apply it after `terraform/010-workload`. It installs the apps that use the data services of root 1.

## Contents

- `hsec-app-frigate`: Frigate.
- `hsec-conn-mqtt`: the Redpanda Connect bridge from MQTT to Redpanda.
- `hsec-conn-discord`: the Redpanda Connect notifier from Redpanda to Discord.
- `hsec-app-flink`: the Flink operator and the stream job.
- `hsec-app-spark`: the Spark operator and the batch job.

This root creates nothing directly. It only calls the modules.

## Before you apply

1. Apply `terraform/010-workload`. This root uses its Secrets and its services.
2. Build and push the Flink image. Run the command from the repository root.
3. Build and push the Spark image. Run the command from the repository root.

```sh
docker buildx build --platform linux/amd64 -t <registry>/hsec-app-flink:<tag> --push hsec-app-flink/
docker buildx build --platform linux/amd64 -t <registry>/hsec-app-spark:<tag> --push hsec-app-spark/
```

4. Run the ClickHouse migrations. See the [hsec-db-clickhouse README](../../hsec-db-clickhouse/README.md#migrate).
5. Run the Iceberg migrations. See the [hsec-db-iceberg README](../../hsec-db-iceberg/README.md#migrate).

The jobs do not create tables. If an Iceberg table is missing, the Flink job fails at start.

## Apply

1. Copy `terraform.tfvars.example` to `terraform.tfvars`.
2. Fill in the values. Use the same registry and tags as the images that you pushed.
3. Run `terraform init`.
4. Run `terraform apply`.

## Variables

- Cluster: `kubeconfig_path`, `kube_context`, `namespace`.
- Storage and time: `ssd_storage_class`, `hdd_storage_class`, `timezone`.
- Images: `image_registry`, `flink_image_tag`, `spark_image_tag`.
- Apps: `cameras`, `night_start`, `night_end`, `frigate_url`.

## State

Terraform stores the state in a Kubernetes Secret in `kube-system`, with the suffix `hsec-020-app`. The state holds the values that the modules read from Secrets, for example the RustFS keys for Flink. Only cluster admins can read Secrets in `kube-system`.

## Test

Run `terraform validate` and `terraform test`. The tests cover the module calls.

## Details

See [Roots](../../docs/specs.md#roots), [Variables](../../docs/specs.md#variables), and [Container images](../../docs/specs.md#container-images) in the specs.
