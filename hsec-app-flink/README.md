# hsec-app-flink

Flink 2.2.1 runs the stream job `hsec-stream`. The Flink Kubernetes Operator 1.16.1 runs and upgrades the job. The job does these steps:

- Reads all `frigate.*` topics from Redpanda (`hsec-q-redpanda`) and removes duplicate messages.
- Writes the Iceberg tables in the catalog `lake`, and builds visits.
- For each alert review, sends an alert with a snapshot image to `hsec.alerts` at once. After the review ends, it sends the review video as clip parts.

## Deploy

- Terraform root: `terraform/020-app`. Apply `terraform/010-workload` first, because it creates Redpanda, RustFS, Postgres, and the Secrets.
- Module inputs: image, time zone, `frigate_url`, Secrets `flink-env` and `rustfs-root`.
- Operator: chart `flink-kubernetes-operator` 1.16.1, installed with `helm_release`.
- Job: the local chart `terraform/chart/` holds the FlinkDeployment `hsec-stream`. The module installs it with `helm_release` after the operator.
- Reads from: Redpanda, and the Frigate API at `http://frigate-api.hsec.svc.cluster.local:5000`.
- Writes to: the Iceberg catalog `lake`, the topic `hsec.alerts`, and the checkpoints in the bucket `hsec-flink`.
- Resources: TaskManager 1.5 GiB and CPU 0.5, JobManager 768 MiB and CPU 0.25, operator 512 MiB and CPU 0.1.

The job does not create tables. Before you apply the root, run the Iceberg migrations in `hsec-db-iceberg`.

Also build and push the image. Run this command from the repository root:

```sh
docker buildx build --platform linux/amd64 -t <registry>/hsec-app-flink:<tag> --push hsec-app-flink/
```

## Files

- `pom.xml`: the Maven project.
- `src/main/java/`: the stream job.
- `src/test/java/`: the JUnit 5 tests.
- `Dockerfile`: the image, based on `flink:2.2.1-scala_2.12-java17`. The job jar is at `/opt/flink/usrlib/hsec-stream.jar`.
- `terraform/`: the Terraform module and its tests.
- `terraform/chart/`: the local Helm chart of the FlinkDeployment.

## Test

- Run the JUnit 5 tests with Maven. They use the Flink test utilities and a Flink MiniCluster. They create their Iceberg test tables in Java code, so the tables must match `hsec-db-iceberg/migrations/`.
- In `terraform/chart/`, run `helm lint` and `helm template`.
- In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`.

## Details

See [Flink](../docs/specs.md#flink), [Iceberg](../docs/specs.md#iceberg), and [Container images](../docs/specs.md#container-images) in the specs.
