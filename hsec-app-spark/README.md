# hsec-app-spark

Spark 4.0.4 runs the batch job `hsec-batch` every 15 minutes. The Spark Operator 2.5.2 starts each run. The job reads the Iceberg tables, computes the stats, and appends them to ClickHouse (`hsec-db-clickhouse`). The first run at or after 03:00 local time also cleans and compacts the Iceberg tables.

## Deploy

- Terraform root: `terraform/020-app`. Apply `terraform/010-workload` first, because it creates RustFS, Postgres, ClickHouse, and the Secret.
- Module inputs: image, time zone, night window, Secret `spark-env`.
- Operator: chart `spark-operator` 2.5.2, installed with `helm_release`.
- Job: the local chart `terraform/chart/` holds the ScheduledSparkApplication `hsec-batch`. The module installs it with `helm_release` after the operator.
- Schedule: `*/15 * * * *` with `concurrencyPolicy: Forbid`, so only one run is active at a time.
- Reads from: the Iceberg catalog `lake`.
- Writes to: the ClickHouse database `hsec`, as the user `spark_writer`, on HTTP port 8123.
- Resources: operator 256 MiB and CPU 0.1. During a run, the driver uses 1 GiB and CPU 0.25, and the executor uses 1 GiB and CPU 0.5.

Before you apply the root, build and push the image. Run this command from the repository root:

```sh
docker buildx build --platform linux/amd64 -t <registry>/hsec-app-spark:<tag> --push hsec-app-spark/
```

## Files

- `pom.xml`: the Maven project.
- `src/main/java/`: the batch job and the transforms. The main class is `hsec.batch.BatchJob`.
- `src/test/java/`: the JUnit 5 tests.
- `Dockerfile`: the image, based on `apache/spark:4.0.4-scala2.13-java17-ubuntu`. The job jar is at `/opt/hsec/hsec-batch.jar`.
- `terraform/`: the Terraform module and its tests.
- `terraform/chart/`: the local Helm chart of the ScheduledSparkApplication.

## Test

- Run the JUnit 5 tests with Maven. They use a local SparkSession and call the transform classes from `src/main/java`.
- In `terraform/chart/`, run `helm lint` and `helm template`.
- In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`.

## Details

See [Spark](../docs/specs.md#spark), [Retention and maintenance](../docs/specs.md#retention-and-maintenance), and [Container images](../docs/specs.md#container-images) in the specs.
