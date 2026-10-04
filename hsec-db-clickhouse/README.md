# hsec-db-clickhouse

ClickHouse 26.3.39.7 (LTS) stores the stats that Spark (`hsec-app-spark`) computes. This module also sets up the existing Grafana with one data source and four dashboards.

## Deploy

- Terraform root: `terraform/010-workload`.
- Module inputs: HDD class, time zone, ClickHouse passwords, Grafana namespace.
- Pod: StatefulSet with 1 replica, image `clickhouse/clickhouse-server:26.3.39.7`.
- Service: `clickhouse`, type `ClusterIP`, ports 8123 (HTTP) and 9000 (native).
- Volume: PVC `clickhouse-data` (HDD, 5 GiB).
- Database: the pod sets `CLICKHOUSE_DB=hsec`, so the image creates the database `hsec` at its first start.
- Tables: Terraform creates no tables. You run the migrations in `migrations/` by hand. See [Migrate](#migrate).
- Users: `hsec_admin` for the migrations, `spark_writer` for Spark, and `grafana_reader` for Grafana.
- Grafana: the data source `hsec-clickhouse`, the folder "Home security", and four dashboards. The root configures the Grafana provider with `grafana_url` and `grafana_auth`.
- Resources: memory 1.5 GiB, CPU request 0.25.

## Files

- `config.d/hsec.xml`: the server configuration. It sets the time zone and the memory limit. It also deletes system log rows after 7 days.
- `users.d/hsec.xml.tftpl`: the users. Terraform fills in the password hashes.
- `migrations/`: the golang-migrate files. `0001_init.up.sql` creates the five tables, and `0001_init.down.sql` drops them.
- `grafana/dashboards/`: the four dashboard JSON files.
- `terraform/`: the Terraform module and its tests.

## Migrate

You need golang-migrate 4.20.1 on the admin machine. Run the migrations after `terraform/010-workload` and before `terraform/020-app`.

1. In a first terminal, run `kubectl -n hsec port-forward svc/clickhouse 9000:9000`.
2. In a second terminal, go to the repository root.
3. Run the commands below.

```sh
CH_PASSWORD=$(kubectl -n hsec get secret clickhouse-admin -o jsonpath='{.data.CLICKHOUSE_ADMIN_PASSWORD}' | base64 -d)
migrate -path hsec-db-clickhouse/migrations \
  -database "clickhouse://localhost:9000?username=hsec_admin&password=${CH_PASSWORD}&database=hsec&x-multi-statement=true" \
  up
```

Do not run `migrate down` on the cluster. It deletes the tables and their data.

To add a migration, run `migrate create -ext sql -dir migrations -seq -digits 4 <name>` in this folder. Never change a file after it ran on the cluster.

## Test

- With `clickhouse local`, run `CREATE DATABASE hsec`. Then run each `.up.sql` file in order, and each `.down.sql` file in reverse order. Make sure that no statement fails.
- In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`.

## Details

See [ClickHouse](../docs/specs.md#clickhouse), [ClickHouse migrations](../docs/specs.md#clickhouse-migrations), and [Grafana setup](../docs/specs.md#grafana-setup) in the specs.
