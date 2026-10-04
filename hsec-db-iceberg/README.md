# hsec-db-iceberg

Postgres 18.6 holds the Iceberg JDBC catalog `lake`. For each table, the catalog stores the location of its current metadata file. The table data is in RustFS (`hsec-db-rustfs`). This folder also holds the migrations that create and change the seven Iceberg tables.

## Deploy

- Terraform root: `terraform/010-workload`.
- Module inputs: SSD class, Secret `postgres-iceberg`.
- Pod: StatefulSet with 1 replica, image `postgres:18.6-alpine`.
- Service: `postgres`, type `ClusterIP`, port 5432.
- Volume: PVC `postgres-data` (SSD, 2 GiB), mounted at `/var/lib/postgresql`.
- Address: `postgres.hsec.svc.cluster.local:5432`, database `iceberg_catalog`, user `iceberg`.
- Resources: memory 512 MiB, CPU request 0.1.

The Iceberg JDBC catalog creates its own tables in Postgres on first use.

## Files

- `migrations/`: the Spark SQL migrations, with golang-migrate file names. `0001_init.up.sql` creates the namespace `hsec` and the seven tables.
- `migrate.sh`: runs one migration file with a local `spark-sql`, through tunnels to the cluster.
- `terraform/`: the Terraform module and its tests.

## Migrate

golang-migrate has no Iceberg driver, so `migrate.sh` runs the files with a local `spark-sql`. Nothing records which files ran, so you run each file once, in order.

You need Java 17 and Spark 4.0.4 (`spark-4.0.4-bin-hadoop3`) on the admin machine, with `spark-sql` on the `PATH`. Run the migrations after `terraform/010-workload` and before `terraform/020-app`.

1. In a first terminal, run `kubectl -n hsec port-forward svc/postgres 15432:5432`.
2. In a second terminal, run `kubectl -n hsec port-forward svc/rustfs-svc 19000:9000`.
3. In a third terminal, go to the repository root.
4. Run each new file with the script, for example:

```sh
hsec-db-iceberg/migrate.sh hsec-db-iceberg/migrations/0001_init.up.sql
```

The script reads the passwords from the Secret `spark-env`. It runs `spark-sql` in a temporary folder, with the `lake` catalog pointed at the two tunnels. Its exit code is the exit code of `spark-sql`. The first run downloads the Iceberg and Postgres jars from Maven Central.

To add a migration, run `migrate create -ext sql -dir migrations -seq -digits 4 <name>` in this folder. Then delete the `.down.sql` file, because a dropped Iceberg table loses its data. Never change a file after it ran on the cluster.

## Test

- Run each `.up.sql` file in order with a local `spark-sql` 4.0.4, with a Hadoop catalog named `lake` on a temporary folder. Make sure that no file fails.
- Run `shellcheck migrate.sh`.
- In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`.

## Details

See [Postgres](../docs/specs.md#postgres), [Iceberg](../docs/specs.md#iceberg), and [Iceberg migrations](../docs/specs.md#iceberg-migrations) in the specs.
