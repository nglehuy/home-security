# hsec-db-rustfs

RustFS 1.0.1 is the S3-compatible object store. It runs as one pod in standalone mode. It has two buckets:

- `hsec-lake`: the Iceberg data files, as Parquet.
- `hsec-flink`: the Flink checkpoints, savepoints, and job recovery data.

## Deploy

- Terraform root: `terraform/010-workload`.
- Module inputs: HDD class, Secret `rustfs-root`.
- Chart: `rustfs` 1.0.1 from `https://charts.rustfs.com`, installed with `helm_release`.
- Buckets: the Job `rustfs-buckets` creates them. A second run changes nothing.
- Address: `http://rustfs-svc.hsec.svc.cluster.local:9000`. The console on port 9001 stays inside the cluster.
- Volumes: 400 GiB for data and 1 GiB for logs, on the HDD class.
- Resources: memory 512 MiB, CPU request 0.1.

## Files

- `terraform/`: the Terraform module and its tests.

## Test

In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`.

## Details

See [RustFS](../docs/specs.md#rustfs) in the specs.
