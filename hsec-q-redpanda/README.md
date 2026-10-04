# hsec-q-redpanda

Redpanda 26.2.3 stores the Frigate messages, the system messages, and the alerts for 7 days. It runs as one broker, without TLS and without the console.

## Deploy

- Terraform root: `terraform/010-workload`.
- Module inputs: SSD class.
- Chart: `redpanda` 26.2.4 from `https://charts.redpanda.com`, installed with `helm_release`.
- Topics: the Job `redpanda-topics` creates 14 topics. Automatic topic creation is off.
- Kafka API: `redpanda-0.redpanda.hsec.svc.cluster.local:9093`.
- Admin API: `redpanda-0.redpanda.hsec.svc.cluster.local:9644`.
- Volume: 10 GiB on the SSD class.
- Resources: memory 2.5 GiB, CPU 1.

Each topic has 1 partition and 1 replica. The topic `hsec.alerts` accepts messages up to 32 MiB, because an alert message can carry a clip part.

## Files

- `terraform/`: the Terraform module and its tests.

## Test

In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`.

## Details

See [Redpanda](../docs/specs.md#redpanda), [Message contract](../docs/specs.md#message-contract), and [Video reference](../docs/specs.md#video-reference) in the specs.
