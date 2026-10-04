# hsec-conn-mqtt

The bridge is a Redpanda Connect 4.112.0 pipeline. It reads the general Frigate topics from Mosquitto (`hsec-q-mqtt`). It writes each message to the matching topic in Redpanda (`hsec-q-redpanda`).

The bridge adds the headers `video_camera`, `video_from`, and `video_to` to each camera message. A camera message without a camera or a time goes to `hsec.rejected`. System messages get no headers.

## Deploy

- Terraform root: `terraform/020-app`. Apply `terraform/010-workload` first, because it creates Mosquitto, Redpanda, and the Secret.
- Module inputs: Secret `bridge-env`.
- Pod: Deployment with 1 replica and the `Recreate` strategy, so the old pod stops before the new pod starts.
- Pipeline: the ConfigMap `bridge-pipeline` holds `frigate-to-redpanda.yaml`. The pod has a hash of this file as an annotation. After a change to the file, `terraform apply` restarts the pod.
- Probes: liveness `GET /ping` and readiness `GET /ready`, on port 4195.
- Reads from: `mosquitto.hsec.svc.cluster.local:1883`, as the user `rpconnect`, with QoS 1 and a fixed client ID.
- Writes to: `redpanda-0.redpanda.hsec.svc.cluster.local:9093`, one message at a time.
- Resources: memory 128 MiB, CPU request 0.05.

## Files

- `connect/frigate-to-redpanda.yaml`: the pipeline.
- `connect/tests/`: the test cases for the `route` processor.
- `terraform/`: the Terraform module and its tests.

## Test

- Run the pipeline tests with `rpk connect test`. They cover the 9 cases in the specs.
- In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`.

## Details

See [Redpanda Connect bridge](../docs/specs.md#redpanda-connect-bridge) and the bridge test cases in [Testing](../docs/specs.md#testing) in the specs.
