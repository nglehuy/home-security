# hsec-q-mqtt

Mosquitto 2.1.2 is the MQTT broker. Frigate (`hsec-app-frigate`) publishes to it, and the bridge (`hsec-conn-mqtt`) reads from it. Mosquitto queues the messages while the bridge is offline.

## Deploy

- Terraform root: `terraform/010-workload`.
- Module inputs: SSD class, Secret `mosquitto-env`.
- Service: `mosquitto`, type `ClusterIP`, port 1883.
- Volume: PVC `mosquitto-data` (SSD, 1 GiB) for the saved queue.
- Address: `mosquitto.hsec.svc.cluster.local:1883`.
- Users: `frigate` can read and write `frigate/#`. `rpconnect` can only read the general Frigate topics.
- Resources: memory 64 MiB, CPU request 0.05.

## Files

- `mosquitto.conf`: the broker configuration. Each client must log in. Mosquitto saves the queue every 60 seconds and keeps up to 10000 queued messages.
- `acl`: the topics that each user can read or write.
- `terraform/`: the Terraform module and its tests.

## Test

In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`. The smoke test `tests/smoke-test.sh` stops the bridge and publishes 100 messages. Then it makes sure that all 100 messages arrive in Redpanda.

## Details

See [Mosquitto](../docs/specs.md#mosquitto) in the specs.
