# hsec-app-frigate

Frigate 0.18.0 reads the IP cameras and finds objects and faces on the Intel UHD 620. It keeps the video of alerts and detections for 14 days. It publishes its messages to Mosquitto (`hsec-q-mqtt`). The Flink job (`hsec-app-flink`) gets alert images and clips from its API.

## Deploy

- Terraform root: `terraform/020-app`. Apply `terraform/010-workload` first, because it creates the GPU plugin, the Secret, and Mosquitto.
- Module inputs: SSD and HDD classes, time zone, cameras, Secret `frigate-env`.
- Services: `frigate` (type `LoadBalancer`, ports 8971, 8554, 8555) and `frigate-api` (type `ClusterIP`, port 5000).
- Volumes: PVC `frigate-config` (SSD, 5 GiB) and PVC `frigate-media` (HDD, 200 GiB).
- Address: `https://192.168.1.17:8971`. It needs a Frigate login.
- Port 5000 has no login. A network policy allows only the Flink TaskManager to reach it.
- Resources: memory 3 GiB, CPU request 1, `gpu.intel.com/i915: 1`.

On the first start, Frigate downloads its models and prints the admin password in its log.

## Files

- `config.seed.yml.tftpl`: the starting Frigate configuration. Terraform renders it into a ConfigMap. If `/config/config.yml` does not exist, the init container copies the file there. So a later change to this file does not change an existing configuration.
- `terraform/`: the Terraform module.
- `terraform/tests/`: the tests for `terraform test`.

## Test

In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`. The tests use mock providers. They cover the rendered Frigate configuration, the Secret names, and the labels.

## Details

See [Frigate](../docs/specs.md#frigate) and [Network policies](../docs/specs.md#network-policies) in the specs.
