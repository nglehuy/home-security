# hsec-conn-discord

The notifier is a Redpanda Connect 4.112.0 pipeline. It reads the topic `hsec.alerts` from Redpanda (`hsec-q-redpanda`) and posts each message to Discord with a webhook. First comes the alert text with the snapshot image, then the clip parts of the review.

## Deploy

- Terraform root: `terraform/020-app`. Apply `terraform/010-workload` first, because it creates Redpanda and the Secret.
- Module inputs: time zone, Secret `notifier-env`.
- Webhook: you give the URL to `terraform/010-workload` as the sensitive variable `discord_webhook_url`. It goes into `notifier-env` as `DISCORD_WEBHOOK_URL`.
- Pod: Deployment with 1 replica and the `Recreate` strategy, so the old pod stops before the new pod starts.
- Pipeline: the ConfigMap `notifier-pipeline` holds `alerts-to-discord.yaml`. The pod has a hash of this file as an annotation. After a change to the file, `terraform apply` restarts the pod.
- Probes: liveness `GET /ping` and readiness `GET /ready`, on port 4195.
- Reads from: topic `hsec.alerts`, consumer group `hsec-discord-notifier`.
- Writes to: the Discord webhook, at most 1 message every 2 seconds.
- Resources: memory 256 MiB, CPU request 0.05.

## Files

- `connect/alerts-to-discord.yaml`: the pipeline.
- `connect/tests/`: the test cases for the `to_discord` processor.
- `terraform/`: the Terraform module and its tests.

## Test

- Run the pipeline tests with `rpk connect test`. They cover the 5 cases in the specs.
- In `terraform/`, run `terraform fmt -check`, `terraform validate`, and `terraform test`.

## Details

See [Discord notifier](../docs/specs.md#discord-notifier) and [Alerts](../docs/specs.md#alerts) in the specs.
