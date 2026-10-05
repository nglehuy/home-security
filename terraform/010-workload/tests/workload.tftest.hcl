mock_provider "kubernetes" {}
mock_provider "helm" {}
mock_provider "random" {}

variables {
  kube_context          = "test"
  node_name             = "vostro"
  ssd_storage_class     = "ssd"
  hdd_storage_class     = "hdd"
  discord_webhook_url   = "https://discord.com/api/webhooks/1/abc"
  frigate_rtsp_password = "camera"
}

run "secrets" {
  command = apply

  assert {
    condition = jsonencode({ for k, s in kubernetes_secret_v1.this : k => sort(keys(nonsensitive(s.data))) }) == jsonencode({
      "bridge-env"       = ["MQTT_BRIDGE_PASSWORD", "MQTT_BRIDGE_USER"]
      "clickhouse-admin" = ["CLICKHOUSE_ADMIN_PASSWORD"]
      "flink-env"        = ["POSTGRES_ICEBERG_PASSWORD", "RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY"]
      "frigate-env"      = ["FRIGATE_MQTT_PASSWORD", "FRIGATE_MQTT_USER", "FRIGATE_RTSP_PASSWORD"]
      "mosquitto-env"    = ["MQTT_BRIDGE_PASSWORD", "MQTT_FRIGATE_PASSWORD"]
      "notifier-env"     = ["DISCORD_WEBHOOK_URL"]
      "postgres-iceberg" = ["POSTGRES_PASSWORD"]
      "rustfs-root"      = ["RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY"]
      "spark-env"        = ["CLICKHOUSE_SPARK_PASSWORD", "POSTGRES_ICEBERG_PASSWORD", "RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY"]
    })
    error_message = "Each Secret must hold exactly the keys that its pods read."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["mosquitto-env"].data["MQTT_FRIGATE_PASSWORD"] == kubernetes_secret_v1.this["frigate-env"].data["FRIGATE_MQTT_PASSWORD"])
    error_message = "Frigate and Mosquitto must share the MQTT password of frigate."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["mosquitto-env"].data["MQTT_BRIDGE_PASSWORD"] == kubernetes_secret_v1.this["bridge-env"].data["MQTT_BRIDGE_PASSWORD"])
    error_message = "The bridge and Mosquitto must share the MQTT password of rpconnect."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["flink-env"].data["RUSTFS_SECRET_KEY"] == kubernetes_secret_v1.this["rustfs-root"].data["RUSTFS_SECRET_KEY"] && kubernetes_secret_v1.this["spark-env"].data["RUSTFS_ACCESS_KEY"] == kubernetes_secret_v1.this["rustfs-root"].data["RUSTFS_ACCESS_KEY"])
    error_message = "Flink and Spark must use the RustFS keys."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["spark-env"].data["POSTGRES_ICEBERG_PASSWORD"] == kubernetes_secret_v1.this["postgres-iceberg"].data["POSTGRES_PASSWORD"])
    error_message = "Spark must use the Postgres password."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["frigate-env"].data["FRIGATE_RTSP_PASSWORD"]) == "camera" && nonsensitive(kubernetes_secret_v1.this["notifier-env"].data["DISCORD_WEBHOOK_URL"]) == "https://discord.com/api/webhooks/1/abc"
    error_message = "The given secret values must go into their Secrets."
  }

  assert {
    condition     = alltrue([for p in random_password.this : p.special == false])
    error_message = "Passwords must use only letters and digits."
  }
}

run "network_policies" {
  command = plan

  assert {
    condition     = kubernetes_network_policy_v1.same_namespace.spec[0].pod_selector[0].match_expressions[0].operator == "NotIn" && kubernetes_network_policy_v1.same_namespace.spec[0].pod_selector[0].match_expressions[0].values == toset(["frigate"])
    error_message = "The same-namespace policy must not select Frigate."
  }

  assert {
    condition     = kubernetes_network_policy_v1.frigate.spec[0].ingress[1].from[0].pod_selector[0].match_labels == tomap({ app = "hsec-stream", component = "taskmanager" })
    error_message = "Only the Flink TaskManager may reach Frigate port 5000."
  }

  assert {
    condition     = [for p in kubernetes_network_policy_v1.frigate.spec[0].ingress[1].ports : p.port] == ["5000"] && length(kubernetes_network_policy_v1.frigate.spec[0].ingress[0].from) == 0
    error_message = "The LAN ports must be open to all, and port 5000 only to Flink."
  }

  assert {
    condition     = length(kubernetes_network_policy_v1.clickhouse_grafana) == 0
    error_message = "Without grafana_namespace, there is no Grafana rule."
  }
}

run "grafana_rule_when_set" {
  command = plan

  variables {
    grafana_namespace = "monitoring"
  }

  assert {
    condition     = kubernetes_network_policy_v1.clickhouse_grafana[0].spec[0].ingress[0].from[0].namespace_selector[0].match_labels == tomap({ "kubernetes.io/metadata.name" = "monitoring" })
    error_message = "Grafana pods must reach ClickHouse port 8123."
  }
}

run "gpu_plugin" {
  command = plan

  assert {
    condition     = kubernetes_daemon_set_v1.gpu_plugin.metadata[0].namespace == "intel-gpu-plugin" && kubernetes_daemon_set_v1.gpu_plugin.spec[0].template[0].spec[0].container[0].image == "intel/intel-gpu-plugin:0.37.1"
    error_message = "The GPU plugin must be 0.37.1 in its own namespace."
  }

  assert {
    condition     = kubernetes_daemon_set_v1.gpu_plugin.spec[0].template[0].spec[0].node_selector["kubernetes.io/hostname"] == "vostro"
    error_message = "The GPU plugin must run on the given node."
  }

  assert {
    condition     = kubernetes_daemon_set_v1.gpu_plugin.spec[0].template[0].spec[0].container[0].security_context[0].se_linux_options[0].type == "container_device_plugin_t"
    error_message = "The plugin needs the SELinux type for device plugins on Fedora."
  }
}

run "rejects_a_webhook_without_https" {
  command = plan

  variables {
    discord_webhook_url = "http://discord.com/api/webhooks/1/abc"
  }

  expect_failures = [var.discord_webhook_url]
}
