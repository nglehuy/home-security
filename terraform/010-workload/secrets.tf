# Every internal password. Letters and digits only, so a password fits in a URL
# without escaping.
locals {
  password_lengths = {
    mqtt_frigate       = 32
    mqtt_bridge        = 32
    rustfs_access_key  = 20
    rustfs_secret_key  = 40
    postgres_iceberg   = 32
    clickhouse_admin   = 32
    clickhouse_spark   = 32
    clickhouse_grafana = 32
  }
}

resource "random_password" "this" {
  for_each = local.password_lengths

  length  = each.value
  special = false
}

locals {
  pw = { for k, v in random_password.this : k => v.result }

  # The names are a separate list, because for_each cannot use a map with secret values.
  secret_names = toset([
    "frigate-env", "mosquitto-env", "bridge-env", "notifier-env", "rustfs-root",
    "flink-env", "spark-env", "postgres-iceberg", "clickhouse-admin",
  ])

  secret_data = {
    "frigate-env" = {
      FRIGATE_RTSP_PASSWORD = var.frigate_rtsp_password
      FRIGATE_MQTT_USER     = "frigate"
      FRIGATE_MQTT_PASSWORD = local.pw.mqtt_frigate
    }
    "mosquitto-env" = {
      MQTT_FRIGATE_PASSWORD = local.pw.mqtt_frigate
      MQTT_BRIDGE_PASSWORD  = local.pw.mqtt_bridge
    }
    "bridge-env" = {
      MQTT_BRIDGE_USER     = "rpconnect"
      MQTT_BRIDGE_PASSWORD = local.pw.mqtt_bridge
    }
    "notifier-env" = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
    }
    # The key names that the RustFS chart expects.
    "rustfs-root" = {
      RUSTFS_ACCESS_KEY = local.pw.rustfs_access_key
      RUSTFS_SECRET_KEY = local.pw.rustfs_secret_key
    }
    "flink-env" = {
      POSTGRES_ICEBERG_PASSWORD = local.pw.postgres_iceberg
      RUSTFS_ACCESS_KEY         = local.pw.rustfs_access_key
      RUSTFS_SECRET_KEY         = local.pw.rustfs_secret_key
    }
    "spark-env" = {
      POSTGRES_ICEBERG_PASSWORD = local.pw.postgres_iceberg
      RUSTFS_ACCESS_KEY         = local.pw.rustfs_access_key
      RUSTFS_SECRET_KEY         = local.pw.rustfs_secret_key
      CLICKHOUSE_SPARK_PASSWORD = local.pw.clickhouse_spark
    }
    "postgres-iceberg" = {
      POSTGRES_PASSWORD = local.pw.postgres_iceberg
    }
    # No pod uses it. It is only for the ClickHouse migrations.
    "clickhouse-admin" = {
      CLICKHOUSE_ADMIN_PASSWORD = local.pw.clickhouse_admin
    }
  }
}

resource "kubernetes_secret_v1" "this" {
  for_each = local.secret_names

  metadata {
    name      = each.key
    namespace = kubernetes_namespace_v1.hsec.metadata[0].name
    labels    = local.labels
  }

  data = local.secret_data[each.key]
}
