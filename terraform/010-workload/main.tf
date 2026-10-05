locals {
  labels = { "app.kubernetes.io/part-of" = "hsec" }
}

resource "kubernetes_namespace_v1" "hsec" {
  metadata {
    name   = var.namespace
    labels = local.labels
  }
}

# The data services. The apps in terraform/020-app use them.

module "rustfs" {
  source = "../../hsec-db-rustfs/terraform"

  namespace         = kubernetes_namespace_v1.hsec.metadata[0].name
  labels            = local.labels
  hdd_storage_class = var.hdd_storage_class
  root_secret_name  = kubernetes_secret_v1.this["rustfs-root"].metadata[0].name
}

module "redpanda" {
  source = "../../hsec-q-redpanda/terraform"

  namespace         = kubernetes_namespace_v1.hsec.metadata[0].name
  labels            = local.labels
  ssd_storage_class = var.ssd_storage_class
}

module "mqtt" {
  source = "../../hsec-q-mqtt/terraform"

  namespace         = kubernetes_namespace_v1.hsec.metadata[0].name
  labels            = local.labels
  ssd_storage_class = var.ssd_storage_class
  env_secret_name   = kubernetes_secret_v1.this["mosquitto-env"].metadata[0].name
}

module "clickhouse" {
  source = "../../hsec-db-clickhouse/terraform"

  namespace         = kubernetes_namespace_v1.hsec.metadata[0].name
  labels            = local.labels
  hdd_storage_class = var.hdd_storage_class
  timezone          = var.timezone
  admin_password    = random_password.this["clickhouse_admin"].result
  spark_password    = random_password.this["clickhouse_spark"].result
  grafana_password  = random_password.this["clickhouse_grafana"].result
}

module "iceberg" {
  source = "../../hsec-db-iceberg/terraform"

  namespace            = kubernetes_namespace_v1.hsec.metadata[0].name
  labels               = local.labels
  ssd_storage_class    = var.ssd_storage_class
  password_secret_name = kubernetes_secret_v1.this["postgres-iceberg"].metadata[0].name
}
