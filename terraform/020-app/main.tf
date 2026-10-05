locals {
  labels = { "app.kubernetes.io/part-of" = "hsec" }

  registry    = trimsuffix(var.image_registry, "/")
  flink_image = "${local.registry}/hsec-app-flink:${var.flink_image_tag}"
  spark_image = "${local.registry}/hsec-app-spark:${var.spark_image_tag}"
}

# terraform/010-workload creates the namespace, the Secrets, and the data
# services. The plan fails here if that root is not applied yet.
data "kubernetes_namespace_v1" "hsec" {
  metadata {
    name = var.namespace
  }
}

module "frigate" {
  source = "../../hsec-app-frigate/terraform"

  namespace         = data.kubernetes_namespace_v1.hsec.metadata[0].name
  labels            = local.labels
  ssd_storage_class = var.ssd_storage_class
  hdd_storage_class = var.hdd_storage_class
  timezone          = var.timezone
  cameras           = var.cameras
  env_secret_name   = "frigate-env"
}

module "conn_mqtt" {
  source = "../../hsec-conn-mqtt/terraform"

  namespace       = data.kubernetes_namespace_v1.hsec.metadata[0].name
  labels          = local.labels
  env_secret_name = "bridge-env"
}

module "conn_discord" {
  source = "../../hsec-conn-discord/terraform"

  namespace       = data.kubernetes_namespace_v1.hsec.metadata[0].name
  labels          = local.labels
  timezone        = var.timezone
  env_secret_name = "notifier-env"
}

module "flink" {
  source = "../../hsec-app-flink/terraform"

  namespace          = data.kubernetes_namespace_v1.hsec.metadata[0].name
  labels             = local.labels
  image              = local.flink_image
  timezone           = var.timezone
  frigate_url        = var.frigate_url
  env_secret_name    = "flink-env"
  rustfs_secret_name = "rustfs-root"
}

module "spark" {
  source = "../../hsec-app-spark/terraform"

  namespace       = data.kubernetes_namespace_v1.hsec.metadata[0].name
  labels          = local.labels
  image           = local.spark_image
  timezone        = var.timezone
  night_start     = var.night_start
  night_end       = var.night_end
  env_secret_name = "spark-env"
}
