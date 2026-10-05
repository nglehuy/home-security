locals {
  name  = "clickhouse"
  image = "clickhouse/clickhouse-server:26.3.39.7"

  selector = { "app.kubernetes.io/name" = local.name }
  labels   = merge(var.labels, local.selector)

  server_config = templatefile("${path.module}/../config.d/hsec.xml.tftpl", {
    timezone = var.timezone
  })
  # The users file holds password hashes, so it goes into a Secret.
  users_config = templatefile("${path.module}/../users.d/hsec.xml.tftpl", {
    admin_sha256   = sha256(var.admin_password)
    spark_sha256   = sha256(var.spark_password)
    grafana_sha256 = sha256(var.grafana_password)
  })
}

resource "kubernetes_config_map_v1" "server" {
  metadata {
    name      = "clickhouse-server-config"
    namespace = var.namespace
    labels    = local.labels
  }

  data = {
    "hsec.xml" = local.server_config
  }
}

resource "kubernetes_secret_v1" "users" {
  metadata {
    name      = "clickhouse-users"
    namespace = var.namespace
    labels    = local.labels
  }

  data = {
    "hsec.xml" = local.users_config
  }
}

resource "kubernetes_persistent_volume_claim_v1" "data" {
  metadata {
    name      = "clickhouse-data"
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.hdd_storage_class

    resources {
      requests = {
        storage = "5Gi"
      }
    }
  }

  # Local volumes bind only when the first pod starts.
  wait_until_bound = false
}

resource "kubernetes_service_v1" "clickhouse" {
  metadata {
    name      = local.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    type     = "ClusterIP"
    selector = local.selector

    port {
      name        = "http"
      port        = 8123
      target_port = 8123
    }

    port {
      name        = "native"
      port        = 9000
      target_port = 9000
    }
  }
}

# No ClickHouse Keeper, because no table uses replication.
resource "kubernetes_stateful_set_v1" "clickhouse" {
  metadata {
    name      = local.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    replicas     = 1
    service_name = kubernetes_service_v1.clickhouse.metadata[0].name

    selector {
      match_labels = local.selector
    }

    template {
      metadata {
        labels = local.labels
        # A change of the server or user files restarts the pod.
        annotations = {
          "checksum/config" = sha256(join("", [local.server_config, local.users_config]))
        }
      }

      spec {
        container {
          name  = "clickhouse"
          image = local.image

          # At the first start, the image creates the database hsec. The migrations connect to it.
          env {
            name  = "CLICKHOUSE_DB"
            value = "hsec"
          }

          port {
            name           = "http"
            container_port = 8123
          }

          port {
            name           = "native"
            container_port = 9000
          }

          resources {
            requests = {
              cpu    = "250m"
              memory = "1536Mi"
            }
            limits = {
              memory = "1536Mi"
            }
          }

          readiness_probe {
            http_get {
              path = "/ping"
              port = 8123
            }
            period_seconds = 10
          }

          liveness_probe {
            http_get {
              path = "/ping"
              port = 8123
            }
            period_seconds    = 30
            failure_threshold = 5
          }

          volume_mount {
            name       = "data"
            mount_path = "/var/lib/clickhouse"
          }

          # Single files, so the files that the image ships in these folders stay.
          volume_mount {
            name       = "server"
            mount_path = "/etc/clickhouse-server/config.d/hsec.xml"
            sub_path   = "hsec.xml"
            read_only  = true
          }

          volume_mount {
            name       = "users"
            mount_path = "/etc/clickhouse-server/users.d/hsec.xml"
            sub_path   = "hsec.xml"
            read_only  = true
          }
        }

        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.data.metadata[0].name
          }
        }

        volume {
          name = "server"
          config_map {
            name = kubernetes_config_map_v1.server.metadata[0].name
          }
        }

        volume {
          name = "users"
          secret {
            secret_name = kubernetes_secret_v1.users.metadata[0].name
          }
        }
      }
    }
  }
}
