locals {
  name     = "postgres"
  image    = "postgres:18.6-alpine"
  database = "iceberg_catalog"
  user     = "iceberg"

  selector = { "app.kubernetes.io/name" = local.name }
  labels   = merge(var.labels, local.selector)
}

resource "kubernetes_persistent_volume_claim_v1" "data" {
  metadata {
    name      = "postgres-data"
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.ssd_storage_class

    resources {
      requests = {
        storage = "2Gi"
      }
    }
  }

  # Local volumes bind only when the first pod starts.
  wait_until_bound = false
}

resource "kubernetes_service_v1" "postgres" {
  metadata {
    name      = local.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    type     = "ClusterIP"
    selector = local.selector

    port {
      name        = "postgres"
      port        = 5432
      target_port = 5432
    }
  }
}

# Holds only the Iceberg JDBC catalog. The catalog creates its own tables on first use.
resource "kubernetes_stateful_set_v1" "postgres" {
  metadata {
    name      = local.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    replicas     = 1
    service_name = kubernetes_service_v1.postgres.metadata[0].name

    selector {
      match_labels = local.selector
    }

    template {
      metadata {
        labels = local.labels
      }

      spec {
        container {
          name  = "postgres"
          image = local.image

          env {
            name  = "POSTGRES_DB"
            value = local.database
          }

          env {
            name  = "POSTGRES_USER"
            value = local.user
          }

          env {
            name = "POSTGRES_PASSWORD"
            value_from {
              secret_key_ref {
                name = var.password_secret_name
                key  = "POSTGRES_PASSWORD"
              }
            }
          }

          port {
            name           = "postgres"
            container_port = 5432
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "512Mi"
            }
            limits = {
              memory = "512Mi"
            }
          }

          readiness_probe {
            exec {
              command = ["pg_isready", "-U", local.user, "-d", local.database]
            }
            period_seconds = 10
          }

          liveness_probe {
            exec {
              command = ["pg_isready", "-U", local.user, "-d", local.database]
            }
            period_seconds    = 30
            failure_threshold = 5
          }

          # Postgres 18 images keep the data under this path, not under .../data.
          volume_mount {
            name       = "data"
            mount_path = "/var/lib/postgresql"
          }
        }

        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.data.metadata[0].name
          }
        }
      }
    }
  }
}
