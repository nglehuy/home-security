locals {
  name          = "notifier"
  image         = "docker.redpanda.com/redpandadata/connect:4.112.0"
  pipeline_file = "alerts-to-discord.yaml"
  pipeline      = file("${path.module}/../connect/${local.pipeline_file}")

  selector = { "app.kubernetes.io/name" = local.name }
  labels   = merge(var.labels, local.selector)
}

resource "kubernetes_config_map_v1" "pipeline" {
  metadata {
    name      = "notifier-pipeline"
    namespace = var.namespace
    labels    = local.labels
  }

  data = {
    (local.pipeline_file) = local.pipeline
  }
}

resource "kubernetes_deployment_v1" "notifier" {
  metadata {
    name      = local.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    replicas = 1

    # Two pods in the same consumer group can post an alert twice during a
    # rebalance, so the old pod stops before the new pod starts.
    strategy {
      type = "Recreate"
    }

    selector {
      match_labels = local.selector
    }

    template {
      metadata {
        labels = local.labels
        # A pipeline change changes only the ConfigMap. The new hash restarts the pod.
        annotations = {
          "checksum/pipeline" = sha256(local.pipeline)
        }
      }

      spec {
        container {
          name  = "connect"
          image = local.image
          args  = ["run", "/config/${local.pipeline_file}"]

          # The alert text shows the local time.
          env {
            name  = "TZ"
            value = var.timezone
          }

          env {
            name = "NAMESPACE"
            value_from {
              field_ref {
                field_path = "metadata.namespace"
              }
            }
          }

          env_from {
            secret_ref {
              name = var.env_secret_name
            }
          }

          port {
            name           = "http"
            container_port = 4195
          }

          resources {
            requests = {
              cpu    = "50m"
              memory = "256Mi"
            }
            limits = {
              memory = "256Mi"
            }
          }

          liveness_probe {
            http_get {
              path = "/ping"
              port = 4195
            }
            period_seconds = 30
          }

          # /ready answers 200 after the input and the output connect.
          readiness_probe {
            http_get {
              path = "/ready"
              port = 4195
            }
            period_seconds = 10
          }

          volume_mount {
            name       = "pipeline"
            mount_path = "/config"
            read_only  = true
          }
        }

        volume {
          name = "pipeline"
          config_map {
            name = kubernetes_config_map_v1.pipeline.metadata[0].name
          }
        }
      }
    }
  }
}
