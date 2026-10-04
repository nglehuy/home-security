locals {
  image   = "docker.redpanda.com/redpandadata/redpanda:v26.2.3"
  brokers = "redpanda-0.redpanda.${var.namespace}.svc.cluster.local:9093"
  script  = file("${path.module}/../topics.sh")

  topics_labels = merge(var.labels, { "app.kubernetes.io/name" = "redpanda-topics" })

  # The chart defaults are for a three-broker production cluster.
  values = {
    statefulset = {
      replicas = 1
    }
    console = {
      enabled = false
    }
    tls = {
      enabled = false
    }
    external = {
      enabled = false
    }
    resources = {
      cpu = {
        cores = 1
      }
      memory = {
        container = {
          max = "2.5Gi"
        }
      }
    }
    storage = {
      persistentVolume = {
        size         = "10Gi"
        storageClass = var.ssd_storage_class
      }
    }
    config = {
      cluster = {
        auto_create_topics_enabled = false
      }
    }
    commonLabels = var.labels
  }
}

# Normal mode with fsync on. --mode dev-container would turn fsync off.
resource "helm_release" "redpanda" {
  name       = "redpanda"
  namespace  = var.namespace
  repository = "https://charts.redpanda.com"
  chart      = "redpanda"
  version    = "26.2.4"
  values     = [yamlencode(local.values)]
  wait       = true
  timeout    = 600
}

resource "kubernetes_config_map_v1" "topics" {
  metadata {
    name      = "redpanda-topics"
    namespace = var.namespace
    labels    = local.topics_labels
  }

  data = {
    "topics.sh" = local.script
  }
}

# Creates the topics. The script is safe to run again. A new script hash
# replaces the Job, so a changed topic list runs again.
resource "kubernetes_job_v1" "topics" {
  metadata {
    name      = "redpanda-topics-${substr(sha256(local.script), 0, 8)}"
    namespace = var.namespace
    labels    = local.topics_labels
  }

  spec {
    backoff_limit = 3

    template {
      metadata {
        labels = local.topics_labels
      }

      spec {
        restart_policy = "Never"

        container {
          name    = "topics"
          image   = local.image
          command = ["sh", "/scripts/topics.sh"]

          env {
            name  = "BROKERS"
            value = local.brokers
          }

          resources {
            requests = {
              cpu    = "50m"
              memory = "128Mi"
            }
            limits = {
              memory = "128Mi"
            }
          }

          volume_mount {
            name       = "scripts"
            mount_path = "/scripts"
            read_only  = true
          }
        }

        volume {
          name = "scripts"
          config_map {
            name = kubernetes_config_map_v1.topics.metadata[0].name
          }
        }
      }
    }
  }

  wait_for_completion = true

  timeouts {
    create = "10m"
    update = "10m"
  }

  depends_on = [helm_release.redpanda]
}
