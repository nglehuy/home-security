locals {
  endpoint = "http://rustfs-svc.${var.namespace}.svc.cluster.local:9000"
  # Iceberg data files, and the Flink checkpoints, savepoints, and recovery data.
  buckets = ["hsec-lake", "hsec-flink"]

  buckets_script = <<-EOT
    set -eu
    for B in ${join(" ", local.buckets)}; do
      aws --endpoint-url "$S3_ENDPOINT" s3api head-bucket --bucket "$B" 2>/dev/null \
        || aws --endpoint-url "$S3_ENDPOINT" s3api create-bucket --bucket "$B"
    done
  EOT

  job_labels = merge(var.labels, { "app.kubernetes.io/name" = "rustfs-buckets" })
}

# The chart default is a distributed cluster with 4 pods.
resource "helm_release" "rustfs" {
  name       = "rustfs"
  namespace  = var.namespace
  repository = "https://charts.rustfs.com"
  chart      = "rustfs"
  version    = "1.0.1"
  wait       = true
  timeout    = 600

  values = [yamlencode({
    mode = {
      standalone = {
        enabled = true
      }
      distributed = {
        enabled = false
      }
    }
    replicaCount = 1
    storageclass = {
      name            = var.hdd_storage_class
      dataStorageSize = "400Gi"
      logStorageSize  = "1Gi"
    }
    secret = {
      existingSecret = var.root_secret_name
    }
    resources = {
      requests = {
        cpu    = "100m"
        memory = "512Mi"
      }
      limits = {
        memory = "512Mi"
      }
    }
    # The S3 API and the console stay inside the cluster.
    ingress = {
      enabled = false
    }
    commonLabels = var.labels
  })]
}

# Creates the buckets. A second run changes nothing.
resource "kubernetes_job_v1" "buckets" {
  metadata {
    name      = "rustfs-buckets-${substr(sha256(local.buckets_script), 0, 8)}"
    namespace = var.namespace
    labels    = local.job_labels
  }

  spec {
    backoff_limit = 5

    template {
      metadata {
        labels = local.job_labels
      }

      spec {
        restart_policy = "Never"

        container {
          name    = "buckets"
          image   = "amazon/aws-cli:2.34.62"
          command = ["sh", "-c", local.buckets_script]

          env {
            name  = "S3_ENDPOINT"
            value = local.endpoint
          }

          env {
            name  = "AWS_DEFAULT_REGION"
            value = "us-east-1"
          }

          env {
            name = "AWS_ACCESS_KEY_ID"
            value_from {
              secret_key_ref {
                name = var.root_secret_name
                key  = "RUSTFS_ACCESS_KEY"
              }
            }
          }

          env {
            name = "AWS_SECRET_ACCESS_KEY"
            value_from {
              secret_key_ref {
                name = var.root_secret_name
                key  = "RUSTFS_SECRET_KEY"
              }
            }
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
        }
      }
    }
  }

  wait_for_completion = true

  timeouts {
    create = "10m"
    update = "10m"
  }

  depends_on = [helm_release.rustfs]
}
