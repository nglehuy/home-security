resource "helm_release" "operator" {
  name       = "spark-operator"
  namespace  = var.namespace
  repository = "https://kubeflow.github.io/spark-operator"
  chart      = "spark-operator"
  version    = "2.5.2"
  wait       = true

  values = [yamlencode({
    spark = {
      jobNamespaces = [var.namespace]
    }
    # The job uses envFrom, which needs the webhook.
    webhook = {
      enable = true
      resources = {
        requests = {
          cpu    = "50m"
          memory = "128Mi"
        }
        limits = {
          memory = "128Mi"
        }
      }
    }
    # The controller runs spark-submit, a JVM, inside its own pod. With 128 MiB
    # the pod is OOMKilled at the first submit.
    controller = {
      resources = {
        requests = {
          cpu    = "50m"
          memory = "512Mi"
        }
        limits = {
          memory = "512Mi"
        }
      }
    }
  })]
}

# Helm checks the ScheduledSparkApplication only when it installs it, after the
# operator release created the CRDs. A kubernetes_manifest would need the CRDs at plan time.
resource "helm_release" "job" {
  name      = "hsec-batch"
  namespace = var.namespace
  chart     = "${path.module}/chart"
  wait      = false

  values = [yamlencode({
    image          = var.image
    timezone       = var.timezone
    nightStart     = var.night_start
    nightEnd       = var.night_end
    envSecret      = var.env_secret_name
    serviceAccount = "spark-operator-spark"
    labels         = var.labels
  })]

  depends_on = [helm_release.operator]
}
