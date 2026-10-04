# The Presto S3 file system for checkpoints reads its keys only from the Flink
# configuration, so the module reads them from the RustFS Secret.
data "kubernetes_secret_v1" "rustfs" {
  metadata {
    name      = var.rustfs_secret_name
    namespace = var.namespace
  }
}

resource "helm_release" "operator" {
  name       = "flink-kubernetes-operator"
  namespace  = var.namespace
  repository = "https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.16.1/"
  chart      = "flink-kubernetes-operator"
  version    = "1.16.1"
  wait       = true

  values = [yamlencode({
    watchNamespaces = [var.namespace]
    # The webhook needs cert-manager, which the cluster does not have.
    webhook = {
      create = false
    }
    operatorPod = {
      resources = {
        requests = {
          cpu    = "100m"
          memory = "512Mi"
        }
        limits = {
          memory = "512Mi"
        }
      }
    }
  })]
}

# Helm checks the FlinkDeployment only when it installs it, after the operator
# release created the CRDs. A kubernetes_manifest would need the CRDs at plan time.
resource "helm_release" "job" {
  name      = "hsec-stream"
  namespace = var.namespace
  chart     = "${path.module}/chart"
  wait      = false

  values = [yamlencode({
    image      = var.image
    timezone   = var.timezone
    frigateUrl = var.frigate_url
    envSecret  = var.env_secret_name
    labels     = var.labels
  })]

  set_sensitive = [
    {
      name  = "s3AccessKey"
      value = data.kubernetes_secret_v1.rustfs.data["RUSTFS_ACCESS_KEY"]
    },
    {
      name  = "s3SecretKey"
      value = data.kubernetes_secret_v1.rustfs.data["RUSTFS_SECRET_KEY"]
    },
  ]

  depends_on = [helm_release.operator]
}
