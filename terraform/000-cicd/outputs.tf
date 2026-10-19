# The kubeconfig of the deploy workflow. Save it as a GitHub secret without a
# file on disk. See README.md.
output "ci_kubeconfig" {
  description = "kubeconfig for the deploy workflow, with the ci-deployer token."
  sensitive   = true
  value = yamlencode({
    apiVersion = "v1"
    kind       = "Config"
    clusters = [{
      name = var.cluster_name
      cluster = {
        server = "https://${var.api_host}:6443"
        # The k3s API certificate does not list the MagicDNS name, but it
        # always lists "kubernetes".
        tls-server-name            = "kubernetes"
        certificate-authority-data = base64encode(kubernetes_secret_v1.ci_token.data["ca.crt"])
      }
    }]
    users = [{
      name = local.name
      user = {
        token = kubernetes_secret_v1.ci_token.data["token"]
      }
    }]
    contexts = [{
      name = var.cluster_name
      context = {
        cluster = var.cluster_name
        user    = local.name
      }
    }]
    current-context = var.cluster_name
  })
}
