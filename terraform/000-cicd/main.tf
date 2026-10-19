locals {
  labels    = { "app.kubernetes.io/part-of" = "hsec" }
  name      = "ci-deployer"
  namespace = "kube-system"
}

# The identity of the deploy workflow (.github/workflows/deploy.yml). The
# runner joins the tailnet and reaches the k3s API on port 6443.
resource "kubernetes_service_account_v1" "ci" {
  metadata {
    name      = local.name
    namespace = local.namespace
    labels    = local.labels
  }
}

# The workflow applies 010-workload and 020-app. They create namespaces, CRDs,
# ClusterRoles, and their state Secrets in kube-system, so the workflow needs
# cluster-admin.
resource "kubernetes_cluster_role_binding_v1" "ci" {
  metadata {
    name   = local.name
    labels = local.labels
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "cluster-admin"
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.ci.metadata[0].name
    namespace = kubernetes_service_account_v1.ci.metadata[0].namespace
  }
}

# A long-lived token. Kubernetes fills in the token and the cluster CA, and
# Terraform waits for them. To revoke the token and get a new one, run:
#   terraform apply -replace=kubernetes_secret_v1.ci_token
resource "kubernetes_secret_v1" "ci_token" {
  metadata {
    name      = "${local.name}-token"
    namespace = local.namespace
    labels    = local.labels
    annotations = {
      "kubernetes.io/service-account.name" = kubernetes_service_account_v1.ci.metadata[0].name
    }
  }

  type                           = "kubernetes.io/service-account-token"
  wait_for_service_account_token = true
}
