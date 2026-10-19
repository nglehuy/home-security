# Checks of the CI identity and its kubeconfig. The runs use apply, so the
# output gets the mock token and CA.

mock_provider "kubernetes" {
  # Kubernetes fills in these keys of a service account token Secret.
  override_resource {
    target = kubernetes_secret_v1.ci_token
    values = {
      data = {
        "ca.crt"  = "test-ca"
        namespace = "kube-system"
        token     = "test-token"
      }
    }
  }
}

variables {
  kube_context = "test"
  cluster_name = "pers-infra"
  api_host     = "vn-exit.tail1234.ts.net"
}

run "ci_identity" {
  command = apply

  assert {
    condition = (
      kubernetes_service_account_v1.ci.metadata[0].name == "ci-deployer" &&
      kubernetes_service_account_v1.ci.metadata[0].namespace == "kube-system"
    )
    error_message = "The CI identity must be the ServiceAccount ci-deployer in kube-system."
  }

  assert {
    condition = (
      kubernetes_cluster_role_binding_v1.ci.role_ref[0].kind == "ClusterRole" &&
      kubernetes_cluster_role_binding_v1.ci.role_ref[0].name == "cluster-admin" &&
      length(kubernetes_cluster_role_binding_v1.ci.subject) == 1 &&
      kubernetes_cluster_role_binding_v1.ci.subject[0].kind == "ServiceAccount" &&
      kubernetes_cluster_role_binding_v1.ci.subject[0].name == "ci-deployer" &&
      kubernetes_cluster_role_binding_v1.ci.subject[0].namespace == "kube-system"
    )
    error_message = "cluster-admin must be bound to ci-deployer only."
  }

  assert {
    condition = (
      kubernetes_secret_v1.ci_token.type == "kubernetes.io/service-account-token" &&
      kubernetes_secret_v1.ci_token.metadata[0].namespace == "kube-system" &&
      kubernetes_secret_v1.ci_token.metadata[0].annotations["kubernetes.io/service-account.name"] == "ci-deployer" &&
      kubernetes_secret_v1.ci_token.wait_for_service_account_token == true
    )
    error_message = "The token Secret must be a service account token of ci-deployer, and Terraform must wait until Kubernetes fills it in."
  }
}

run "ci_kubeconfig" {
  command = apply

  assert {
    condition     = yamldecode(nonsensitive(output.ci_kubeconfig)).clusters[0].cluster.server == "https://vn-exit.tail1234.ts.net:6443"
    error_message = "The API server must be api_host on port 6443."
  }

  assert {
    condition     = yamldecode(nonsensitive(output.ci_kubeconfig)).clusters[0].cluster["tls-server-name"] == "kubernetes"
    error_message = "The kubeconfig must check the certificate against the name kubernetes, because the k3s certificate does not list the MagicDNS name."
  }

  assert {
    condition     = yamldecode(nonsensitive(output.ci_kubeconfig)).clusters[0].cluster["certificate-authority-data"] == base64encode("test-ca")
    error_message = "certificate-authority-data must be the base64 of ca.crt from the token Secret."
  }

  assert {
    condition     = yamldecode(nonsensitive(output.ci_kubeconfig)).users[0].user.token == "test-token"
    error_message = "The user token must be the token from the token Secret, not base64 encoded."
  }

  assert {
    condition = (
      yamldecode(nonsensitive(output.ci_kubeconfig))["current-context"] == "pers-infra" &&
      yamldecode(nonsensitive(output.ci_kubeconfig)).contexts[0].name == "pers-infra" &&
      yamldecode(nonsensitive(output.ci_kubeconfig)).contexts[0].context.cluster == yamldecode(nonsensitive(output.ci_kubeconfig)).clusters[0].name &&
      yamldecode(nonsensitive(output.ci_kubeconfig)).contexts[0].context.user == yamldecode(nonsensitive(output.ci_kubeconfig)).users[0].name
    )
    error_message = "The current context must exist and point at the cluster and the user of the kubeconfig."
  }
}

run "custom_cluster_name" {
  command = apply

  variables {
    cluster_name = "home"
  }

  assert {
    condition = (
      yamldecode(nonsensitive(output.ci_kubeconfig)).clusters[0].name == "home" &&
      yamldecode(nonsensitive(output.ci_kubeconfig)).contexts[0].name == "home" &&
      yamldecode(nonsensitive(output.ci_kubeconfig)).contexts[0].context.cluster == "home" &&
      yamldecode(nonsensitive(output.ci_kubeconfig))["current-context"] == "home"
    )
    error_message = "The cluster, the context, and the current context must follow var.cluster_name."
  }
}

run "rejects_machine_name_only" {
  command = plan

  variables {
    api_host = "vn-exit"
  }

  expect_failures = [var.api_host]
}

run "rejects_url" {
  command = plan

  variables {
    api_host = "https://vn-exit.tail1234.ts.net"
  }

  expect_failures = [var.api_host]
}

run "rejects_other_domain" {
  command = plan

  variables {
    api_host = "k3s.example.com"
  }

  expect_failures = [var.api_host]
}
