terraform {
  required_version = "~> 1.16"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 3.3"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.3"
    }
  }

  # The state holds the RustFS keys that the Flink module reads from a Secret.
  # Only cluster admins can read Secrets in kube-system.
  backend "kubernetes" {
    secret_suffix = "hsec-020-app"
    namespace     = "kube-system"
    config_path   = "~/.kube/config"
  }
}
