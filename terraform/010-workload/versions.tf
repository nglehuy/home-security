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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.9"
    }
  }

  # The state holds all passwords in plain text. Only cluster admins can read
  # Secrets in kube-system. Use -backend-config to change the kubeconfig.
  backend "kubernetes" {
    secret_suffix = "hsec-010-workload"
    namespace     = "kube-system"
    config_path   = "~/.kube/config"
  }
}
