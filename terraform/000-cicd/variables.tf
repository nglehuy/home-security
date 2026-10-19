variable "kubeconfig_path" {
  description = "Path of the kubeconfig file."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "Context in the kubeconfig file."
  type        = string
}

variable "cluster_name" {
  description = "Name of the cluster and of the context in the CI kubeconfig, for example pers-infra."
  type        = string
}

variable "api_host" {
  description = "MagicDNS name of the k3s node on the tailnet, for example vn-exit.tail1234.ts.net. The CI kubeconfig reaches the API at https://<api_host>:6443."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]+\\.[a-z0-9-]+\\.ts\\.net$", var.api_host))
    error_message = "api_host must be the MagicDNS name of the node, for example vn-exit.tail1234.ts.net."
  }
}
