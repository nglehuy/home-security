variable "kubeconfig_path" {
  description = "Path of the kubeconfig file."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "Context in the kubeconfig file."
  type        = string
}

variable "namespace" {
  description = "Namespace for all apps."
  type        = string
  default     = "hsec"
}

variable "timezone" {
  description = "Local time zone, for example Asia/Singapore."
  type        = string
  default     = "Asia/Singapore"
}

variable "ssd_storage_class" {
  description = "Storage class on the SSD."
  type        = string
}

variable "hdd_storage_class" {
  description = "Storage class on the HDD, for RustFS and ClickHouse."
  type        = string
}

variable "domain" {
  description = "Local DNS domain of the web UIs. hsec.home.arpa uses the special-use name for home networks (RFC 8375). RustFS gets rustfs.<domain>."
  type        = string
  default     = "hsec.home.arpa"

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$", var.domain))
    error_message = "domain must be a DNS name in lowercase, without a scheme, a port, or a path."
  }
}

variable "grafana_namespace" {
  description = "Namespace of the Grafana pods. If set, a network policy lets them reach ClickHouse on port 8123."
  type        = string
  default     = null
}

variable "discord_webhook_url" {
  description = "Discord webhook URL of the alert channel."
  type        = string
  sensitive   = true

  validation {
    condition     = startswith(var.discord_webhook_url, "https://")
    error_message = "discord_webhook_url must start with https://."
  }
}

variable "frigate_rtsp_password" {
  description = "Camera password. Frigate puts it into the camera URLs."
  type        = string
  sensitive   = true
}
