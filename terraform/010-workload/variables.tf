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

variable "node_name" {
  description = "Name of the node with the Intel UHD 620, for the GPU plugin."
  type        = string
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
