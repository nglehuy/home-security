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
  description = "Namespace for all apps. terraform/010-workload creates it."
  type        = string
  default     = "hsec"
}

variable "timezone" {
  description = "Local time zone, for example Asia/Singapore."
  type        = string
  default     = "Asia/Singapore"
}

variable "ssd_storage_class" {
  description = "Storage class on the SSD, for the Frigate configuration."
  type        = string
}

variable "hdd_storage_class" {
  description = "Storage class on the HDD, for the Frigate media."
  type        = string
}

variable "image_registry" {
  description = "Registry of the two custom images, for example registry.lan:5000."
  type        = string
}

variable "flink_image_tag" {
  description = "Tag of the hsec-app-flink image that you pushed."
  type        = string
}

variable "spark_image_tag" {
  description = "Tag of the hsec-app-spark image that you pushed."
  type        = string
}

variable "cameras" {
  description = "Cameras by name. The URLs can contain {FRIGATE_RTSP_PASSWORD}."
  type = map(object({
    detect_url    = string
    record_url    = string
    detect_width  = number
    detect_height = number
  }))
  # The camera URLs show the camera addresses and user names. Terraform hides
  # sensitive values in its output, which matters for the public CI logs.
  sensitive = true
}

variable "night_start" {
  description = "Start of the night window for the day and night stats, as HH:MM."
  type        = string
  default     = "23:00"
}

variable "night_end" {
  description = "End of the night window for the day and night stats, as HH:MM."
  type        = string
  default     = "06:00"
}

variable "domain" {
  description = "Local DNS domain of the web UIs. hsec.home.arpa uses the special-use name for home networks (RFC 8375). Frigate gets frigate.<domain>, and Flink gets flink.<domain>."
  type        = string
  default     = "hsec.home.arpa"

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$", var.domain))
    error_message = "domain must be a DNS name in lowercase, without a scheme, a port, or a path."
  }
}
