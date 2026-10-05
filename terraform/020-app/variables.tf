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

variable "frigate_url" {
  description = "Base URL of the Frigate UI for alert links, for example https://192.168.1.17:8971."
  type        = string

  validation {
    condition     = can(regex("^https?://", var.frigate_url))
    error_message = "frigate_url must start with http:// or https://."
  }
}
