variable "namespace" {
  description = "Namespace for Frigate."
  type        = string
}

variable "labels" {
  description = "Common labels for every resource."
  type        = map(string)
  default     = {}
}

variable "ssd_storage_class" {
  description = "Storage class for the configuration volume."
  type        = string
}

variable "hdd_storage_class" {
  description = "Storage class for the media volume."
  type        = string
}

variable "timezone" {
  description = "Local time zone, for example Asia/Singapore."
  type        = string
}

variable "env_secret_name" {
  description = "Secret with FRIGATE_RTSP_PASSWORD, FRIGATE_MQTT_USER, and FRIGATE_MQTT_PASSWORD."
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

  validation {
    condition     = length(var.cameras) > 0
    error_message = "Give at least one camera."
  }

  validation {
    condition     = alltrue([for name, cam in var.cameras : can(regex("^[a-zA-Z0-9_-]+$", name))])
    error_message = "A camera name can use only letters, digits, underscores, and dashes."
  }

  validation {
    condition     = alltrue([for cam in values(var.cameras) : cam.detect_width > 0 && cam.detect_height > 0])
    error_message = "detect_width and detect_height must be greater than 0."
  }
}
