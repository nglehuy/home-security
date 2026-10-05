variable "namespace" {
  description = "Namespace for the operator and the job."
  type        = string
}

variable "labels" {
  description = "Common labels for every resource."
  type        = map(string)
  default     = {}
}

variable "image" {
  description = "Image of the job, for example registry.lan/hsec-app-spark:1.0.0."
  type        = string
}

variable "timezone" {
  description = "Local time zone of the schedule and of the stats."
  type        = string
}

variable "night_start" {
  description = "Start of the night window, as HH:MM local time."
  type        = string
  default     = "23:00"

  validation {
    condition     = can(regex("^([01][0-9]|2[0-3]):[0-5][0-9]$", var.night_start))
    error_message = "night_start must be HH:MM, for example 23:00."
  }
}

variable "night_end" {
  description = "End of the night window, as HH:MM local time."
  type        = string
  default     = "06:00"

  validation {
    condition     = can(regex("^([01][0-9]|2[0-3]):[0-5][0-9]$", var.night_end))
    error_message = "night_end must be HH:MM, for example 06:00."
  }
}

variable "env_secret_name" {
  description = "Secret with POSTGRES_ICEBERG_PASSWORD, RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, and CLICKHOUSE_SPARK_PASSWORD."
  type        = string
}
