variable "namespace" {
  description = "Namespace for the notifier."
  type        = string
}

variable "labels" {
  description = "Common labels for every resource."
  type        = map(string)
  default     = {}
}

variable "timezone" {
  description = "Local time zone for the alert text, for example Asia/Singapore."
  type        = string
}

variable "env_secret_name" {
  description = "Secret with DISCORD_WEBHOOK_URL."
  type        = string
}
