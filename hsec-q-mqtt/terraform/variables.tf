variable "namespace" {
  description = "Namespace for Mosquitto."
  type        = string
}

variable "labels" {
  description = "Common labels for every resource."
  type        = map(string)
  default     = {}
}

variable "ssd_storage_class" {
  description = "Storage class for the saved message queue."
  type        = string
}

variable "env_secret_name" {
  description = "Secret with MQTT_FRIGATE_PASSWORD and MQTT_BRIDGE_PASSWORD."
  type        = string
}
