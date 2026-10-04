variable "namespace" {
  description = "Namespace for the bridge."
  type        = string
}

variable "labels" {
  description = "Common labels for every resource."
  type        = map(string)
  default     = {}
}

variable "env_secret_name" {
  description = "Secret with MQTT_BRIDGE_USER and MQTT_BRIDGE_PASSWORD."
  type        = string
}
