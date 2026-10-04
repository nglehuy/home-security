variable "namespace" {
  description = "Namespace for Redpanda."
  type        = string
}

variable "labels" {
  description = "Common labels for every resource."
  type        = map(string)
  default     = {}
}

variable "ssd_storage_class" {
  description = "Storage class for the Redpanda volume."
  type        = string
}
