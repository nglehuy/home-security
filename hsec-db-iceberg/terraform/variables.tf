variable "namespace" {
  description = "Namespace for Postgres."
  type        = string
}

variable "labels" {
  description = "Common labels for every resource."
  type        = map(string)
  default     = {}
}

variable "ssd_storage_class" {
  description = "Storage class for the Postgres volume."
  type        = string
}

variable "password_secret_name" {
  description = "Secret with POSTGRES_PASSWORD for the user iceberg."
  type        = string
}
