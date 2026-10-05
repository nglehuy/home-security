variable "namespace" {
  description = "Namespace for ClickHouse."
  type        = string
}

variable "labels" {
  description = "Common labels for every resource."
  type        = map(string)
  default     = {}
}

variable "hdd_storage_class" {
  description = "Storage class for the ClickHouse volume."
  type        = string
}

variable "timezone" {
  description = "Server time zone. Local days and hours in the tables use it."
  type        = string
}

variable "admin_password" {
  description = "Password of hsec_admin, for the migrations."
  type        = string
  sensitive   = true
}

variable "spark_password" {
  description = "Password of spark_writer, for the Spark job."
  type        = string
  sensitive   = true
}

variable "grafana_password" {
  description = "Password of grafana_reader, for the Grafana data source."
  type        = string
  sensitive   = true
}
