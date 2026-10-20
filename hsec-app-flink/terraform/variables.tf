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
  description = "Image of the job, for example registry.lan/hsec-app-flink:1.0.0."
  type        = string
}

variable "timezone" {
  description = "Local time zone of the alert text."
  type        = string
}

variable "env_secret_name" {
  description = "Secret with POSTGRES_ICEBERG_PASSWORD, RUSTFS_ACCESS_KEY, and RUSTFS_SECRET_KEY."
  type        = string
}

variable "rustfs_secret_name" {
  description = "Secret of RustFS with RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY, for the checkpoint file system."
  type        = string
}

variable "ingress_host" {
  description = "Host name of the Flink web UI on Traefik, for example flink.hsec.home.arpa."
  type        = string
}
