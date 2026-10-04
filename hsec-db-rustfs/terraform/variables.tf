variable "namespace" {
  description = "Namespace for RustFS."
  type        = string
}

variable "labels" {
  description = "Common labels for every resource."
  type        = map(string)
  default     = {}
}

variable "hdd_storage_class" {
  description = "Storage class for the data and log volumes."
  type        = string
}

variable "root_secret_name" {
  description = "Secret with RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY."
  type        = string
}
