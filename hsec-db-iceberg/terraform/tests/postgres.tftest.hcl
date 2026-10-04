mock_provider "kubernetes" {}

variables {
  namespace            = "hsec"
  labels               = { "app.kubernetes.io/part-of" = "hsec" }
  ssd_storage_class    = "ssd"
  password_secret_name = "postgres-iceberg"
}

run "postgres" {
  command = plan

  assert {
    condition     = kubernetes_stateful_set_v1.postgres.spec[0].template[0].spec[0].container[0].image == "postgres:18.6-alpine"
    error_message = "Postgres must be 18.6."
  }

  assert {
    condition     = { for e in kubernetes_stateful_set_v1.postgres.spec[0].template[0].spec[0].container[0].env : e.name => e.value if e.value != null && e.value != "" } == { POSTGRES_DB = "iceberg_catalog", POSTGRES_USER = "iceberg" }
    error_message = "The database must be iceberg_catalog with the user iceberg."
  }

  assert {
    condition     = [for e in kubernetes_stateful_set_v1.postgres.spec[0].template[0].spec[0].container[0].env : e.value_from[0].secret_key_ref[0].name if e.name == "POSTGRES_PASSWORD"] == ["postgres-iceberg"]
    error_message = "The password must come from the given Secret."
  }

  assert {
    condition     = kubernetes_stateful_set_v1.postgres.spec[0].template[0].spec[0].container[0].volume_mount[0].mount_path == "/var/lib/postgresql"
    error_message = "Postgres 18 keeps its data under /var/lib/postgresql."
  }

  assert {
    condition     = kubernetes_persistent_volume_claim_v1.data.metadata[0].name == "postgres-data" && kubernetes_persistent_volume_claim_v1.data.spec[0].storage_class_name == "ssd" && kubernetes_persistent_volume_claim_v1.data.spec[0].resources[0].requests.storage == "2Gi"
    error_message = "The volume must be postgres-data, 2 GiB on the SSD class."
  }

  assert {
    condition     = kubernetes_service_v1.postgres.metadata[0].name == "postgres" && kubernetes_service_v1.postgres.spec[0].port[0].port == 5432
    error_message = "Flink and Spark reach the catalog at postgres:5432."
  }
}
