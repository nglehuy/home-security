mock_provider "kubernetes" {}

variables {
  namespace         = "hsec"
  labels            = { "app.kubernetes.io/part-of" = "hsec" }
  hdd_storage_class = "hdd"
  timezone          = "Asia/Singapore"
  admin_password    = "adminpw"
  spark_password    = "sparkpw"
  grafana_password  = "grafanapw"
}

run "server_and_users" {
  command = plan

  assert {
    condition     = strcontains(kubernetes_config_map_v1.server.data["hsec.xml"], "<timezone>Asia/Singapore</timezone>")
    error_message = "The server must use the given time zone."
  }

  assert {
    condition     = strcontains(kubernetes_secret_v1.users.data["hsec.xml"], "<password_sha256_hex>${sha256("sparkpw")}</password_sha256_hex>")
    error_message = "spark_writer must have the SHA-256 of its password."
  }

  assert {
    condition     = !strcontains(kubernetes_secret_v1.users.data["hsec.xml"], "sparkpw") && !strcontains(kubernetes_secret_v1.users.data["hsec.xml"], "adminpw")
    error_message = "The users file must hold hashes, never the passwords."
  }

  assert {
    condition     = strcontains(kubernetes_secret_v1.users.data["hsec.xml"], "GRANT SELECT ON system.clusters") && strcontains(kubernetes_secret_v1.users.data["hsec.xml"], "GRANT SELECT ON system.macros")
    error_message = "The Spark connector needs to read system.clusters and system.macros."
  }
}

run "pod" {
  command = plan

  assert {
    condition     = kubernetes_stateful_set_v1.clickhouse.spec[0].template[0].spec[0].container[0].image == "clickhouse/clickhouse-server:26.3.39.7"
    error_message = "ClickHouse must be 26.3.39.7."
  }

  assert {
    condition     = [for e in kubernetes_stateful_set_v1.clickhouse.spec[0].template[0].spec[0].container[0].env : e.value if e.name == "CLICKHOUSE_DB"] == ["hsec"]
    error_message = "The image must create the database hsec for the migrations."
  }

  assert {
    condition     = kubernetes_stateful_set_v1.clickhouse.spec[0].template[0].spec[0].container[0].resources[0].limits["memory"] == "1536Mi"
    error_message = "ClickHouse must get 1.5 GiB."
  }

  assert {
    condition     = length(kubernetes_stateful_set_v1.clickhouse.spec[0].template[0].metadata[0].annotations["checksum/config"]) == 64
    error_message = "A change of the configuration must restart the pod."
  }

  assert {
    condition     = kubernetes_persistent_volume_claim_v1.data.spec[0].storage_class_name == "hdd" && kubernetes_persistent_volume_claim_v1.data.spec[0].resources[0].requests.storage == "5Gi"
    error_message = "The volume must be 5 GiB on the HDD class."
  }

  assert {
    condition     = [for p in kubernetes_service_v1.clickhouse.spec[0].port : p.port] == [8123, 9000]
    error_message = "The Service must offer HTTP on 8123 and the native protocol on 9000."
  }
}
