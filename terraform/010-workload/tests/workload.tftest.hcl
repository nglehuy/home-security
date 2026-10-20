mock_provider "kubernetes" {}
mock_provider "helm" {}
mock_provider "random" {}

variables {
  kube_context          = "test"
  ssd_storage_class     = "ssd"
  hdd_storage_class     = "hdd"
  discord_webhook_url   = "https://discord.com/api/webhooks/1/abc"
  frigate_rtsp_password = "camera"
}

run "migrations" {
  command = plan

  assert {
    condition     = sort(keys(kubernetes_config_map_v1.clickhouse_migrations.data)) == sort(tolist(fileset("../../hsec-db-clickhouse/migrations", "*.sql")))
    error_message = "The ClickHouse ConfigMap must hold every ClickHouse migration file."
  }

  assert {
    condition     = sort(keys(kubernetes_config_map_v1.iceberg_migrations.data)) == sort(tolist(fileset("../../hsec-db-iceberg/migrations", "*.up.sql")))
    error_message = "The Iceberg ConfigMap must hold every Iceberg .up.sql file."
  }

  assert {
    condition     = can(regex("^clickhouse-migrations-[0-9a-f]{8}$", kubernetes_job_v1.clickhouse_migrations.metadata[0].name)) && can(regex("^iceberg-migrations-[0-9a-f]{8}$", kubernetes_job_v1.iceberg_migrations.metadata[0].name))
    error_message = "Each Job name must end with a hash of its files."
  }

  assert {
    condition = kubernetes_job_v1.clickhouse_migrations.spec[0].template[0].spec[0].container[0].args == tolist([
      "-path", "/migrations",
      "-database", "clickhouse://clickhouse.hsec.svc.cluster.local:9000?username=hsec_admin&password=$(CLICKHOUSE_ADMIN_PASSWORD)&database=hsec&x-multi-statement=true",
      "up",
    ])
    error_message = "golang-migrate must run up as hsec_admin against the in-cluster ClickHouse."
  }

  assert {
    condition     = kubernetes_job_v1.clickhouse_migrations.spec[0].template[0].spec[0].container[0].image == "migrate/migrate:v4.20.1" && [for e in kubernetes_job_v1.clickhouse_migrations.spec[0].template[0].spec[0].container[0].env : e.value_from[0].secret_key_ref[0].name if e.name == "CLICKHOUSE_ADMIN_PASSWORD"] == ["clickhouse-admin"]
    error_message = "The ClickHouse Job must use golang-migrate 4.20.1 and the password from clickhouse-admin."
  }

  assert {
    condition     = [for v in kubernetes_job_v1.clickhouse_migrations.spec[0].template[0].spec[0].volume : v.host_path[0].path if v.name == "zoneinfo"] == ["/usr/share/zoneinfo"] && [for m in kubernetes_job_v1.clickhouse_migrations.spec[0].template[0].spec[0].container[0].volume_mount : [m.mount_path, m.read_only] if m.name == "zoneinfo"] == [["/usr/share/zoneinfo", true]]
    error_message = "The ClickHouse Job must get the time zone data of the node, read-only."
  }

  assert {
    condition     = kubernetes_job_v1.iceberg_migrations.spec[0].template[0].spec[0].container[0].image == "apache/spark:4.0.4-scala2.13-java17-ubuntu" && kubernetes_job_v1.iceberg_migrations.spec[0].template[0].spec[0].container[0].env_from[0].secret_ref[0].name == "spark-env"
    error_message = "The Iceberg Job must use the public Spark 4.0.4 image and the Secret spark-env."
  }

  assert {
    condition = alltrue([for s in [
      "--packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0,org.apache.iceberg:iceberg-aws-bundle:1.12.0,org.postgresql:postgresql:42.7.13",
      "spark.jars.ivy /tmp/ivy",
      "spark.sql.catalog.lake.uri jdbc:postgresql://postgres.hsec.svc.cluster.local:5432/iceberg_catalog",
      "spark.sql.catalog.lake.s3.endpoint http://rustfs-svc.hsec.svc.cluster.local:9000",
      "spark.sql.catalog.lake.jdbc.schema-version V1",
      "spark.sql.catalog.lake.jdbc.password $POSTGRES_ICEBERG_PASSWORD",
      "for f in /migrations/*.up.sql; do",
    ] : strcontains(kubernetes_job_v1.iceberg_migrations.spec[0].template[0].spec[0].container[0].command[2], s)])
    error_message = "The Iceberg Job must get the jars of the hsec-app-spark image, use the in-cluster catalog with V1, read the password from the environment, and run every .up.sql file."
  }

  assert {
    condition     = kubernetes_job_v1.clickhouse_migrations.wait_for_completion && kubernetes_job_v1.iceberg_migrations.wait_for_completion
    error_message = "Terraform must wait until each migration Job completes."
  }
}

run "migrations_follow_the_namespace" {
  command = plan

  variables {
    namespace = "home"
  }

  assert {
    condition     = strcontains(kubernetes_job_v1.clickhouse_migrations.spec[0].template[0].spec[0].container[0].args[3], "clickhouse.home.svc.cluster.local:9000") && strcontains(kubernetes_job_v1.iceberg_migrations.spec[0].template[0].spec[0].container[0].command[2], "postgres.home.svc.cluster.local:5432")
    error_message = "The migration Jobs must use the addresses in the given namespace."
  }
}

run "secrets" {
  command = apply

  assert {
    condition = jsonencode({ for k, s in kubernetes_secret_v1.this : k => sort(keys(nonsensitive(s.data))) }) == jsonencode({
      "bridge-env"       = ["MQTT_BRIDGE_PASSWORD", "MQTT_BRIDGE_USER"]
      "clickhouse-admin" = ["CLICKHOUSE_ADMIN_PASSWORD"]
      "flink-env"        = ["POSTGRES_ICEBERG_PASSWORD", "RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY"]
      "frigate-env"      = ["FRIGATE_MQTT_PASSWORD", "FRIGATE_MQTT_USER", "FRIGATE_RTSP_PASSWORD"]
      "mosquitto-env"    = ["MQTT_BRIDGE_PASSWORD", "MQTT_FRIGATE_PASSWORD"]
      "notifier-env"     = ["DISCORD_WEBHOOK_URL"]
      "postgres-iceberg" = ["POSTGRES_PASSWORD"]
      "rustfs-root"      = ["RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY"]
      "spark-env"        = ["CLICKHOUSE_SPARK_PASSWORD", "POSTGRES_ICEBERG_PASSWORD", "RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY"]
    })
    error_message = "Each Secret must hold exactly the keys that its pods read."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["mosquitto-env"].data["MQTT_FRIGATE_PASSWORD"] == kubernetes_secret_v1.this["frigate-env"].data["FRIGATE_MQTT_PASSWORD"])
    error_message = "Frigate and Mosquitto must share the MQTT password of frigate."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["mosquitto-env"].data["MQTT_BRIDGE_PASSWORD"] == kubernetes_secret_v1.this["bridge-env"].data["MQTT_BRIDGE_PASSWORD"])
    error_message = "The bridge and Mosquitto must share the MQTT password of rpconnect."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["flink-env"].data["RUSTFS_SECRET_KEY"] == kubernetes_secret_v1.this["rustfs-root"].data["RUSTFS_SECRET_KEY"] && kubernetes_secret_v1.this["spark-env"].data["RUSTFS_ACCESS_KEY"] == kubernetes_secret_v1.this["rustfs-root"].data["RUSTFS_ACCESS_KEY"])
    error_message = "Flink and Spark must use the RustFS keys."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["spark-env"].data["POSTGRES_ICEBERG_PASSWORD"] == kubernetes_secret_v1.this["postgres-iceberg"].data["POSTGRES_PASSWORD"])
    error_message = "Spark must use the Postgres password."
  }

  assert {
    condition     = nonsensitive(kubernetes_secret_v1.this["frigate-env"].data["FRIGATE_RTSP_PASSWORD"]) == "camera" && nonsensitive(kubernetes_secret_v1.this["notifier-env"].data["DISCORD_WEBHOOK_URL"]) == "https://discord.com/api/webhooks/1/abc"
    error_message = "The given secret values must go into their Secrets."
  }

  assert {
    condition     = alltrue([for p in random_password.this : p.special == false])
    error_message = "Passwords must use only letters and digits."
  }
}

run "network_policies" {
  command = plan

  assert {
    condition     = kubernetes_network_policy_v1.same_namespace.spec[0].pod_selector[0].match_expressions[0].operator == "NotIn" && kubernetes_network_policy_v1.same_namespace.spec[0].pod_selector[0].match_expressions[0].values == toset(["frigate"])
    error_message = "The same-namespace policy must not select Frigate."
  }

  assert {
    condition     = kubernetes_network_policy_v1.frigate.spec[0].ingress[1].from[0].pod_selector[0].match_labels == tomap({ app = "hsec-stream", component = "taskmanager" })
    error_message = "Only the Flink TaskManager may reach Frigate port 5000."
  }

  assert {
    condition     = [for p in kubernetes_network_policy_v1.frigate.spec[0].ingress[1].ports : p.port] == ["5000"] && length(kubernetes_network_policy_v1.frigate.spec[0].ingress[0].from) == 0
    error_message = "The LAN ports must be open to all, and port 5000 only to Flink."
  }

  assert {
    condition     = kubernetes_network_policy_v1.traefik.spec[0].pod_selector[0].match_expressions[0].operator == "In" && kubernetes_network_policy_v1.traefik.spec[0].pod_selector[0].match_expressions[0].values == toset(["hsec-stream", "rustfs"])
    error_message = "The Traefik policy must select only the Flink and RustFS pods."
  }

  assert {
    condition     = [for p in kubernetes_network_policy_v1.traefik.spec[0].ingress[0].ports : p.port] == ["8081", "9001"]
    error_message = "Traefik may reach only the Flink UI port and the RustFS console port."
  }

  assert {
    condition     = kubernetes_network_policy_v1.traefik.spec[0].ingress[0].from[0].namespace_selector[0].match_labels == tomap({ "kubernetes.io/metadata.name" = "kube-system" }) && kubernetes_network_policy_v1.traefik.spec[0].ingress[0].from[0].pod_selector[0].match_labels == tomap({ "app.kubernetes.io/name" = "traefik" })
    error_message = "Only the Traefik pod in kube-system may use the Traefik rule."
  }

  assert {
    condition     = length(kubernetes_network_policy_v1.clickhouse_grafana) == 0
    error_message = "Without grafana_namespace, there is no Grafana rule."
  }
}

run "grafana_rule_when_set" {
  command = plan

  variables {
    grafana_namespace = "monitoring"
  }

  assert {
    condition     = kubernetes_network_policy_v1.clickhouse_grafana[0].spec[0].ingress[0].from[0].namespace_selector[0].match_labels == tomap({ "kubernetes.io/metadata.name" = "monitoring" })
    error_message = "Grafana pods must reach ClickHouse port 8123."
  }
}

run "rejects_a_webhook_without_https" {
  command = plan

  variables {
    discord_webhook_url = "http://discord.com/api/webhooks/1/abc"
  }

  expect_failures = [var.discord_webhook_url]
}

run "rejects_a_domain_with_a_scheme" {
  command = plan

  variables {
    domain = "http://home.lan"
  }

  expect_failures = [var.domain]
}

run "domain_default" {
  command = plan

  assert {
    condition     = var.domain == "hsec.home.arpa"
    error_message = "The default domain must be hsec.home.arpa, the special-use name for home networks (RFC 8375)."
  }
}
