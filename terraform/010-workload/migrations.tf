# The schema migrations. Each Job runs after its databases are ready. Its name
# holds a hash of its files, so it runs again only when a file changes.

locals {
  clickhouse_migrations_dir = "${path.module}/../../hsec-db-clickhouse/migrations"
  iceberg_migrations_dir    = "${path.module}/../../hsec-db-iceberg/migrations"

  clickhouse_migrations = { for f in fileset(local.clickhouse_migrations_dir, "*.sql") : f => file("${local.clickhouse_migrations_dir}/${f}") }
  iceberg_migrations    = { for f in fileset(local.iceberg_migrations_dir, "*.up.sql") : f => file("${local.iceberg_migrations_dir}/${f}") }

  # The public image of the hsec-app-spark base. --packages gets the same jar
  # versions as hsec-app-spark from Maven Central, into /tmp/ivy.
  spark_image    = "apache/spark:4.0.4-scala2.13-java17-ubuntu"
  spark_packages = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0,org.apache.iceberg:iceberg-aws-bundle:1.12.0,org.postgresql:postgresql:42.7.13"

  # The catalog properties of the Spark job, with the in-cluster addresses.
  # The passwords go into a file, not onto the command line. Nothing records
  # which Iceberg files ran, so each file must change nothing on a second run.
  iceberg_script = <<-EOT
    set -eu
    umask 077
    cat > /tmp/spark.conf <<CONF
    spark.jars.ivy /tmp/ivy
    spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    spark.sql.catalog.lake org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.lake.type jdbc
    spark.sql.catalog.lake.uri jdbc:postgresql://postgres.${var.namespace}.svc.cluster.local:5432/iceberg_catalog
    spark.sql.catalog.lake.jdbc.user iceberg
    spark.sql.catalog.lake.jdbc.password $POSTGRES_ICEBERG_PASSWORD
    spark.sql.catalog.lake.jdbc.schema-version V1
    spark.sql.catalog.lake.warehouse s3://hsec-lake/warehouse
    spark.sql.catalog.lake.io-impl org.apache.iceberg.aws.s3.S3FileIO
    spark.sql.catalog.lake.s3.endpoint http://rustfs-svc.${var.namespace}.svc.cluster.local:9000
    spark.sql.catalog.lake.s3.path-style-access true
    spark.sql.catalog.lake.s3.access-key-id $RUSTFS_ACCESS_KEY
    spark.sql.catalog.lake.s3.secret-access-key $RUSTFS_SECRET_KEY
    spark.sql.catalog.lake.client.region us-east-1
    CONF
    for f in /migrations/*.up.sql; do
      echo "Running $f"
      /opt/spark/bin/spark-sql --master "local[1]" --packages ${local.spark_packages} --properties-file /tmp/spark.conf -f "$f"
    done
  EOT

  clickhouse_job_labels = merge(local.labels, { "app.kubernetes.io/name" = "clickhouse-migrations" })
  iceberg_job_labels    = merge(local.labels, { "app.kubernetes.io/name" = "iceberg-migrations" })
}

resource "kubernetes_config_map_v1" "clickhouse_migrations" {
  metadata {
    name      = "clickhouse-migrations"
    namespace = kubernetes_namespace_v1.hsec.metadata[0].name
    labels    = local.labels
  }

  data = local.clickhouse_migrations
}

# golang-migrate records the applied version in hsec.schema_migrations.
resource "kubernetes_job_v1" "clickhouse_migrations" {
  metadata {
    name      = "clickhouse-migrations-${substr(sha256(jsonencode(local.clickhouse_migrations)), 0, 8)}"
    namespace = kubernetes_namespace_v1.hsec.metadata[0].name
    labels    = local.clickhouse_job_labels
  }

  spec {
    backoff_limit = 3

    template {
      metadata {
        labels = local.clickhouse_job_labels
      }

      spec {
        restart_policy = "Never"

        container {
          name  = "migrate"
          image = "migrate/migrate:v4.20.1"
          # Kubernetes puts the password into $(CLICKHOUSE_ADMIN_PASSWORD). It has
          # only letters and digits, so it fits in the URL.
          args = [
            "-path", "/migrations",
            "-database", "clickhouse://clickhouse.${var.namespace}.svc.cluster.local:9000?username=hsec_admin&password=$(CLICKHOUSE_ADMIN_PASSWORD)&database=hsec&x-multi-statement=true",
            "up",
          ]

          env {
            name = "CLICKHOUSE_ADMIN_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.this["clickhouse-admin"].metadata[0].name
                key  = "CLICKHOUSE_ADMIN_PASSWORD"
              }
            }
          }

          volume_mount {
            name       = "migrations"
            mount_path = "/migrations"
            read_only  = true
          }

          # The driver loads the time zone of the server, and the image has no
          # time zone data.
          volume_mount {
            name       = "zoneinfo"
            mount_path = "/usr/share/zoneinfo"
            read_only  = true
          }

          resources {
            requests = {
              cpu    = "50m"
              memory = "64Mi"
            }
            limits = {
              memory = "64Mi"
            }
          }
        }

        volume {
          name = "migrations"
          config_map {
            name = kubernetes_config_map_v1.clickhouse_migrations.metadata[0].name
          }
        }

        volume {
          name = "zoneinfo"
          host_path {
            path = "/usr/share/zoneinfo"
            type = "Directory"
          }
        }
      }
    }
  }

  wait_for_completion = true

  timeouts {
    create = "10m"
    update = "10m"
  }

  depends_on = [module.clickhouse]
}

resource "kubernetes_config_map_v1" "iceberg_migrations" {
  metadata {
    name      = "iceberg-migrations"
    namespace = kubernetes_namespace_v1.hsec.metadata[0].name
    labels    = local.labels
  }

  data = local.iceberg_migrations
}

resource "kubernetes_job_v1" "iceberg_migrations" {
  metadata {
    name      = "iceberg-migrations-${substr(sha256(jsonencode([local.iceberg_migrations, local.iceberg_script])), 0, 8)}"
    namespace = kubernetes_namespace_v1.hsec.metadata[0].name
    labels    = local.iceberg_job_labels
  }

  spec {
    backoff_limit = 3

    template {
      metadata {
        labels = local.iceberg_job_labels
      }

      spec {
        restart_policy = "Never"

        container {
          name    = "spark-sql"
          image   = local.spark_image
          command = ["sh", "-c", local.iceberg_script]

          env_from {
            secret_ref {
              name = kubernetes_secret_v1.this["spark-env"].metadata[0].name
            }
          }

          volume_mount {
            name       = "migrations"
            mount_path = "/migrations"
            read_only  = true
          }

          resources {
            requests = {
              cpu    = "250m"
              memory = "1536Mi"
            }
            limits = {
              memory = "1536Mi"
            }
          }
        }

        volume {
          name = "migrations"
          config_map {
            name = kubernetes_config_map_v1.iceberg_migrations.metadata[0].name
          }
        }
      }
    }
  }

  wait_for_completion = true

  timeouts {
    create = "15m"
    update = "15m"
  }

  # Postgres holds the catalog, and RustFS holds the bucket hsec-lake.
  depends_on = [module.iceberg, module.rustfs]
}
