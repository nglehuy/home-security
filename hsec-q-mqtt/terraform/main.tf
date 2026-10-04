locals {
  name  = "mosquitto"
  image = "eclipse-mosquitto:2.1.2-alpine"

  selector = { "app.kubernetes.io/name" = local.name }
  labels   = merge(var.labels, local.selector)

  config_files = {
    "mosquitto.conf" = file("${path.module}/../mosquitto.conf")
    "acl"            = file("${path.module}/../acl")
  }

  # Builds the password file from the Secret, so no password is in a ConfigMap.
  # It also copies the ACL, because Mosquitto wants both files owned by its
  # own user, and a ConfigMap mount is always owned by root.
  make_passwd = <<-EOT
    set -eu
    mosquitto_passwd -c -b /mosquitto/auth/passwd frigate "$MQTT_FRIGATE_PASSWORD"
    mosquitto_passwd -b /mosquitto/auth/passwd rpconnect "$MQTT_BRIDGE_PASSWORD"
    cp /mosquitto/config/acl /mosquitto/auth/acl
    chown 1883:1883 /mosquitto/auth/passwd /mosquitto/auth/acl
    chmod 0700 /mosquitto/auth/passwd /mosquitto/auth/acl
  EOT
}

resource "kubernetes_config_map_v1" "config" {
  metadata {
    name      = "mosquitto-config"
    namespace = var.namespace
    labels    = local.labels
  }

  data = local.config_files
}

resource "kubernetes_persistent_volume_claim_v1" "data" {
  metadata {
    name      = "mosquitto-data"
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.ssd_storage_class

    resources {
      requests = {
        storage = "1Gi"
      }
    }
  }

  # Local volumes bind only when the first pod starts.
  wait_until_bound = false
}

resource "kubernetes_deployment_v1" "mosquitto" {
  metadata {
    name      = local.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    replicas = 1

    strategy {
      type = "Recreate"
    }

    selector {
      match_labels = local.selector
    }

    template {
      metadata {
        labels = local.labels
        annotations = {
          "checksum/config" = sha256(jsonencode(local.config_files))
        }
      }

      spec {
        init_container {
          name    = "make-passwd"
          image   = local.image
          command = ["sh", "-c", local.make_passwd]

          env_from {
            secret_ref {
              name = var.env_secret_name
            }
          }

          volume_mount {
            name       = "config"
            mount_path = "/mosquitto/config"
          }

          volume_mount {
            name       = "auth"
            mount_path = "/mosquitto/auth"
          }
        }

        container {
          name  = "mosquitto"
          image = local.image

          port {
            name           = "mqtt"
            container_port = 1883
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

          liveness_probe {
            tcp_socket {
              port = 1883
            }
            period_seconds = 30
          }

          volume_mount {
            name       = "config"
            mount_path = "/mosquitto/config"
          }

          volume_mount {
            name       = "auth"
            mount_path = "/mosquitto/auth"
          }

          volume_mount {
            name       = "data"
            mount_path = "/mosquitto/data"
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map_v1.config.metadata[0].name
          }
        }

        volume {
          name = "auth"
          empty_dir {}
        }

        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.data.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "mosquitto" {
  metadata {
    name      = local.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    type     = "ClusterIP"
    selector = local.selector

    port {
      name        = "mqtt"
      port        = 1883
      target_port = 1883
    }
  }
}
