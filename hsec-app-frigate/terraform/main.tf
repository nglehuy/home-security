locals {
  name  = "frigate"
  image = "ghcr.io/blakeblackshear/frigate:0.18.0"

  selector = { "app.kubernetes.io/name" = local.name }
  labels   = merge(var.labels, local.selector)

  seed_config = templatefile("${path.module}/../config.seed.yml.tftpl", {
    namespace = var.namespace
    cameras   = var.cameras
  })
}

# The init container copies this file only when /config/config.yml does not
# exist. After the first start, the Frigate UI owns the configuration.
resource "kubernetes_config_map_v1" "seed" {
  metadata {
    name      = "frigate-seed"
    namespace = var.namespace
    labels    = local.labels
  }

  data = {
    "config.yml" = local.seed_config
  }
}

resource "kubernetes_persistent_volume_claim_v1" "config" {
  metadata {
    name      = "frigate-config"
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.ssd_storage_class

    resources {
      requests = {
        storage = "5Gi"
      }
    }
  }

  # Local volumes bind only when the first pod starts.
  wait_until_bound = false
}

resource "kubernetes_persistent_volume_claim_v1" "media" {
  metadata {
    name      = "frigate-media"
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.hdd_storage_class

    resources {
      requests = {
        storage = "200Gi"
      }
    }
  }

  wait_until_bound = false
}

resource "kubernetes_deployment_v1" "frigate" {
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
      }

      spec {
        termination_grace_period_seconds = 30

        init_container {
          name    = "seed-config"
          image   = local.image
          command = ["sh", "-c", "[ -f /config/config.yml ] || cp /seed/config.yml /config/config.yml"]

          volume_mount {
            name       = "config"
            mount_path = "/config"
          }

          volume_mount {
            name       = "seed"
            mount_path = "/seed"
          }
        }

        container {
          name  = "frigate"
          image = local.image

          env {
            name  = "TZ"
            value = var.timezone
          }

          env_from {
            secret_ref {
              name = var.env_secret_name
            }
          }

          port {
            name           = "https"
            container_port = 8971
          }

          port {
            name           = "rtsp"
            container_port = 8554
          }

          port {
            name           = "webrtc-tcp"
            container_port = 8555
            protocol       = "TCP"
          }

          port {
            name           = "webrtc-udp"
            container_port = 8555
            protocol       = "UDP"
          }

          port {
            name           = "api"
            container_port = 5000
          }

          resources {
            requests = {
              cpu                  = "1"
              memory               = "3Gi"
              "gpu.intel.com/i915" = "1"
            }
            limits = {
              memory               = "3Gi"
              "gpu.intel.com/i915" = "1"
            }
          }

          # The first start downloads models and takes several minutes.
          startup_probe {
            http_get {
              path = "/api/version"
              port = 5000
            }
            period_seconds    = 10
            failure_threshold = 60
          }

          liveness_probe {
            http_get {
              path = "/api/version"
              port = 5000
            }
            period_seconds = 30
          }

          volume_mount {
            name       = "config"
            mount_path = "/config"
          }

          volume_mount {
            name       = "media"
            mount_path = "/media/frigate"
          }

          volume_mount {
            name       = "shm"
            mount_path = "/dev/shm"
          }

          volume_mount {
            name       = "cache"
            mount_path = "/tmp/cache"
          }
        }

        volume {
          name = "config"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.config.metadata[0].name
          }
        }

        volume {
          name = "media"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.media.metadata[0].name
          }
        }

        volume {
          name = "seed"
          config_map {
            name = kubernetes_config_map_v1.seed.metadata[0].name
          }
        }

        # Memory-backed volumes count toward the 3 GiB memory limit.
        volume {
          name = "shm"
          empty_dir {
            medium     = "Memory"
            size_limit = "512Mi"
          }
        }

        volume {
          name = "cache"
          empty_dir {
            medium     = "Memory"
            size_limit = "1Gi"
          }
        }
      }
    }
  }

  timeouts {
    create = "15m"
    update = "15m"
  }
}

# UI, RTSP restream, and WebRTC for LAN clients, through k3s ServiceLB.
resource "kubernetes_service_v1" "frigate" {
  metadata {
    name      = local.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    type     = "LoadBalancer"
    selector = local.selector

    port {
      name        = "https"
      port        = 8971
      target_port = 8971
    }

    port {
      name        = "rtsp"
      port        = 8554
      target_port = 8554
    }

    port {
      name        = "webrtc-tcp"
      port        = 8555
      target_port = 8555
      protocol    = "TCP"
    }

    port {
      name        = "webrtc-udp"
      port        = 8555
      target_port = 8555
      protocol    = "UDP"
    }
  }
}

# Port 5000 has no login. A network policy lets only the Flink TaskManager in.
resource "kubernetes_service_v1" "api" {
  metadata {
    name      = "frigate-api"
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    type     = "ClusterIP"
    selector = local.selector

    port {
      name        = "api"
      port        = 5000
      target_port = 5000
    }
  }
}
