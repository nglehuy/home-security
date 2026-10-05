# The Intel GPU device plugin gives the UHD 620 to Frigate as gpu.intel.com/i915.
# Translated from deployments/gpu_plugin/base/intel-gpu-plugin.yaml of release v0.37.1.
# It offers only Intel GPUs, so the AMD GPU never reaches a pod.

resource "kubernetes_namespace_v1" "gpu" {
  metadata {
    name   = "intel-gpu-plugin"
    labels = local.labels
  }
}

resource "kubernetes_daemon_set_v1" "gpu_plugin" {
  metadata {
    name      = "intel-gpu-plugin"
    namespace = kubernetes_namespace_v1.gpu.metadata[0].name
    labels    = merge(local.labels, { app = "intel-gpu-plugin" })
  }

  spec {
    selector {
      match_labels = { app = "intel-gpu-plugin" }
    }

    strategy {
      type = "RollingUpdate"
      rolling_update {
        max_surge       = 0
        max_unavailable = 1
      }
    }

    template {
      metadata {
        labels = merge(local.labels, { app = "intel-gpu-plugin" })
      }

      spec {
        automount_service_account_token = false

        node_selector = {
          "kubernetes.io/arch"     = "amd64"
          "kubernetes.io/hostname" = var.node_name
        }

        container {
          name  = "intel-gpu-plugin"
          image = "intel/intel-gpu-plugin:0.37.1"
          # Only Frigate uses the GPU.
          args = ["-shared-dev-num=1"]

          env {
            name = "NODE_NAME"
            value_from {
              field_ref {
                field_path = "spec.nodeName"
              }
            }
          }

          env {
            name = "HOST_IP"
            value_from {
              field_ref {
                field_path = "status.hostIP"
              }
            }
          }

          security_context {
            read_only_root_filesystem  = true
            allow_privilege_escalation = false

            se_linux_options {
              type = "container_device_plugin_t"
            }

            capabilities {
              drop = ["ALL"]
            }

            seccomp_profile {
              type = "RuntimeDefault"
            }
          }

          resources {
            requests = {
              cpu    = "40m"
              memory = "45Mi"
            }
            limits = {
              cpu    = "100m"
              memory = "115Mi"
            }
          }

          volume_mount {
            name       = "devfs"
            mount_path = "/dev/dri"
            read_only  = true
          }

          volume_mount {
            name       = "sysfsdrm"
            mount_path = "/sys/class/drm"
            read_only  = true
          }

          volume_mount {
            name       = "kubeletsockets"
            mount_path = "/var/lib/kubelet/device-plugins"
          }

          volume_mount {
            name       = "cdipath"
            mount_path = "/var/run/cdi"
          }
        }

        volume {
          name = "devfs"
          host_path {
            path = "/dev/dri"
          }
        }

        volume {
          name = "sysfsdrm"
          host_path {
            path = "/sys/class/drm"
          }
        }

        volume {
          name = "kubeletsockets"
          host_path {
            path = "/var/lib/kubelet/device-plugins"
          }
        }

        volume {
          name = "cdipath"
          host_path {
            path = "/var/run/cdi"
            type = "DirectoryOrCreate"
          }
        }
      }
    }
  }
}
