# The k3s embedded network policy controller enforces these policies. Egress is
# not limited. Kubernetes always allows traffic from the node where a pod runs,
# so probes, port forwards, and the Spark operator webhook need no rule.

# Every pod except Frigate accepts traffic only from pods in the same namespace.
# Network policies add up, so this policy must not select Frigate. Otherwise
# every pod in the namespace could reach port 5000, which has no login.
resource "kubernetes_network_policy_v1" "same_namespace" {
  metadata {
    name      = "allow-same-namespace"
    namespace = kubernetes_namespace_v1.hsec.metadata[0].name
    labels    = local.labels
  }

  spec {
    pod_selector {
      match_expressions {
        key      = "app.kubernetes.io/name"
        operator = "NotIn"
        values   = ["frigate"]
      }
    }

    policy_types = ["Ingress"]

    ingress {
      from {
        pod_selector {}
      }
    }
  }
}

resource "kubernetes_network_policy_v1" "frigate" {
  metadata {
    name      = "frigate"
    namespace = kubernetes_namespace_v1.hsec.metadata[0].name
    labels    = local.labels
  }

  spec {
    pod_selector {
      match_labels = { "app.kubernetes.io/name" = "frigate" }
    }

    policy_types = ["Ingress"]

    # UI, RTSP restream, and WebRTC for LAN and WireGuard clients, through ServiceLB.
    ingress {
      ports {
        port     = "8971"
        protocol = "TCP"
      }
      ports {
        port     = "8554"
        protocol = "TCP"
      }
      ports {
        port     = "8555"
        protocol = "TCP"
      }
      ports {
        port     = "8555"
        protocol = "UDP"
      }
    }

    # Only the Flink TaskManager downloads alert images and clips from port 5000.
    ingress {
      ports {
        port     = "5000"
        protocol = "TCP"
      }
      from {
        pod_selector {
          match_labels = {
            app       = "hsec-stream"
            component = "taskmanager"
          }
        }
      }
    }
  }
}

resource "kubernetes_network_policy_v1" "clickhouse_grafana" {
  count = var.grafana_namespace == null ? 0 : 1

  metadata {
    name      = "clickhouse-from-grafana"
    namespace = kubernetes_namespace_v1.hsec.metadata[0].name
    labels    = local.labels
  }

  spec {
    pod_selector {
      match_labels = { "app.kubernetes.io/name" = "clickhouse" }
    }

    policy_types = ["Ingress"]

    ingress {
      ports {
        port     = "8123"
        protocol = "TCP"
      }
      from {
        namespace_selector {
          match_labels = { "kubernetes.io/metadata.name" = var.grafana_namespace }
        }
      }
    }
  }
}
