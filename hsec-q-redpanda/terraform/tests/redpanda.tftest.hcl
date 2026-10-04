mock_provider "kubernetes" {}
mock_provider "helm" {}

variables {
  namespace         = "hsec"
  labels            = { "app.kubernetes.io/part-of" = "hsec" }
  ssd_storage_class = "ssd"
}

run "chart" {
  command = plan

  assert {
    condition     = helm_release.redpanda.chart == "redpanda" && helm_release.redpanda.version == "26.2.4"
    error_message = "The chart must be redpanda 26.2.4."
  }

  assert {
    condition     = yamldecode(helm_release.redpanda.values[0]).statefulset.replicas == 1
    error_message = "Redpanda must run one broker."
  }

  assert {
    condition     = !yamldecode(helm_release.redpanda.values[0]).tls.enabled && !yamldecode(helm_release.redpanda.values[0]).external.enabled && !yamldecode(helm_release.redpanda.values[0]).console.enabled
    error_message = "TLS, external access, and the console must be off."
  }

  assert {
    condition     = yamldecode(helm_release.redpanda.values[0]).resources == { cpu = { cores = 1 }, memory = { container = { max = "2.5Gi" } } }
    error_message = "Redpanda must get 1 core and 2.5 GiB."
  }

  assert {
    condition     = yamldecode(helm_release.redpanda.values[0]).storage.persistentVolume == { size = "10Gi", storageClass = "ssd" }
    error_message = "The volume must be 10 GiB on the SSD class."
  }

  assert {
    condition     = yamldecode(helm_release.redpanda.values[0]).config.cluster.auto_create_topics_enabled == false
    error_message = "Only the topics Job creates topics."
  }
}

run "topics_job" {
  command = plan

  assert {
    condition     = kubernetes_config_map_v1.topics.data["topics.sh"] == file("../topics.sh")
    error_message = "The Job must run topics.sh from the app folder."
  }

  assert {
    condition     = kubernetes_job_v1.topics.metadata[0].name == "redpanda-topics-${substr(sha256(file("../topics.sh")), 0, 8)}"
    error_message = "The Job name must change when the script changes."
  }

  assert {
    condition     = kubernetes_job_v1.topics.spec[0].template[0].spec[0].container[0].env[0].value == "redpanda-0.redpanda.hsec.svc.cluster.local:9093"
    error_message = "The Job must use the internal Kafka address."
  }
}

run "other_namespace" {
  command = plan

  variables {
    namespace = "lab"
  }

  assert {
    condition     = kubernetes_job_v1.topics.spec[0].template[0].spec[0].container[0].env[0].value == "redpanda-0.redpanda.lab.svc.cluster.local:9093"
    error_message = "The Kafka address must follow the namespace."
  }
}
