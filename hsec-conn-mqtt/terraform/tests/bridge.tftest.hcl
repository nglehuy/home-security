mock_provider "kubernetes" {}

variables {
  namespace       = "hsec"
  labels          = { "app.kubernetes.io/part-of" = "hsec" }
  env_secret_name = "bridge-env"
}

run "pipeline_and_pod" {
  command = plan

  assert {
    condition     = kubernetes_config_map_v1.pipeline.metadata[0].name == "bridge-pipeline" && kubernetes_config_map_v1.pipeline.data["frigate-to-redpanda.yaml"] == file("../connect/frigate-to-redpanda.yaml")
    error_message = "The ConfigMap bridge-pipeline must hold the pipeline file from the app folder."
  }

  assert {
    condition     = kubernetes_deployment_v1.bridge.spec[0].template[0].metadata[0].annotations["checksum/pipeline"] == sha256(file("../connect/frigate-to-redpanda.yaml"))
    error_message = "The pod must carry the hash of the pipeline file."
  }

  assert {
    condition     = kubernetes_deployment_v1.bridge.spec[0].strategy[0].type == "Recreate"
    error_message = "The old bridge pod must stop before the new pod starts."
  }

  assert {
    condition     = kubernetes_deployment_v1.bridge.spec[0].template[0].spec[0].container[0].args == tolist(["run", "/config/frigate-to-redpanda.yaml"])
    error_message = "The container must run the mounted pipeline."
  }

  assert {
    condition     = kubernetes_deployment_v1.bridge.spec[0].template[0].spec[0].container[0].env_from[0].secret_ref[0].name == "bridge-env"
    error_message = "The bridge must read its MQTT login from the given Secret."
  }

  assert {
    condition     = kubernetes_deployment_v1.bridge.spec[0].template[0].spec[0].container[0].liveness_probe[0].http_get[0].path == "/ping" && kubernetes_deployment_v1.bridge.spec[0].template[0].spec[0].container[0].readiness_probe[0].http_get[0].path == "/ready"
    error_message = "The probes must use /ping and /ready."
  }

  assert {
    condition     = kubernetes_deployment_v1.bridge.spec[0].template[0].spec[0].container[0].env[0].value_from[0].field_ref[0].field_path == "metadata.namespace"
    error_message = "The pipeline must get the namespace of its pod."
  }
}
