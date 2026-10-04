mock_provider "kubernetes" {}

variables {
  namespace       = "hsec"
  labels          = { "app.kubernetes.io/part-of" = "hsec" }
  timezone        = "Asia/Singapore"
  env_secret_name = "notifier-env"
}

run "pipeline_and_pod" {
  command = plan

  assert {
    condition     = kubernetes_config_map_v1.pipeline.metadata[0].name == "notifier-pipeline" && kubernetes_config_map_v1.pipeline.data["alerts-to-discord.yaml"] == file("../connect/alerts-to-discord.yaml")
    error_message = "The ConfigMap notifier-pipeline must hold the pipeline file from the app folder."
  }

  assert {
    condition     = kubernetes_deployment_v1.notifier.spec[0].template[0].metadata[0].annotations["checksum/pipeline"] == sha256(file("../connect/alerts-to-discord.yaml"))
    error_message = "The pod must carry the hash of the pipeline file."
  }

  assert {
    condition     = kubernetes_deployment_v1.notifier.spec[0].strategy[0].type == "Recreate"
    error_message = "The old notifier pod must stop before the new pod starts."
  }

  assert {
    condition     = kubernetes_deployment_v1.notifier.spec[0].template[0].spec[0].container[0].args == tolist(["run", "/config/alerts-to-discord.yaml"])
    error_message = "The container must run the mounted pipeline."
  }

  assert {
    condition     = kubernetes_deployment_v1.notifier.spec[0].template[0].spec[0].container[0].env[0].name == "TZ" && kubernetes_deployment_v1.notifier.spec[0].template[0].spec[0].container[0].env[0].value == "Asia/Singapore"
    error_message = "The notifier must get the local time zone."
  }

  assert {
    condition     = kubernetes_deployment_v1.notifier.spec[0].template[0].spec[0].container[0].env_from[0].secret_ref[0].name == "notifier-env"
    error_message = "The notifier must read the webhook URL from the given Secret."
  }

  assert {
    condition     = kubernetes_deployment_v1.notifier.spec[0].template[0].spec[0].container[0].resources[0].limits["memory"] == "256Mi"
    error_message = "One message can hold a 19 MiB file, so the notifier needs 256 MiB."
  }

  assert {
    condition     = kubernetes_deployment_v1.notifier.spec[0].template[0].spec[0].container[0].liveness_probe[0].http_get[0].path == "/ping" && kubernetes_deployment_v1.notifier.spec[0].template[0].spec[0].container[0].readiness_probe[0].http_get[0].path == "/ready"
    error_message = "The probes must use /ping and /ready."
  }
}
