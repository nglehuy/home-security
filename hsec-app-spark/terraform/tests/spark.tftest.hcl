mock_provider "helm" {}
mock_provider "kubernetes" {}

variables {
  namespace       = "hsec"
  labels          = { "app.kubernetes.io/part-of" = "hsec" }
  image           = "registry.lan/hsec-app-spark:1.0.0"
  timezone        = "Asia/Singapore"
  env_secret_name = "spark-env"
}

run "operator" {
  command = plan

  assert {
    condition     = helm_release.operator.chart == "spark-operator" && helm_release.operator.version == "2.5.2"
    error_message = "The operator chart must be 2.5.2."
  }

  assert {
    condition     = yamldecode(helm_release.operator.values[0]).spark.jobNamespaces == ["hsec"] && yamldecode(helm_release.operator.values[0]).webhook.enable
    error_message = "The operator must run jobs only in hsec, with the webhook for envFrom."
  }

  assert {
    condition     = yamldecode(helm_release.operator.values[0]).controller.resources.limits.memory == "512Mi" && yamldecode(helm_release.operator.values[0]).webhook.resources.limits.memory == "128Mi"
    error_message = "The controller runs spark-submit and needs 512 MiB. The webhook needs 128 MiB."
  }
}

run "job" {
  command = plan

  assert {
    condition     = helm_release.job.chart == "./chart"
    error_message = "The job must come from the local chart."
  }

  assert {
    condition     = yamldecode(helm_release.job.values[0]) == { image = "registry.lan/hsec-app-spark:1.0.0", timezone = "Asia/Singapore", nightStart = "23:00", nightEnd = "06:00", envSecret = "spark-env", serviceAccount = "spark-operator-spark", labels = { "app.kubernetes.io/part-of" = "hsec" } }
    error_message = "The job values must hold the image, the time zone, the night window, and the Secret."
  }
}

run "rejects_a_bad_night_window" {
  command = plan

  variables {
    night_start = "25:00"
  }

  expect_failures = [var.night_start]
}
