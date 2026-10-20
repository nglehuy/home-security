mock_provider "kubernetes" {
  mock_data "kubernetes_secret_v1" {
    defaults = {
      data = {
        RUSTFS_ACCESS_KEY = "access"
        RUSTFS_SECRET_KEY = "secret"
      }
    }
  }
}

mock_provider "helm" {}

variables {
  namespace          = "hsec"
  labels             = { "app.kubernetes.io/part-of" = "hsec" }
  image              = "registry.lan/hsec-app-flink:1.0.0"
  timezone           = "Asia/Singapore"
  env_secret_name    = "flink-env"
  rustfs_secret_name = "rustfs-root"
  ingress_host       = "flink.home.lan"
}

run "operator" {
  command = plan

  assert {
    condition     = helm_release.operator.chart == "flink-kubernetes-operator" && helm_release.operator.version == "1.16.1"
    error_message = "The operator chart must be 1.16.1."
  }

  assert {
    condition     = yamldecode(helm_release.operator.values[0]).watchNamespaces == ["hsec"] && yamldecode(helm_release.operator.values[0]).webhook.create == false
    error_message = "The operator must watch only hsec, without the webhook."
  }

  assert {
    condition     = yamldecode(helm_release.operator.values[0]).operatorPod.resources.limits.memory == "512Mi"
    error_message = "The operator must get 512 MiB."
  }
}

run "job" {
  command = plan

  assert {
    condition     = helm_release.job.chart == "./chart"
    error_message = "The job must come from the local chart."
  }

  assert {
    condition     = yamldecode(helm_release.job.values[0]) == { image = "registry.lan/hsec-app-flink:1.0.0", timezone = "Asia/Singapore", envSecret = "flink-env", labels = { "app.kubernetes.io/part-of" = "hsec" } }
    error_message = "The job values must hold the image, the time zone, and the Secret. The chart builds the Frigate URL from the namespace."
  }

  assert {
    condition     = [for s in helm_release.job.set_sensitive : s.name] == ["s3AccessKey", "s3SecretKey"]
    error_message = "The RustFS keys must go to the chart as sensitive values."
  }
}

run "ui_ingress" {
  command = plan

  assert {
    condition     = kubernetes_ingress_v1.ui.spec[0].ingress_class_name == "traefik" && kubernetes_ingress_v1.ui.spec[0].rule[0].host == "flink.home.lan"
    error_message = "Traefik must route the given host."
  }

  assert {
    condition     = kubernetes_ingress_v1.ui.spec[0].rule[0].http[0].path[0].backend[0].service[0].name == "hsec-stream-rest" && kubernetes_ingress_v1.ui.spec[0].rule[0].http[0].path[0].backend[0].service[0].port[0].number == 8081
    error_message = "The Ingress must go to the REST Service that the operator creates for the job."
  }

  assert {
    condition     = length(kubernetes_ingress_v1.ui.spec[0].tls) == 0
    error_message = "The Ingress must use plain HTTP."
  }
}
