mock_provider "kubernetes" {}
mock_provider "helm" {}

variables {
  namespace         = "hsec"
  labels            = { "app.kubernetes.io/part-of" = "hsec" }
  hdd_storage_class = "hdd"
  root_secret_name  = "rustfs-root"
}

run "chart" {
  command = plan

  assert {
    condition     = helm_release.rustfs.chart == "rustfs" && helm_release.rustfs.version == "1.0.1"
    error_message = "The chart must be rustfs 1.0.1."
  }

  assert {
    condition     = yamldecode(helm_release.rustfs.values[0]).mode == { standalone = { enabled = true }, distributed = { enabled = false } } && yamldecode(helm_release.rustfs.values[0]).replicaCount == 1
    error_message = "RustFS must run one standalone pod."
  }

  assert {
    condition     = yamldecode(helm_release.rustfs.values[0]).storageclass == { name = "hdd", dataStorageSize = "400Gi", logStorageSize = "1Gi" }
    error_message = "RustFS must keep 400 GiB of data and 1 GiB of logs on the HDD class."
  }

  assert {
    condition     = yamldecode(helm_release.rustfs.values[0]).secret.existingSecret == "rustfs-root" && yamldecode(helm_release.rustfs.values[0]).ingress.enabled == false
    error_message = "RustFS must use the given Secret and no Ingress."
  }
}

run "buckets" {
  command = plan

  assert {
    condition     = strcontains(kubernetes_job_v1.buckets.spec[0].template[0].spec[0].container[0].command[2], "for B in hsec-lake hsec-flink; do")
    error_message = "The Job must create both buckets."
  }

  assert {
    condition     = [for e in kubernetes_job_v1.buckets.spec[0].template[0].spec[0].container[0].env : e.value if e.name == "S3_ENDPOINT"] == ["http://rustfs-svc.hsec.svc.cluster.local:9000"]
    error_message = "The Job must use the in-cluster S3 address."
  }

  assert {
    condition     = [for e in kubernetes_job_v1.buckets.spec[0].template[0].spec[0].container[0].env : e.value_from[0].secret_key_ref[0].key if e.name == "AWS_SECRET_ACCESS_KEY"] == ["RUSTFS_SECRET_KEY"]
    error_message = "The Job must read the RustFS keys from the Secret."
  }
}
