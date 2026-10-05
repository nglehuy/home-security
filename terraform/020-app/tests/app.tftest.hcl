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
  kube_context      = "test"
  ssd_storage_class = "ssd"
  hdd_storage_class = "hdd"
  image_registry    = "registry.lan:5000/"
  flink_image_tag   = "1.2.3"
  spark_image_tag   = "4.5.6"
  frigate_url       = "https://192.168.1.17:8971"
  cameras = {
    front_door = {
      detect_url    = "rtsp://admin:{FRIGATE_RTSP_PASSWORD}@192.168.1.20:554/stream2"
      record_url    = "rtsp://admin:{FRIGATE_RTSP_PASSWORD}@192.168.1.20:554/stream1"
      detect_width  = 640
      detect_height = 360
    }
  }
}

run "plans_all_apps" {
  command = plan

  assert {
    condition     = output.images == { flink = "registry.lan:5000/hsec-app-flink:1.2.3", spark = "registry.lan:5000/hsec-app-spark:4.5.6" }
    error_message = "Each image must have the name of its app folder and the given tag."
  }
}

run "rejects_a_frigate_url_without_scheme" {
  command = plan

  variables {
    frigate_url = "192.168.1.17:8971"
  }

  expect_failures = [var.frigate_url]
}
