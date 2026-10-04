mock_provider "kubernetes" {}

variables {
  namespace         = "hsec"
  labels            = { "app.kubernetes.io/part-of" = "hsec" }
  ssd_storage_class = "ssd"
  hdd_storage_class = "hdd"
  timezone          = "Asia/Singapore"
  env_secret_name   = "frigate-env"
  cameras = {
    front_door = {
      detect_url    = "rtsp://admin:{FRIGATE_RTSP_PASSWORD}@192.168.1.20:554/sub"
      record_url    = "rtsp://admin:{FRIGATE_RTSP_PASSWORD}@192.168.1.20:554/main"
      detect_width  = 640
      detect_height = 360
    }
    garage = {
      detect_url    = "rtsp://admin:{FRIGATE_RTSP_PASSWORD}@192.168.1.21:554/sub"
      record_url    = "rtsp://admin:{FRIGATE_RTSP_PASSWORD}@192.168.1.21:554/main"
      detect_width  = 1280
      detect_height = 720
    }
  }
}

run "seed_configuration" {
  command = plan

  assert {
    condition     = keys(yamldecode(kubernetes_config_map_v1.seed.data["config.yml"]).cameras) == ["front_door", "garage"]
    error_message = "The seed configuration must list every camera."
  }

  assert {
    condition     = yamldecode(kubernetes_config_map_v1.seed.data["config.yml"]).cameras.garage.detect == { width = 1280, height = 720, fps = 5 }
    error_message = "Each camera must use its own detect size and 5 fps."
  }

  assert {
    condition = yamldecode(kubernetes_config_map_v1.seed.data["config.yml"]).cameras.front_door.ffmpeg.inputs == [
      { path = "rtsp://admin:{FRIGATE_RTSP_PASSWORD}@192.168.1.20:554/main", roles = ["record"] },
      { path = "rtsp://admin:{FRIGATE_RTSP_PASSWORD}@192.168.1.20:554/sub", roles = ["detect"] },
    ]
    error_message = "The record and detect streams must keep the password placeholder."
  }

  assert {
    condition     = yamldecode(kubernetes_config_map_v1.seed.data["config.yml"]).mqtt == { enabled = true, host = "mosquitto.hsec.svc.cluster.local", port = 1883, user = "{FRIGATE_MQTT_USER}", password = "{FRIGATE_MQTT_PASSWORD}", qos = 1 }
    error_message = "MQTT must use the in-cluster Mosquitto with QoS 1 and the password placeholders."
  }

  assert {
    condition     = yamldecode(kubernetes_config_map_v1.seed.data["config.yml"]).detectors.ov == { type = "openvino", device = "GPU" }
    error_message = "Detection must use OpenVINO on the GPU."
  }

  assert {
    condition     = yamldecode(kubernetes_config_map_v1.seed.data["config.yml"]).record == { enabled = true, continuous = { days = 0 }, motion = { days = 0 }, alerts = { retain = { days = 14 } }, detections = { retain = { days = 14 } } }
    error_message = "Frigate must keep only alert and detection video, for 14 days."
  }

  assert {
    condition     = yamldecode(kubernetes_config_map_v1.seed.data["config.yml"]).snapshots.retain.default == 30
    error_message = "Snapshots must stay for 30 days."
  }
}

run "pod" {
  command = plan

  assert {
    condition     = kubernetes_deployment_v1.frigate.spec[0].strategy[0].type == "Recreate"
    error_message = "Frigate must stop before a new pod starts."
  }

  assert {
    condition     = kubernetes_deployment_v1.frigate.spec[0].template[0].spec[0].container[0].env_from[0].secret_ref[0].name == "frigate-env"
    error_message = "Frigate must read its passwords from the given Secret."
  }

  assert {
    condition     = kubernetes_deployment_v1.frigate.spec[0].template[0].spec[0].container[0].resources[0].limits == tomap({ memory = "3Gi", "gpu.intel.com/i915" = "1" })
    error_message = "Frigate must get 3 GiB of memory and one Intel GPU."
  }

  assert {
    condition     = kubernetes_deployment_v1.frigate.spec[0].template[0].spec[0].init_container[0].command[2] == "[ -f /config/config.yml ] || cp /seed/config.yml /config/config.yml"
    error_message = "The init container must copy the seed only when no configuration exists."
  }

  assert {
    condition     = kubernetes_deployment_v1.frigate.spec[0].template[0].spec[0].container[0].startup_probe[0].failure_threshold == 60
    error_message = "The startup probe must allow 10 minutes for the first start."
  }

  assert {
    condition     = kubernetes_deployment_v1.frigate.spec[0].template[0].metadata[0].labels == tomap({ "app.kubernetes.io/name" = "frigate", "app.kubernetes.io/part-of" = "hsec" })
    error_message = "The pod must have the name label, which the network policies use."
  }
}

run "volumes_and_services" {
  command = plan

  assert {
    condition     = kubernetes_persistent_volume_claim_v1.config.spec[0].storage_class_name == "ssd" && kubernetes_persistent_volume_claim_v1.config.spec[0].resources[0].requests.storage == "5Gi"
    error_message = "The configuration volume must be 5 GiB on the SSD class."
  }

  assert {
    condition     = kubernetes_persistent_volume_claim_v1.media.spec[0].storage_class_name == "hdd" && kubernetes_persistent_volume_claim_v1.media.spec[0].resources[0].requests.storage == "200Gi"
    error_message = "The media volume must be 200 GiB on the HDD class."
  }

  assert {
    condition     = kubernetes_service_v1.frigate.spec[0].type == "LoadBalancer" && !contains([for p in kubernetes_service_v1.frigate.spec[0].port : p.port], 5000)
    error_message = "The LAN Service must not expose port 5000."
  }

  assert {
    condition     = kubernetes_service_v1.api.spec[0].type == "ClusterIP" && kubernetes_service_v1.api.spec[0].port[0].port == 5000
    error_message = "frigate-api must expose port 5000 inside the cluster only."
  }
}

run "rejects_no_cameras" {
  command = plan

  variables {
    cameras = {}
  }

  expect_failures = [var.cameras]
}

run "rejects_bad_camera_name" {
  command = plan

  variables {
    cameras = {
      "front door" = {
        detect_url    = "rtsp://x/sub"
        record_url    = "rtsp://x/main"
        detect_width  = 640
        detect_height = 360
      }
    }
  }

  expect_failures = [var.cameras]
}

run "rejects_zero_detect_size" {
  command = plan

  variables {
    cameras = {
      front_door = {
        detect_url    = "rtsp://x/sub"
        record_url    = "rtsp://x/main"
        detect_width  = 0
        detect_height = 360
      }
    }
  }

  expect_failures = [var.cameras]
}
