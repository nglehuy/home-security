mock_provider "kubernetes" {}

variables {
  namespace         = "hsec"
  labels            = { "app.kubernetes.io/part-of" = "hsec" }
  ssd_storage_class = "ssd"
  env_secret_name   = "mosquitto-env"
}

run "configuration" {
  command = plan

  assert {
    condition     = kubernetes_config_map_v1.config.data["mosquitto.conf"] == file("../mosquitto.conf") && kubernetes_config_map_v1.config.data["acl"] == file("../acl")
    error_message = "The ConfigMap must hold the files from the app folder."
  }

  assert {
    condition     = strcontains(kubernetes_config_map_v1.config.data["mosquitto.conf"], "allow_anonymous false")
    error_message = "Each client must log in."
  }

  assert {
    condition     = !strcontains(kubernetes_config_map_v1.config.data["acl"], "topic write") && !strcontains(kubernetes_config_map_v1.config.data["acl"], "rpconnect\ntopic readwrite")
    error_message = "The bridge user must only read."
  }
}

run "pod" {
  command = plan

  assert {
    condition     = kubernetes_deployment_v1.mosquitto.spec[0].strategy[0].type == "Recreate"
    error_message = "Only one Mosquitto pod can use the queue volume."
  }

  assert {
    condition     = kubernetes_deployment_v1.mosquitto.spec[0].template[0].spec[0].init_container[0].env_from[0].secret_ref[0].name == "mosquitto-env"
    error_message = "The init container must read the passwords from the given Secret."
  }

  assert {
    condition     = strcontains(kubernetes_deployment_v1.mosquitto.spec[0].template[0].spec[0].init_container[0].command[2], "mosquitto_passwd -b /mosquitto/auth/passwd rpconnect \"$MQTT_BRIDGE_PASSWORD\"")
    error_message = "The password file must hold the bridge user."
  }

  assert {
    condition     = strcontains(kubernetes_deployment_v1.mosquitto.spec[0].template[0].spec[0].init_container[0].command[2], "chown 1883:1883 /mosquitto/auth/passwd /mosquitto/auth/acl")
    error_message = "Mosquitto must own the password file and the ACL."
  }

  assert {
    condition     = length(kubernetes_deployment_v1.mosquitto.spec[0].template[0].metadata[0].annotations["checksum/config"]) == 64
    error_message = "A configuration change must restart the pod."
  }

  assert {
    condition     = kubernetes_persistent_volume_claim_v1.data.spec[0].storage_class_name == "ssd" && kubernetes_persistent_volume_claim_v1.data.spec[0].resources[0].requests.storage == "1Gi"
    error_message = "The queue volume must be 1 GiB on the SSD class."
  }

  assert {
    condition     = kubernetes_service_v1.mosquitto.spec[0].port[0].port == 1883 && kubernetes_service_v1.mosquitto.metadata[0].name == "mosquitto"
    error_message = "Other apps reach Mosquitto at mosquitto:1883."
  }
}
