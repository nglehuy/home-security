terraform {
  required_version = "~> 1.16"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 3.3"
    }
  }

  # The state holds the ci-deployer token, which has cluster-admin rights. So
  # the state stays on the admin machine, in .terraform/, which .gitignore
  # excludes. Terraform keeps its backend record in .terraform/terraform.tfstate,
  # so the state uses another file name. The deploy workflow never applies this
  # root.
  backend "local" {
    path = ".terraform/local.tfstate"
  }
}
