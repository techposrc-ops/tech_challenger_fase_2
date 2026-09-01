terraform {
  required_version = ">= 1.7.0"

  backend "gcs" {
    bucket = "alfabetizacao-tfstate-429919068824"
    prefix = "terraform/state"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}
