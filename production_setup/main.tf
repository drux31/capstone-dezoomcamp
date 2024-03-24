terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  # Credentials only needs to be set if you do not have 
  # the GOOGLE_APPLICATION_CREDENTIALS set
  #  credentials = 
  project = "drux-de-zoomcamp"
}

resource "google_storage_bucket" "data-lake-bucket" {
  name     = "dez-capstone-bucket"
  location = "EU"

  # Optional, but recommended settings:
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90 // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = "dbt_dez_capstone_prod"
  delete_contents_on_destroy = true
  project                    = "drux-de-zoomcamp"
  location                   = "EU"
}

resource "google_bigquery_dataset" "dataset-staging" {
  dataset_id                 = "staging"
  delete_contents_on_destroy = true
  project                    = "drux-de-zoomcamp"
  location                   = "EU"
}