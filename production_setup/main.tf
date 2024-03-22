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

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "dbt_dez_capstone_prod"
  delete_contents_on_destroy = true
  project    = "drux-de-zoomcamp"
  location   = "EU"
}