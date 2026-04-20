locals {
  apis = [
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each                   = toset(local.apis)
  project                    = var.project_id
  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
}

# ---------- Data Lake (GCS) ----------
resource "google_storage_bucket" "lake" {
  name                        = "${var.prefix}-lake-${var.project_id}"
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 90 }
    action { type = "Delete" }
  }

  depends_on = [google_project_service.apis]
}

# ---------- Data Warehouse (BigQuery) ----------
resource "google_bigquery_dataset" "raw" {
  dataset_id    = "speed_raw"
  friendly_name = "Ookla raw external tables"
  location      = var.location
  depends_on    = [google_project_service.apis]
}

resource "google_bigquery_dataset" "marts" {
  dataset_id    = "speed_marts"
  friendly_name = "dbt marts"
  location      = var.location
  depends_on    = [google_project_service.apis]
}

# ---------- Service Account for Airflow/dbt ----------
resource "google_service_account" "runner" {
  account_id   = "${var.prefix}-runner"
  display_name = "Malaysia Speed pipeline runner"
}

resource "google_project_iam_member" "runner_roles" {
  for_each = toset([
    "roles/storage.admin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.runner.email}"
}

resource "google_service_account_key" "runner_key" {
  service_account_id = google_service_account.runner.name
}

resource "local_sensitive_file" "sa_key" {
  content         = base64decode(google_service_account_key.runner_key.private_key)
  filename        = "${path.module}/../credentials/sa-key.json"
  file_permission = "0600"
}
