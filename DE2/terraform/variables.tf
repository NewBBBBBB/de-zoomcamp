variable "project_id" {
  type        = string
  description = "GCP project id"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "location" {
  type        = string
  default     = "US"
  description = "Multi-region for GCS & BQ"
}

variable "prefix" {
  type        = string
  default     = "my-speed"
  description = "Resource name prefix"
}
