variable "region" {
  type    = string
  default = "us-east-1"
}

variable "lake_force_destroy" {
  description = "Allow `terraform destroy` to delete the lake bucket even if it still has data."
  type        = bool
  default     = false
}

variable "athena_results_retention_days" {
  description = "Athena query results are disposable; expire them to keep storage near zero."
  type        = number
  default     = 7
}
