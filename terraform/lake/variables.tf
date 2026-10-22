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

variable "pipeline_allowed_cidrs" {
  description = "Public CIDRs the pipeline IAM user may call S3 from (e.g. the VM's IP as /32). Empty = no IP restriction."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for c in var.pipeline_allowed_cidrs : can(cidrhost(c, 0))])
    error_message = "Each entry must be a valid CIDR, e.g. 203.0.113.10/32."
  }
}
