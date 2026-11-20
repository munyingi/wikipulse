variable "environment" {
  description = "Deployment stage. Each stage gets its own workspace."
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "capacity_id" {
  description = "Fabric capacity ID (the trial capacity works). Admin portal > Capacity settings."
  type        = string
}

variable "name_prefix" {
  type    = string
  default = "wikipulse"
}

variable "workspace_roles" {
  description = "Extra principals and their workspace role, keyed by a label."
  type = map(object({
    id   = string
    type = string # User, Group, ServicePrincipal
    role = string # Admin, Member, Contributor, Viewer
  }))
  default = {}
}

variable "key_vault_uri" {
  description = "Azure Key Vault holding the Eventstream connection string for the stream producer notebook. Optional."
  type        = string
  default     = ""
}

variable "enable_airflow_job" {
  description = "Create the Apache Airflow job (needs the tenant setting 'Users can create and use Apache Airflow jobs')."
  type        = bool
  default     = true
}

variable "airflow_version" {
  type    = string
  default = "2.10.5"
}

variable "alert_webhook_url" {
  description = "Teams or Slack incoming webhook for pipeline alerts. Optional."
  type        = string
  default     = ""
  sensitive   = true
}
