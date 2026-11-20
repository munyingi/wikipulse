terraform {
  required_version = ">= 1.8, < 2.0"

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.0"
    }
  }

  # Local state by default. The deploy workflow writes an azurerm backend.tf so CI keeps state in Azure Storage.
}

# Authentication comes from the environment:
#   local:  az login, then FABRIC_USE_CLI=true
#   CI:     FABRIC_TENANT_ID, FABRIC_CLIENT_ID and FABRIC_USE_OIDC=true (GitHub OIDC, no secret stored)
provider "fabric" {
  preview = true # Apache Airflow job and some definition formats are still marked preview in the provider
}
