##############################################################################
# Variables for Netflix Streaming Pipeline infrastructure
##############################################################################

variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "rg-netflix-streaming"
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "eastus2"
}

variable "environment" {
  description = "Environment tag (dev, staging, prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "nflxstream"
}

# ── Event Hubs ───────────────────────────────────────────────────────────────

variable "eventhub_sku" {
  description = "Event Hubs namespace SKU (Basic, Standard, Premium)"
  type        = string
  default     = "Standard"
}

variable "eventhub_capacity" {
  description = "Throughput units for Event Hubs namespace"
  type        = number
  default     = 2
}

variable "eventhub_partition_count" {
  description = "Number of partitions for the Event Hub"
  type        = number
  default     = 4
}

variable "eventhub_message_retention" {
  description = "Message retention in days"
  type        = number
  default     = 7
}

# ── Cosmos DB ────────────────────────────────────────────────────────────────

variable "cosmosdb_offer_type" {
  description = "Cosmos DB offer type"
  type        = string
  default     = "Standard"
}

variable "cosmosdb_consistency_level" {
  description = "Default consistency level for Cosmos DB"
  type        = string
  default     = "Session"
}

variable "cosmosdb_throughput" {
  description = "Provisioned throughput (RU/s) for Cosmos DB containers"
  type        = number
  default     = 400
}

# ── Data Lake ────────────────────────────────────────────────────────────────

variable "datalake_replication" {
  description = "Storage account replication type"
  type        = string
  default     = "LRS"
}

variable "datalake_tier" {
  description = "Storage account tier"
  type        = string
  default     = "Standard"
}

# ── Stream Analytics ─────────────────────────────────────────────────────────

variable "stream_analytics_su" {
  description = "Streaming Units for Stream Analytics job"
  type        = number
  default     = 3
}

# ── Tags ─────────────────────────────────────────────────────────────────────

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    project     = "netflix-streaming-pipeline"
    managed_by  = "terraform"
  }
}
