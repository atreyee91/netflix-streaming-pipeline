##############################################################################
# Netflix Real-Time Streaming Pipeline – Azure Infrastructure (Terraform)
#
# Resources:
#   - Resource Group
#   - Event Hubs Namespace + Hub + Consumer Groups
#   - Stream Analytics Job (inputs, outputs, transformation)
#   - Cosmos DB Account + Database + Containers
#   - Data Lake Storage Gen2 + File Systems
#   - Key Vault (secrets)
#   - Log Analytics Workspace + Application Insights
#   - Managed Identities & RBAC
##############################################################################

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
}

data "azurerm_client_config" "current" {}

locals {
  base_name = "${var.project_name}-${var.environment}"
  all_tags  = merge(var.tags, { environment = var.environment })
}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Resource Group                                                         ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.all_tags
}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Managed Identity                                                       ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

resource "azurerm_user_assigned_identity" "pipeline" {
  name                = "id-${local.base_name}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.all_tags
}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Event Hubs                                                             ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-${local.base_name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = var.eventhub_sku
  capacity            = var.eventhub_capacity
  tags                = local.all_tags
}

resource "azurerm_eventhub" "streaming_events" {
  name                = "netflix-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = var.eventhub_partition_count
  message_retention   = var.eventhub_message_retention
}

resource "azurerm_eventhub" "dead_letter" {
  name                = "netflix-events-dlq"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 2
  message_retention   = 7
}

resource "azurerm_eventhub_consumer_group" "stream_analytics" {
  name                = "cg-stream-analytics"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.streaming_events.name
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_eventhub_consumer_group" "azure_functions" {
  name                = "cg-azure-functions"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.streaming_events.name
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_eventhub_authorization_rule" "send" {
  name                = "send-policy"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.streaming_events.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = false
  send                = true
  manage              = false
}

resource "azurerm_eventhub_authorization_rule" "listen" {
  name                = "listen-policy"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.streaming_events.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = true
  send                = false
  manage              = false
}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Data Lake Storage Gen2                                                 ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

resource "azurerm_storage_account" "datalake" {
  name                     = "dl${replace(local.base_name, "-", "")}01"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = var.datalake_tier
  account_replication_type = var.datalake_replication
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Enable hierarchical namespace for ADLS Gen2
  tags                     = local.all_tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "processed" {
  name               = "processed"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "curated" {
  name               = "curated"
  storage_account_id = azurerm_storage_account.datalake.id
}

# RBAC: give managed identity Blob Data Contributor on the storage account
resource "azurerm_role_assignment" "datalake_contributor" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.pipeline.principal_id
}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Cosmos DB                                                              ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

resource "azurerm_cosmosdb_account" "main" {
  name                = "cosmos-${local.base_name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  offer_type          = var.cosmosdb_offer_type
  kind                = "GlobalDocumentDB"
  tags                = local.all_tags

  consistency_policy {
    consistency_level = var.cosmosdb_consistency_level
  }

  geo_location {
    location          = azurerm_resource_group.main.location
    failover_priority = 0
  }
}

resource "azurerm_cosmosdb_sql_database" "streaming" {
  name                = "netflix-streaming"
  resource_group_name = azurerm_resource_group.main.name
  account_name        = azurerm_cosmosdb_account.main.name
}

resource "azurerm_cosmosdb_sql_container" "events" {
  name                  = "processed-events"
  resource_group_name   = azurerm_resource_group.main.name
  account_name          = azurerm_cosmosdb_account.main.name
  database_name         = azurerm_cosmosdb_sql_database.streaming.name
  partition_key_paths   = ["/content_id"]
  throughput            = var.cosmosdb_throughput

  indexing_policy {
    indexing_mode = "consistent"

    included_path {
      path = "/event_type/?"
    }
    included_path {
      path = "/timestamp/?"
    }
    included_path {
      path = "/user_id/?"
    }
    included_path {
      path = "/device_type/?"
    }
    excluded_path {
      path = "/*"
    }
  }
}

resource "azurerm_cosmosdb_sql_container" "aggregations" {
  name                  = "aggregations"
  resource_group_name   = azurerm_resource_group.main.name
  account_name          = azurerm_cosmosdb_account.main.name
  database_name         = azurerm_cosmosdb_sql_database.streaming.name
  partition_key_paths   = ["/aggregation_type"]
  throughput            = var.cosmosdb_throughput
  default_ttl           = 86400 # 24 hours

  indexing_policy {
    indexing_mode = "consistent"
    included_path {
      path = "/window_end/?"
    }
    excluded_path {
      path = "/*"
    }
  }
}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Stream Analytics                                                       ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

resource "azurerm_stream_analytics_job" "main" {
  name                                     = "asa-${local.base_name}"
  resource_group_name                      = azurerm_resource_group.main.name
  location                                 = azurerm_resource_group.main.location
  streaming_units                          = var.stream_analytics_su
  compatibility_level                      = "1.2"
  data_locale                              = "en-US"
  events_late_arrival_max_delay_in_seconds = 60
  events_out_of_order_max_delay_in_seconds = 10
  events_out_of_order_policy               = "Adjust"
  output_error_policy                      = "Drop"
  tags                                     = local.all_tags

  transformation_query = <<-QUERY
    -- Real-time viewer count by content (tumbling window 10s)
    SELECT
        content_id,
        content_title,
        COUNT(DISTINCT user_id) AS active_viewers,
        System.Timestamp()      AS window_end
    INTO [cosmos-viewer-count]
    FROM [eventhub-input]
    WHERE event_type = 'video_start'
    GROUP BY content_id, content_title, TumblingWindow(second, 10)

    -- Average watch time per session (sliding window 5min)
    SELECT
        content_id,
        content_title,
        AVG(duration_seconds)   AS avg_watch_seconds,
        COUNT(*)                AS session_count,
        System.Timestamp()      AS window_end
    INTO [cosmos-watch-time]
    FROM [eventhub-input]
    WHERE event_type IN ('video_stop', 'video_complete')
    GROUP BY content_id, content_title, SlidingWindow(minute, 5)

    -- Top trending content (hopping window 5min, hop 1min)
    SELECT
        content_id,
        content_title,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_viewers,
        System.Timestamp()      AS window_end
    INTO [cosmos-trending]
    FROM [eventhub-input]
    WHERE event_type = 'video_start'
    GROUP BY content_id, content_title, HoppingWindow(minute, 5, 1)

    -- Archive all raw events to Data Lake
    SELECT *
    INTO [datalake-raw]
    FROM [eventhub-input]
  QUERY
}

resource "azurerm_stream_analytics_stream_input_eventhub_v2" "input" {
  name                      = "eventhub-input"
  stream_analytics_job_id   = azurerm_stream_analytics_job.main.id
  eventhub_consumer_group_name = azurerm_eventhub_consumer_group.stream_analytics.name
  eventhub_name             = azurerm_eventhub.streaming_events.name
  servicebus_namespace      = azurerm_eventhub_namespace.main.name
  shared_access_policy_key  = azurerm_eventhub_authorization_rule.listen.primary_key
  shared_access_policy_name = azurerm_eventhub_authorization_rule.listen.name

  serialization {
    type     = "Json"
    encoding = "UTF8"
  }
}

resource "azurerm_stream_analytics_output_blob" "datalake_raw" {
  name                      = "datalake-raw"
  stream_analytics_job_name = azurerm_stream_analytics_job.main.name
  resource_group_name       = azurerm_resource_group.main.name
  storage_account_name      = azurerm_storage_account.datalake.name
  storage_account_key       = azurerm_storage_account.datalake.primary_access_key
  storage_container_name    = azurerm_storage_data_lake_gen2_filesystem.raw.name
  path_pattern              = "netflix-events/{date}/{time}"
  date_format               = "yyyy-MM-dd"
  time_format               = "HH"

  serialization {
    type            = "Json"
    encoding        = "UTF8"
    format          = "LineSeparated"
  }
}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Key Vault                                                              ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

resource "azurerm_key_vault" "main" {
  name                       = "kv-${local.base_name}"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false
  tags                       = local.all_tags

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "Set", "List", "Delete"]
  }

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azurerm_user_assigned_identity.pipeline.principal_id
    secret_permissions = ["Get", "List"]
  }
}

resource "azurerm_key_vault_secret" "eventhub_connection" {
  name         = "eventhub-connection-string"
  value        = azurerm_eventhub_namespace.main.default_primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "cosmos_connection" {
  name         = "cosmos-connection-string"
  value        = azurerm_cosmosdb_account.main.primary_sql_connection_string
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "datalake_key" {
  name         = "datalake-access-key"
  value        = azurerm_storage_account.datalake.primary_access_key
  key_vault_id = azurerm_key_vault.main.id
}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Log Analytics & Application Insights                                   ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

resource "azurerm_log_analytics_workspace" "main" {
  name                = "log-${local.base_name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.all_tags
}

resource "azurerm_application_insights" "main" {
  name                = "appi-${local.base_name}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "other"
  tags                = local.all_tags
}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  Diagnostic Settings                                                    ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

resource "azurerm_monitor_diagnostic_setting" "eventhub" {
  name                       = "diag-eventhub"
  target_resource_id         = azurerm_eventhub_namespace.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "ArchiveLogs"
  }
  enabled_log {
    category = "OperationalLogs"
  }
  metric {
    category = "AllMetrics"
  }
}
