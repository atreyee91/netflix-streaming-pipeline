##############################################################################
# Outputs – Netflix Streaming Pipeline
##############################################################################

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.main.name
}

output "eventhub_name" {
  value = azurerm_eventhub.streaming_events.name
}

output "eventhub_send_connection_string" {
  value     = azurerm_eventhub_authorization_rule.send.primary_connection_string
  sensitive = true
}

output "cosmosdb_endpoint" {
  value = azurerm_cosmosdb_account.main.endpoint
}

output "cosmosdb_primary_key" {
  value     = azurerm_cosmosdb_account.main.primary_key
  sensitive = true
}

output "datalake_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "datalake_primary_endpoint" {
  value = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "stream_analytics_job_name" {
  value = azurerm_stream_analytics_job.main.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

output "app_insights_instrumentation_key" {
  value     = azurerm_application_insights.main.instrumentation_key
  sensitive = true
}

output "app_insights_connection_string" {
  value     = azurerm_application_insights.main.connection_string
  sensitive = true
}

output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.main.id
}

output "managed_identity_client_id" {
  value = azurerm_user_assigned_identity.pipeline.client_id
}
