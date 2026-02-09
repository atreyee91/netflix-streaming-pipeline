// ============================================================================
// Event Hubs Module – Netflix Streaming Pipeline
// ============================================================================

@description('Base name for resource naming')
param baseName string

@description('Azure region')
param location string

@description('SKU for Event Hubs namespace')
@allowed(['Basic', 'Standard', 'Premium'])
param sku string = 'Standard'

@description('Throughput units')
param capacity int = 2

@description('Partition count for the streaming hub')
param partitionCount int = 4

@description('Message retention in days')
param messageRetention int = 7

param tags object = {}

// ── Namespace ───────────────────────────────────────────────────────────────

resource namespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: 'evhns-${baseName}'
  location: location
  tags: tags
  sku: {
    name: sku
    tier: sku
    capacity: capacity
  }
  properties: {
    isAutoInflateEnabled: sku == 'Standard'
    maximumThroughputUnits: sku == 'Standard' ? 10 : 0
  }
}

// ── Main Event Hub ──────────────────────────────────────────────────────────

resource streamingHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: namespace
  name: 'netflix-events'
  properties: {
    partitionCount: partitionCount
    messageRetentionInDays: messageRetention
  }
}

// ── Dead Letter Hub ─────────────────────────────────────────────────────────

resource dlqHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: namespace
  name: 'netflix-events-dlq'
  properties: {
    partitionCount: 2
    messageRetentionInDays: 7
  }
}

// ── Consumer Groups ─────────────────────────────────────────────────────────

resource cgStreamAnalytics 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: streamingHub
  name: 'cg-stream-analytics'
}

resource cgFunctions 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: streamingHub
  name: 'cg-azure-functions'
}

// ── Authorization Rules ─────────────────────────────────────────────────────

resource sendRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2024-01-01' = {
  parent: streamingHub
  name: 'send-policy'
  properties: {
    rights: ['Send']
  }
}

resource listenRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2024-01-01' = {
  parent: streamingHub
  name: 'listen-policy'
  properties: {
    rights: ['Listen']
  }
}

// ── Outputs ─────────────────────────────────────────────────────────────────

output namespaceId string = namespace.id
output namespaceName string = namespace.name
output hubName string = streamingHub.name
output sendConnectionString string = sendRule.listKeys().primaryConnectionString
output listenConnectionString string = listenRule.listKeys().primaryConnectionString
