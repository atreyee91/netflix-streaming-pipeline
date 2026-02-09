// ============================================================================
// Netflix Real-Time Streaming Pipeline – Main Bicep Template
//
// Deploys: Event Hubs, Stream Analytics, Cosmos DB, Data Lake Gen2,
//          Key Vault, Monitoring, Managed Identity
// ============================================================================

targetScope = 'subscription'

// ── Parameters ──────────────────────────────────────────────────────────────

@description('Azure region for all resources')
param location string = 'eastus2'

@description('Environment (dev, staging, prod)')
@allowed(['dev', 'staging', 'prod'])
param environment string = 'dev'

@description('Project name used for naming')
param projectName string = 'nflxstream'

@description('Event Hub throughput units')
param eventHubCapacity int = 2

@description('Event Hub partition count')
param eventHubPartitions int = 4

@description('Cosmos DB throughput (RU/s)')
param cosmosThroughput int = 400

@description('Stream Analytics streaming units')
param streamingUnits int = 3

// ── Variables ───────────────────────────────────────────────────────────────

var baseName = '${projectName}-${environment}'
var rgName = 'rg-netflix-streaming-${environment}'
var tags = {
  project: 'netflix-streaming-pipeline'
  environment: environment
  managed_by: 'bicep'
}

// ── Resource Group ──────────────────────────────────────────────────────────

resource rg 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: rgName
  location: location
  tags: tags
}

// ── Managed Identity ────────────────────────────────────────────────────────

module identity 'modules/identity.bicep' = {
  name: 'deploy-identity'
  scope: rg
  params: {
    baseName: baseName
    location: location
    tags: tags
  }
}

// ── Event Hubs ──────────────────────────────────────────────────────────────

module eventHubs 'modules/eventhubs.bicep' = {
  name: 'deploy-eventhubs'
  scope: rg
  params: {
    baseName: baseName
    location: location
    capacity: eventHubCapacity
    partitionCount: eventHubPartitions
    tags: tags
  }
}

// ── Data Lake Storage Gen2 ──────────────────────────────────────────────────

module dataLake 'modules/datalake.bicep' = {
  name: 'deploy-datalake'
  scope: rg
  params: {
    baseName: baseName
    location: location
    managedIdentityPrincipalId: identity.outputs.principalId
    tags: tags
  }
}

// ── Cosmos DB ───────────────────────────────────────────────────────────────

module cosmosDb 'modules/cosmosdb.bicep' = {
  name: 'deploy-cosmosdb'
  scope: rg
  params: {
    baseName: baseName
    location: location
    throughput: cosmosThroughput
    tags: tags
  }
}

// ── Monitoring ──────────────────────────────────────────────────────────────

module monitoring 'modules/monitoring.bicep' = {
  name: 'deploy-monitoring'
  scope: rg
  params: {
    baseName: baseName
    location: location
    eventHubNamespaceId: eventHubs.outputs.namespaceId
    tags: tags
  }
}

// ── Stream Analytics ────────────────────────────────────────────────────────

module streamAnalytics 'modules/streamanalytics.bicep' = {
  name: 'deploy-streamanalytics'
  scope: rg
  params: {
    baseName: baseName
    location: location
    streamingUnits: streamingUnits
    eventHubNamespace: eventHubs.outputs.namespaceName
    eventHubName: eventHubs.outputs.hubName
    eventHubListenConnectionString: eventHubs.outputs.listenConnectionString
    dataLakeAccountName: dataLake.outputs.accountName
    dataLakeAccountKey: '' // In production, use Key Vault reference
    tags: tags
  }
}

// ── Key Vault ───────────────────────────────────────────────────────────────

module keyVault 'modules/keyvault.bicep' = {
  name: 'deploy-keyvault'
  scope: rg
  params: {
    baseName: baseName
    location: location
    managedIdentityPrincipalId: identity.outputs.principalId
    eventHubConnectionString: eventHubs.outputs.sendConnectionString
    cosmosConnectionString: cosmosDb.outputs.connectionString
    tags: tags
  }
}

// ── Outputs ─────────────────────────────────────────────────────────────────

output resourceGroupName string = rg.name
output eventHubNamespace string = eventHubs.outputs.namespaceName
output cosmosDbEndpoint string = cosmosDb.outputs.endpoint
output dataLakeEndpoint string = dataLake.outputs.primaryDfsEndpoint
output appInsightsKey string = monitoring.outputs.appInsightsInstrumentationKey
