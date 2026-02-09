// ============================================================================
// Cosmos DB Module – Netflix Streaming Pipeline
// ============================================================================

@description('Base name for resource naming')
param baseName string

@description('Azure region')
param location string

@description('Consistency level')
@allowed(['Eventual', 'Session', 'BoundedStaleness', 'Strong', 'ConsistentPrefix'])
param consistencyLevel string = 'Session'

@description('Throughput (RU/s) for containers')
param throughput int = 400

param tags object = {}

// ── Account ─────────────────────────────────────────────────────────────────

resource account 'Microsoft.DocumentDB/databaseAccounts@2024-02-15-preview' = {
  name: 'cosmos-${baseName}'
  location: location
  tags: tags
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    consistencyPolicy: {
      defaultConsistencyLevel: consistencyLevel
    }
    locations: [
      {
        locationName: location
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
    enableAnalyticalStorage: true
  }
}

// ── Database ────────────────────────────────────────────────────────────────

resource database 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2024-02-15-preview' = {
  parent: account
  name: 'netflix-streaming'
  properties: {
    resource: {
      id: 'netflix-streaming'
    }
  }
}

// ── Processed Events Container ──────────────────────────────────────────────

resource eventsContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-02-15-preview' = {
  parent: database
  name: 'processed-events'
  properties: {
    resource: {
      id: 'processed-events'
      partitionKey: {
        paths: ['/content_id']
        kind: 'Hash'
      }
      indexingPolicy: {
        indexingMode: 'consistent'
        includedPaths: [
          { path: '/event_type/?' }
          { path: '/timestamp/?' }
          { path: '/user_id/?' }
          { path: '/device_type/?' }
        ]
        excludedPaths: [
          { path: '/*' }
        ]
      }
    }
    options: {
      throughput: throughput
    }
  }
}

// ── Aggregations Container ──────────────────────────────────────────────────

resource aggregationsContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-02-15-preview' = {
  parent: database
  name: 'aggregations'
  properties: {
    resource: {
      id: 'aggregations'
      partitionKey: {
        paths: ['/aggregation_type']
        kind: 'Hash'
      }
      defaultTtl: 86400
      indexingPolicy: {
        indexingMode: 'consistent'
        includedPaths: [
          { path: '/window_end/?' }
        ]
        excludedPaths: [
          { path: '/*' }
        ]
      }
    }
    options: {
      throughput: throughput
    }
  }
}

// ── Outputs ─────────────────────────────────────────────────────────────────

output accountId string = account.id
output accountName string = account.name
output endpoint string = account.properties.documentEndpoint
output primaryKey string = account.listKeys().primaryMasterKey
output connectionString string = account.listConnectionStrings().connectionStrings[0].connectionString
