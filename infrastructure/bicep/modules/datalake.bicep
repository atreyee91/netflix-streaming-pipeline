// ============================================================================
// Data Lake Storage Gen2 Module – Netflix Streaming Pipeline
// ============================================================================

@description('Base name for resource naming')
param baseName string

@description('Azure region')
param location string

@description('Replication type')
@allowed(['LRS', 'GRS', 'ZRS', 'RAGRS'])
param replication string = 'LRS'

@description('Managed identity principal ID for RBAC')
param managedIdentityPrincipalId string = ''

param tags object = {}

// ── Storage Account with HNS (ADLS Gen2) ───────────────────────────────────

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: 'dl${replace(baseName, '-', '')}01'
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: {
    name: replication
  }
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    networkAcls: {
      defaultAction: 'Allow'
    }
  }
}

// ── Blob Service ────────────────────────────────────────────────────────────

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 7
    }
  }
}

// ── File Systems (containers) ───────────────────────────────────────────────

resource rawContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'raw'
  properties: {
    publicAccess: 'None'
  }
}

resource processedContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'processed'
  properties: {
    publicAccess: 'None'
  }
}

resource curatedContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'curated'
  properties: {
    publicAccess: 'None'
  }
}

// ── RBAC for managed identity ───────────────────────────────────────────────

var storageBlobDataContributorRole = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource roleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(managedIdentityPrincipalId)) {
  name: guid(storageAccount.id, managedIdentityPrincipalId, storageBlobDataContributorRole)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRole)
    principalId: managedIdentityPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// ── Outputs ─────────────────────────────────────────────────────────────────

output accountId string = storageAccount.id
output accountName string = storageAccount.name
output primaryDfsEndpoint string = storageAccount.properties.primaryEndpoints.dfs
output primaryBlobEndpoint string = storageAccount.properties.primaryEndpoints.blob
