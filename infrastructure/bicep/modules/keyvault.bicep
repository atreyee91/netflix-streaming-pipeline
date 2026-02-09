// ============================================================================
// Key Vault Module – Netflix Streaming Pipeline
// ============================================================================

param baseName string
param location string
param managedIdentityPrincipalId string
@secure()
param eventHubConnectionString string
@secure()
param cosmosConnectionString string
param tags object = {}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: 'kv-${baseName}'
  location: location
  tags: tags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    accessPolicies: [
      {
        tenantId: subscription().tenantId
        objectId: managedIdentityPrincipalId
        permissions: {
          secrets: ['get', 'list']
        }
      }
    ]
  }
}

resource ehSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'eventhub-connection-string'
  properties: {
    value: eventHubConnectionString
  }
}

resource cosmosSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'cosmos-connection-string'
  properties: {
    value: cosmosConnectionString
  }
}

output vaultUri string = keyVault.properties.vaultUri
output vaultName string = keyVault.name
