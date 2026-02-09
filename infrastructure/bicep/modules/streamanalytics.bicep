// ============================================================================
// Stream Analytics Module – Netflix Streaming Pipeline
// ============================================================================

@description('Base name for resource naming')
param baseName string

@description('Azure region')
param location string

@description('Streaming units for the job')
param streamingUnits int = 3

@description('Event Hub namespace name')
param eventHubNamespace string

@description('Event Hub name')
param eventHubName string

@description('Event Hub listen policy connection string')
@secure()
param eventHubListenConnectionString string

@description('Data Lake storage account name')
param dataLakeAccountName string

@description('Data Lake storage account key')
@secure()
param dataLakeAccountKey string

param tags object = {}

// ── Stream Analytics Job ────────────────────────────────────────────────────

resource job 'Microsoft.StreamAnalytics/streamingjobs@2021-10-01-preview' = {
  name: 'asa-${baseName}'
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'Standard'
    }
    eventsOutOfOrderPolicy: 'Adjust'
    outputErrorPolicy: 'Drop'
    eventsOutOfOrderMaxDelayInSeconds: 10
    eventsLateArrivalMaxDelayInSeconds: 60
    dataLocale: 'en-US'
    compatibilityLevel: '1.2'
    transformation: {
      name: 'main-transformation'
      properties: {
        streamingUnits: streamingUnits
        query: loadTextContent('../queries/combined_query.asaql')
      }
    }
  }
}

// ── Input: Event Hub ────────────────────────────────────────────────────────

resource input 'Microsoft.StreamAnalytics/streamingjobs/inputs@2021-10-01-preview' = {
  parent: job
  name: 'eventhub-input'
  properties: {
    type: 'Stream'
    datasource: {
      type: 'Microsoft.EventHub/EventHub'
      properties: {
        serviceBusNamespace: eventHubNamespace
        eventHubName: eventHubName
        sharedAccessPolicyName: 'listen-policy'
        sharedAccessPolicyKey: last(split(eventHubListenConnectionString, ';'))
        consumerGroupName: 'cg-stream-analytics'
      }
    }
    serialization: {
      type: 'Json'
      properties: {
        encoding: 'UTF8'
      }
    }
  }
}

// ── Output: Data Lake Raw ───────────────────────────────────────────────────

resource outputDatalake 'Microsoft.StreamAnalytics/streamingjobs/outputs@2021-10-01-preview' = {
  parent: job
  name: 'datalake-raw'
  properties: {
    datasource: {
      type: 'Microsoft.Storage/Blob'
      properties: {
        storageAccounts: [
          {
            accountName: dataLakeAccountName
            accountKey: dataLakeAccountKey
          }
        ]
        container: 'raw'
        pathPattern: 'netflix-events/{date}/{time}'
        dateFormat: 'yyyy-MM-dd'
        timeFormat: 'HH'
      }
    }
    serialization: {
      type: 'Json'
      properties: {
        encoding: 'UTF8'
        format: 'LineSeparated'
      }
    }
  }
}

// ── Outputs ─────────────────────────────────────────────────────────────────

output jobId string = job.id
output jobName string = job.name
