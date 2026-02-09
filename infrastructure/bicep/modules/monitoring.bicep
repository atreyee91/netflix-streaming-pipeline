// ============================================================================
// Monitoring Module – Netflix Streaming Pipeline
// ============================================================================

@description('Base name for resource naming')
param baseName string

@description('Azure region')
param location string

@description('Event Hub namespace resource ID for diagnostics')
param eventHubNamespaceId string = ''

param tags object = {}

// ── Log Analytics Workspace ─────────────────────────────────────────────────

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: 'log-${baseName}'
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

// ── Application Insights ────────────────────────────────────────────────────

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: 'appi-${baseName}'
  location: location
  tags: tags
  kind: 'other'
  properties: {
    Application_Type: 'other'
    WorkspaceResourceId: logAnalytics.id
  }
}

// ── Diagnostic Settings for Event Hub ───────────────────────────────────────

resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (!empty(eventHubNamespaceId)) {
  name: 'diag-eventhub'
  scope: resourceId('Microsoft.EventHub/namespaces', last(split(eventHubNamespaceId, '/')))
  properties: {
    workspaceId: logAnalytics.id
    logs: [
      {
        category: 'ArchiveLogs'
        enabled: true
      }
      {
        category: 'OperationalLogs'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
  }
}

// ── Alert: High Event Hub Throttling ────────────────────────────────────────

resource throttleAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = if (!empty(eventHubNamespaceId)) {
  name: 'alert-eventhub-throttle-${baseName}'
  location: 'global'
  tags: tags
  properties: {
    severity: 2
    enabled: true
    scopes: [eventHubNamespaceId]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          name: 'ThrottledRequests'
          metricName: 'ThrottledRequests'
          operator: 'GreaterThan'
          threshold: 10
          timeAggregation: 'Total'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
    }
  }
}

// ── Outputs ─────────────────────────────────────────────────────────────────

output logAnalyticsId string = logAnalytics.id
output logAnalyticsWorkspaceId string = logAnalytics.properties.customerId
output appInsightsId string = appInsights.id
output appInsightsInstrumentationKey string = appInsights.properties.InstrumentationKey
output appInsightsConnectionString string = appInsights.properties.ConnectionString
