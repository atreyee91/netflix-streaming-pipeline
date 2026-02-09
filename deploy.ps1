# =============================================================================
# Netflix Streaming Pipeline - Deployment Script (PowerShell)
#
# Usage:
#   .\deploy.ps1 [-IaCMode terraform|bicep] [-Environment dev|staging|prod]
#
# Prerequisites:
#   - Azure CLI installed and logged in (az login)
#   - Terraform >= 1.5 (if using terraform mode)
#   - Python 3.9+
#   - Azure Functions Core Tools (for function deployment)
# =============================================================================

param(
    [ValidateSet("terraform", "bicep")]
    [string]$IaCMode = "terraform",

    [ValidateSet("dev", "staging", "prod")]
    [string]$Environment = "dev"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# ── Helpers ──────────────────────────────────────────────────────────────────

function Write-Info  { param($msg) Write-Host "[INFO]  $msg" -ForegroundColor Blue }
function Write-Ok    { param($msg) Write-Host "[OK]    $msg" -ForegroundColor Green }
function Write-Warn  { param($msg) Write-Host "[WARN]  $msg" -ForegroundColor Yellow }
function Write-Err   { param($msg) Write-Host "[ERROR] $msg" -ForegroundColor Red }

# ── Pre-flight ───────────────────────────────────────────────────────────────

Write-Info "Pre-flight checks..."

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    Write-Err "Azure CLI not found. Install: https://aka.ms/installazurecliwindows"
    exit 1
}

try {
    $acct = az account show | ConvertFrom-Json
    Write-Info "Using subscription: $($acct.id)"
} catch {
    Write-Err "Not logged in to Azure. Run: az login"
    exit 1
}

Write-Info "Environment: $Environment"
Write-Info "IaC mode: $IaCMode"

# ── Step 1: Infrastructure ──────────────────────────────────────────────────

Write-Info "--- Step 1: Deploying Azure Infrastructure ($IaCMode) ---"

$ResourceGroup = ""

if ($IaCMode -eq "terraform") {
    if (-not (Get-Command terraform -ErrorAction SilentlyContinue)) {
        Write-Err "Terraform not found."
        exit 1
    }

    Push-Location "$ScriptDir\infrastructure\terraform"

    if (-not (Test-Path "terraform.tfvars")) {
        Write-Warn "terraform.tfvars not found. Copying from example."
        Copy-Item "terraform.tfvars.example" "terraform.tfvars"
        Write-Warn "Edit terraform.tfvars with your values, then re-run."
        Pop-Location
        exit 1
    }

    terraform init -upgrade
    terraform plan -var="environment=$Environment" -out=tfplan
    terraform apply tfplan

    $ResourceGroup = terraform output -raw resource_group_name
    Pop-Location
    Write-Ok "Terraform deployment complete."

} else {
    Push-Location "$ScriptDir\infrastructure\bicep"

    $deploymentName = "netflix-pipeline-$Environment-$(Get-Date -Format 'yyyyMMddHHmmss')"
    az deployment sub create `
        --name $deploymentName `
        --location eastus2 `
        --template-file main.bicep `
        --parameters parameters.json `
        --parameters environment=$Environment

    $ResourceGroup = (az deployment sub show --name $deploymentName --query 'properties.outputs.resourceGroupName.value' -o tsv)
    Pop-Location
    Write-Ok "Bicep deployment complete."
}

# ── Step 2: Python Dependencies ─────────────────────────────────────────────

Write-Info "--- Step 2: Installing Python Dependencies ---"

Push-Location $ScriptDir
python -m venv .venv

if ($IsWindows -or $env:OS -match "Windows") {
    & ".venv\Scripts\Activate.ps1"
} else {
    & ".venv/bin/Activate.ps1"
}

pip install --upgrade pip
pip install -r requirements.txt
Write-Ok "Python dependencies installed."

# ── Step 3: Deploy Azure Functions ───────────────────────────────────────────

Write-Info "--- Step 3: Deploying Azure Functions ---"

if (Get-Command func -ErrorAction SilentlyContinue) {
    $funcAppName = "func-nflxstream-$Environment"

    $exists = az functionapp show --name $funcAppName --resource-group $ResourceGroup 2>$null
    if (-not $exists) {
        Write-Info "Creating Function App: $funcAppName"
        az functionapp create `
            --resource-group $ResourceGroup `
            --consumption-plan-location eastus2 `
            --runtime python `
            --runtime-version 3.11 `
            --functions-version 4 `
            --name $funcAppName `
            --storage-account "dlnflxstream${Environment}01" `
            --os-type Linux
    }

    Push-Location "$ScriptDir\functions"
    func azure functionapp publish $funcAppName --python
    Pop-Location
    Write-Ok "Azure Functions deployed."
} else {
    Write-Warn "Azure Functions Core Tools not found. Skipping function deployment."
}

# ── Step 4: Start Stream Analytics ───────────────────────────────────────────

Write-Info "--- Step 4: Starting Stream Analytics Job ---"

$asaJob = "asa-nflxstream-$Environment"
try {
    az stream-analytics job start `
        --resource-group $ResourceGroup `
        --name $asaJob `
        --output-start-mode JobStartTime
    Write-Ok "Stream Analytics job started."
} catch {
    Write-Warn "Stream Analytics job start failed or already running."
}

# ── Step 5: Tests ────────────────────────────────────────────────────────────

Write-Info "--- Step 5: Running Tests ---"
python -m pytest tests/ -v --tb=short

Pop-Location

# ── Done ─────────────────────────────────────────────────────────────────────

Write-Host ""
Write-Ok "==========================================================="
Write-Ok "  Netflix Streaming Pipeline deployed successfully!"
Write-Ok "  Environment: $Environment"
Write-Ok "  Resource Group: $ResourceGroup"
Write-Ok "==========================================================="
Write-Host ""
Write-Info "Next steps:"
Write-Info "  1. Update .env with connection strings from Azure portal"
Write-Info "  2. Run the data generator:  python -m data_generator.generator --dry-run"
Write-Info "  3. Configure Power BI with the streaming dataset"
Write-Host ""
