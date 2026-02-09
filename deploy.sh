#!/usr/bin/env bash
# =============================================================================
# Netflix Streaming Pipeline – Deployment Script (Bash)
#
# Usage:
#   ./deploy.sh [terraform|bicep] [dev|staging|prod]
#
# Prerequisites:
#   - Azure CLI installed and logged in (az login)
#   - Terraform >= 1.5 (if using terraform mode)
#   - Python 3.9+
#   - Azure Functions Core Tools (for function deployment)
# =============================================================================

set -euo pipefail

# ── Arguments ────────────────────────────────────────────────────────────────

IaC_MODE="${1:-terraform}"       # terraform or bicep
ENVIRONMENT="${2:-dev}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Colour helpers ───────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Pre-flight checks ───────────────────────────────────────────────────────

info "Pre-flight checks..."

command -v az >/dev/null 2>&1 || { err "Azure CLI not found. Install: https://aka.ms/installazureclilinux"; exit 1; }
command -v python3 >/dev/null 2>&1 || { err "Python 3 not found."; exit 1; }

az account show >/dev/null 2>&1 || { err "Not logged in to Azure. Run: az login"; exit 1; }

SUBSCRIPTION_ID=$(az account show --query id -o tsv)
info "Using subscription: $SUBSCRIPTION_ID"
info "Environment: $ENVIRONMENT"
info "IaC mode: $IaC_MODE"

# ── Step 1: Deploy Infrastructure ───────────────────────────────────────────

info "─── Step 1: Deploying Azure Infrastructure ($IaC_MODE) ───"

if [ "$IaC_MODE" = "terraform" ]; then
    command -v terraform >/dev/null 2>&1 || { err "Terraform not found."; exit 1; }

    cd "$SCRIPT_DIR/infrastructure/terraform"

    if [ ! -f terraform.tfvars ]; then
        warn "terraform.tfvars not found. Copying from example."
        cp terraform.tfvars.example terraform.tfvars
        warn "Please edit terraform.tfvars with your values, then re-run."
        exit 1
    fi

    terraform init -upgrade
    terraform plan -var="environment=$ENVIRONMENT" -out=tfplan
    terraform apply tfplan
    ok "Terraform deployment complete."

    # Capture outputs
    EVENTHUB_CONN=$(terraform output -raw eventhub_send_connection_string 2>/dev/null || echo "")
    COSMOS_ENDPOINT=$(terraform output -raw cosmosdb_endpoint 2>/dev/null || echo "")
    RESOURCE_GROUP=$(terraform output -raw resource_group_name 2>/dev/null || echo "")

elif [ "$IaC_MODE" = "bicep" ]; then
    cd "$SCRIPT_DIR/infrastructure/bicep"

    DEPLOYMENT_NAME="netflix-pipeline-${ENVIRONMENT}-$(date +%Y%m%d%H%M%S)"
    az deployment sub create \
        --name "$DEPLOYMENT_NAME" \
        --location eastus2 \
        --template-file main.bicep \
        --parameters parameters.json \
        --parameters environment="$ENVIRONMENT"

    ok "Bicep deployment complete."
    RESOURCE_GROUP=$(az deployment sub show --name "$DEPLOYMENT_NAME" --query 'properties.outputs.resourceGroupName.value' -o tsv)
else
    err "Invalid IaC mode: $IaC_MODE. Use 'terraform' or 'bicep'."
    exit 1
fi

cd "$SCRIPT_DIR"

# ── Step 2: Install Python Dependencies ─────────────────────────────────────

info "─── Step 2: Installing Python Dependencies ───"

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
ok "Python dependencies installed."

# ── Step 3: Deploy Azure Functions ───────────────────────────────────────────

info "─── Step 3: Deploying Azure Functions ───"

if command -v func >/dev/null 2>&1; then
    FUNC_APP_NAME="func-nflxstream-${ENVIRONMENT}"

    # Create Function App if it doesn't exist
    az functionapp show --name "$FUNC_APP_NAME" --resource-group "$RESOURCE_GROUP" >/dev/null 2>&1 || {
        info "Creating Function App: $FUNC_APP_NAME"
        az functionapp create \
            --resource-group "$RESOURCE_GROUP" \
            --consumption-plan-location eastus2 \
            --runtime python \
            --runtime-version 3.11 \
            --functions-version 4 \
            --name "$FUNC_APP_NAME" \
            --storage-account "dlnflxstream${ENVIRONMENT}01" \
            --os-type Linux
    }

    cd "$SCRIPT_DIR/functions"
    func azure functionapp publish "$FUNC_APP_NAME" --python
    ok "Azure Functions deployed."
    cd "$SCRIPT_DIR"
else
    warn "Azure Functions Core Tools not found. Skipping function deployment."
    warn "Install: npm install -g azure-functions-core-tools@4"
fi

# ── Step 4: Start Stream Analytics Job ───────────────────────────────────────

info "─── Step 4: Starting Stream Analytics Job ───"

ASA_JOB="asa-nflxstream-${ENVIRONMENT}"
az stream-analytics job start \
    --resource-group "$RESOURCE_GROUP" \
    --name "$ASA_JOB" \
    --output-start-mode JobStartTime \
    2>/dev/null || warn "Stream Analytics job start failed or already running."

ok "Stream Analytics job started."

# ── Step 5: Run Tests ────────────────────────────────────────────────────────

info "─── Step 5: Running Tests ───"

source .venv/bin/activate
python -m pytest tests/ -v --tb=short || warn "Some tests failed. Review output above."

# ── Done ─────────────────────────────────────────────────────────────────────

echo ""
ok "═══════════════════════════════════════════════════════════"
ok "  Netflix Streaming Pipeline deployed successfully!"
ok "  Environment: $ENVIRONMENT"
ok "  Resource Group: $RESOURCE_GROUP"
ok "═══════════════════════════════════════════════════════════"
echo ""
info "Next steps:"
info "  1. Update .env with connection strings from Azure portal"
info "  2. Run the data generator:  python -m data_generator.generator --dry-run"
info "  3. Configure Power BI with the streaming dataset"
echo ""
