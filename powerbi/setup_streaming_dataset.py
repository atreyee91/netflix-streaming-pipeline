"""
Power BI Streaming Dataset Setup & Data Push

Provisions a real-time streaming dataset in Power BI Service via the REST API
and pushes sample/live data rows for dashboard visualisation.

Prerequisites:
    1. Azure AD App Registration with Power BI API permissions:
       - Dataset.ReadWrite.All
       - Dashboard.ReadWrite.All
    2. Power BI Pro or Premium Per User license
    3. pip install msal requests python-dotenv

Usage:
    # Create the streaming dataset
    python setup_streaming_dataset.py --action create

    # Push sample data to the dataset
    python setup_streaming_dataset.py --action push-sample

    # Push live data from Cosmos DB
    python setup_streaming_dataset.py --action push-live --duration 60

    # List existing datasets
    python setup_streaming_dataset.py --action list

    # Delete the streaming dataset
    python setup_streaming_dataset.py --action delete --dataset-id <id>

Environment variables (or .env file):
    POWERBI_CLIENT_ID       - Azure AD app (client) ID
    POWERBI_CLIENT_SECRET   - Azure AD app client secret
    POWERBI_TENANT_ID       - Azure AD tenant ID
    POWERBI_WORKSPACE_ID    - Power BI workspace (group) ID (optional, uses "My Workspace" if omitted)
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

try:
    from msal import ConfidentialClientApplication
except ImportError:
    print("ERROR: msal package not found. Install it: pip install msal")
    sys.exit(1)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

POWER_BI_API_BASE = "https://api.powerbi.com/v1.0/myorg"
POWER_BI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
AUTHORITY_BASE = "https://login.microsoftonline.com"

DATASET_CONFIG_PATH = Path(__file__).parent / "dataset_config.json"


# ── Authentication ───────────────────────────────────────────────────────────

class PowerBIAuth:
    """Handles Azure AD authentication for Power BI REST API."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self._token_cache: Optional[dict] = None

        self.app = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"{AUTHORITY_BASE}/{self.tenant_id}",
        )

    def get_access_token(self) -> str:
        """Acquire an access token using client credentials flow."""
        result = self.app.acquire_token_for_client(scopes=POWER_BI_SCOPE)

        if "access_token" in result:
            logger.info("Access token acquired successfully.")
            return result["access_token"]

        error = result.get("error_description", result.get("error", "Unknown error"))
        raise RuntimeError(f"Failed to acquire token: {error}")

    def get_headers(self) -> dict:
        token = self.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }


# ── Power BI Client ─────────────────────────────────────────────────────────

class PowerBIClient:
    """Client for Power BI REST API operations."""

    def __init__(self, auth: PowerBIAuth, workspace_id: Optional[str] = None):
        self.auth = auth
        self.workspace_id = workspace_id
        self.base_url = (
            f"{POWER_BI_API_BASE}/groups/{workspace_id}"
            if workspace_id
            else POWER_BI_API_BASE
        )

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/{endpoint}"
        headers = self.auth.get_headers()
        resp = requests.request(method, url, headers=headers, **kwargs)

        if resp.status_code >= 400:
            logger.error(
                "API error %d: %s\nURL: %s",
                resp.status_code, resp.text, url,
            )
            resp.raise_for_status()

        return resp

    # ── Dataset operations ───────────────────────────────────────────────

    def list_datasets(self) -> list[dict]:
        resp = self._request("GET", "datasets")
        datasets = resp.json().get("value", [])
        return datasets

    def create_streaming_dataset(self, config: dict) -> dict:
        """Create a push/streaming dataset from a config dict."""
        payload = {
            "name": config["name"],
            "defaultMode": config.get("defaultMode", "Streaming"),
            "tables": config["tables"],
        }
        resp = self._request("POST", "datasets", json=payload)
        result = resp.json()
        logger.info(
            "Dataset created: id=%s, name=%s",
            result.get("id"), result.get("name"),
        )
        return result

    def delete_dataset(self, dataset_id: str) -> None:
        self._request("DELETE", f"datasets/{dataset_id}")
        logger.info("Dataset deleted: %s", dataset_id)

    def push_rows(self, dataset_id: str, table_name: str, rows: list[dict]) -> None:
        """Push rows to a streaming dataset table."""
        payload = {"rows": rows}
        self._request(
            "POST",
            f"datasets/{dataset_id}/tables/{table_name}/rows",
            json=payload,
        )
        logger.debug("Pushed %d rows to %s", len(rows), table_name)

    def clear_table(self, dataset_id: str, table_name: str) -> None:
        self._request("DELETE", f"datasets/{dataset_id}/tables/{table_name}/rows")
        logger.info("Cleared all rows from %s", table_name)


# ── Sample Data Generator ───────────────────────────────────────────────────

SAMPLE_CONTENT = [
    ("NF001", "Stranger Things S4", "tv_episode"),
    ("NF002", "Wednesday S1", "tv_episode"),
    ("NF003", "Glass Onion", "movie"),
    ("NF007", "Our Planet II", "documentary"),
    ("NF008", "Squid Game S1", "tv_episode"),
    ("NF017", "Rebel Moon", "movie"),
]

COUNTRIES = ["US", "UK", "DE", "JP", "BR", "IN", "AU", "CA", "FR", "KR"]
REGIONS = ["California", "England", "Bavaria", "Kanto", "Sao Paulo",
           "Maharashtra", "NSW", "Ontario", "Ile-de-France", "Seoul"]
CITIES = ["Los Angeles", "London", "Munich", "Tokyo", "Sao Paulo",
          "Mumbai", "Sydney", "Toronto", "Paris", "Seoul"]
DEVICES = ["smart_tv", "mobile", "tablet", "desktop", "game_console", "streaming_stick"]
SEGMENTS = ["power_viewer", "active_viewer", "casual_viewer", "low_engagement"]
TIERS = ["basic", "standard", "premium"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_viewer_count_rows(n: int = 5) -> list[dict]:
    rows = []
    ts = _now_iso()
    for cid, title, ctype in random.sample(SAMPLE_CONTENT, min(n, len(SAMPLE_CONTENT))):
        rows.append({
            "content_id": cid,
            "content_title": title,
            "content_type": ctype,
            "active_viewers": random.randint(50, 5000),
            "total_events": random.randint(100, 10000),
            "window_end": ts,
        })
    return rows


def generate_watch_time_rows(n: int = 5) -> list[dict]:
    rows = []
    ts = _now_iso()
    for cid, title, ctype in random.sample(SAMPLE_CONTENT, min(n, len(SAMPLE_CONTENT))):
        rows.append({
            "content_id": cid,
            "content_title": title,
            "content_type": ctype,
            "avg_watch_seconds": round(random.uniform(300, 5400), 1),
            "session_count": random.randint(10, 500),
            "window_end": ts,
        })
    return rows


def generate_trending_rows(n: int = 5) -> list[dict]:
    rows = []
    ts = _now_iso()
    for cid, title, ctype in random.sample(SAMPLE_CONTENT, min(n, len(SAMPLE_CONTENT))):
        viewers = random.randint(100, 8000)
        rows.append({
            "content_id": cid,
            "content_title": title,
            "content_type": ctype,
            "event_count": random.randint(200, 15000),
            "unique_viewers": viewers,
            "trending_score": round(viewers * 2 + random.uniform(0, 50), 1),
            "window_end": ts,
        })
    return rows


def generate_geo_rows(n: int = 5) -> list[dict]:
    rows = []
    ts = _now_iso()
    for i in range(min(n, len(COUNTRIES))):
        rows.append({
            "country": COUNTRIES[i],
            "region": REGIONS[i],
            "city": CITIES[i],
            "active_viewers": random.randint(20, 3000),
            "total_events": random.randint(50, 8000),
            "window_end": ts,
        })
    return rows


def generate_device_rows() -> list[dict]:
    rows = []
    ts = _now_iso()
    for device in DEVICES:
        rows.append({
            "device_type": device,
            "unique_users": random.randint(50, 3000),
            "event_count": random.randint(100, 8000),
            "avg_bitrate_kbps": round(random.uniform(2500, 20000), 0),
            "window_end": ts,
        })
    return rows


def generate_buffer_rows(n: int = 3) -> list[dict]:
    rows = []
    ts = _now_iso()
    for cid, title, _ in random.sample(SAMPLE_CONTENT, min(n, len(SAMPLE_CONTENT))):
        rows.append({
            "content_id": cid,
            "content_title": title,
            "device_type": random.choice(DEVICES),
            "buffer_count": random.randint(3, 25),
            "avg_buffer_ms": round(random.uniform(500, 8000), 1),
            "max_buffer_ms": round(random.uniform(5000, 15000), 1),
            "affected_users": random.randint(1, 50),
            "window_end": ts,
        })
    return rows


def generate_engagement_rows(n: int = 10) -> list[dict]:
    rows = []
    ts = _now_iso()
    for i in range(n):
        score = random.randint(5, 180)
        segment = (
            "power_viewer" if score >= 80
            else "active_viewer" if score >= 40
            else "casual_viewer" if score >= 15
            else "low_engagement"
        )
        rows.append({
            "user_id": f"U{random.randint(0, 9999):07d}",
            "subscription_tier": random.choice(TIERS),
            "device_type": random.choice(DEVICES),
            "country": random.choice(COUNTRIES),
            "engagement_score": float(score),
            "engagement_segment": segment,
            "total_watch_seconds": round(random.uniform(60, 7200), 1),
            "window_end": ts,
        })
    return rows


TABLE_GENERATORS = {
    "ViewerCount": generate_viewer_count_rows,
    "WatchTime": generate_watch_time_rows,
    "TrendingContent": generate_trending_rows,
    "GeoDistribution": generate_geo_rows,
    "DeviceDistribution": generate_device_rows,
    "BufferMetrics": generate_buffer_rows,
    "EngagementScores": generate_engagement_rows,
}


# ── Live Cosmos DB Push ──────────────────────────────────────────────────────

def push_live_from_cosmos(client: PowerBIClient, dataset_id: str, duration: int):
    """
    Read aggregation data from Cosmos DB and push to Power BI.
    Requires COSMOS_CONNECTION_STRING and azure-cosmos installed.
    """
    try:
        from azure.cosmos import CosmosClient
    except ImportError:
        logger.error("azure-cosmos not installed. Run: pip install azure-cosmos")
        return

    cosmos_conn = os.getenv("COSMOS_CONNECTION_STRING", "")
    if not cosmos_conn:
        logger.error("COSMOS_CONNECTION_STRING not set.")
        return

    cosmos = CosmosClient.from_connection_string(cosmos_conn)
    db = cosmos.get_database_client(os.getenv("COSMOS_DATABASE", "netflix-streaming"))
    container = db.get_container_client(
        os.getenv("COSMOS_CONTAINER_AGGREGATIONS", "aggregations")
    )

    logger.info("Pushing live data from Cosmos DB for %d seconds...", duration)
    end_time = time.time() + duration

    while time.time() < end_time:
        try:
            items = list(container.query_items(
                query="SELECT TOP 50 * FROM c ORDER BY c._ts DESC",
                enable_cross_partition_query=True,
            ))
            if items:
                # Group by aggregation_type and push to corresponding tables
                type_map = {
                    "viewer_count": "ViewerCount",
                    "watch_time": "WatchTime",
                    "trending": "TrendingContent",
                    "geo_distribution": "GeoDistribution",
                    "device_distribution": "DeviceDistribution",
                    "buffer_metrics": "BufferMetrics",
                    "engagement": "EngagementScores",
                }
                for item in items:
                    agg_type = item.get("aggregation_type", "")
                    table = type_map.get(agg_type)
                    if table:
                        client.push_rows(dataset_id, table, [item])

                logger.info("Pushed %d items from Cosmos DB", len(items))
        except Exception:
            logger.exception("Error reading from Cosmos DB")

        time.sleep(5)

    logger.info("Live push complete.")


# ── CLI ──────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    with open(DATASET_CONFIG_PATH) as f:
        return json.load(f)


def validate_env() -> tuple[str, str, str]:
    client_id = os.getenv("POWERBI_CLIENT_ID", "")
    client_secret = os.getenv("POWERBI_CLIENT_SECRET", "")
    tenant_id = os.getenv("POWERBI_TENANT_ID", "")

    missing = []
    if not client_id:
        missing.append("POWERBI_CLIENT_ID")
    if not client_secret:
        missing.append("POWERBI_CLIENT_SECRET")
    if not tenant_id:
        missing.append("POWERBI_TENANT_ID")

    if missing:
        logger.error(
            "Missing required environment variables: %s\n"
            "Set them in your .env file or shell environment.\n"
            "See .env.example for reference.",
            ", ".join(missing),
        )
        sys.exit(1)

    return client_id, client_secret, tenant_id


def main():
    parser = argparse.ArgumentParser(
        description="Power BI Streaming Dataset Setup & Data Push",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_streaming_dataset.py --action create
  python setup_streaming_dataset.py --action push-sample --dataset-id <id>
  python setup_streaming_dataset.py --action push-sample --dataset-id <id> --interval 5 --rounds 12
  python setup_streaming_dataset.py --action push-live --dataset-id <id> --duration 60
  python setup_streaming_dataset.py --action list
  python setup_streaming_dataset.py --action delete --dataset-id <id>
        """,
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["create", "push-sample", "push-live", "list", "delete"],
        help="Action to perform",
    )
    parser.add_argument(
        "--dataset-id",
        default=None,
        help="Dataset ID (required for push-sample, push-live, delete)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Seconds between push rounds (default: 10)",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=6,
        help="Number of push rounds for push-sample (default: 6)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds for push-live (default: 60)",
    )
    args = parser.parse_args()

    client_id, client_secret, tenant_id = validate_env()
    workspace_id = os.getenv("POWERBI_WORKSPACE_ID", None)

    auth = PowerBIAuth(client_id, client_secret, tenant_id)
    client = PowerBIClient(auth, workspace_id)

    # ── Create ───────────────────────────────────────────────────────────

    if args.action == "create":
        config = load_config()
        result = client.create_streaming_dataset(config)
        dataset_id = result.get("id", "")
        print(f"\nDataset created successfully!")
        print(f"  Name : {result.get('name')}")
        print(f"  ID   : {dataset_id}")
        print(f"\nNext step: push data with:")
        print(f"  python setup_streaming_dataset.py --action push-sample --dataset-id {dataset_id}")

    # ── List ─────────────────────────────────────────────────────────────

    elif args.action == "list":
        datasets = client.list_datasets()
        if not datasets:
            print("No datasets found.")
            return
        print(f"\n{'Name':<40} {'ID':<40} {'Mode'}")
        print("-" * 120)
        for ds in datasets:
            print(f"{ds.get('name', ''):<40} {ds.get('id', ''):<40} {ds.get('defaultMode', '')}")

    # ── Delete ───────────────────────────────────────────────────────────

    elif args.action == "delete":
        if not args.dataset_id:
            logger.error("--dataset-id is required for delete action.")
            sys.exit(1)
        client.delete_dataset(args.dataset_id)
        print(f"Dataset {args.dataset_id} deleted.")

    # ── Push Sample ──────────────────────────────────────────────────────

    elif args.action == "push-sample":
        if not args.dataset_id:
            logger.error("--dataset-id is required for push-sample action.")
            sys.exit(1)

        print(f"Pushing sample data: {args.rounds} rounds, {args.interval}s interval\n")

        for round_num in range(1, args.rounds + 1):
            for table_name, generator in TABLE_GENERATORS.items():
                rows = generator() if table_name in ("DeviceDistribution",) else generator()
                try:
                    client.push_rows(args.dataset_id, table_name, rows)
                    logger.info(
                        "Round %d/%d: pushed %d rows to %s",
                        round_num, args.rounds, len(rows), table_name,
                    )
                except requests.HTTPError as e:
                    logger.error("Failed to push to %s: %s", table_name, e)

            if round_num < args.rounds:
                logger.info("Waiting %d seconds before next round...", args.interval)
                time.sleep(args.interval)

        print(f"\nSample data push complete ({args.rounds} rounds).")
        print("Open Power BI Service to view your real-time dashboard.")

    # ── Push Live ────────────────────────────────────────────────────────

    elif args.action == "push-live":
        if not args.dataset_id:
            logger.error("--dataset-id is required for push-live action.")
            sys.exit(1)
        push_live_from_cosmos(client, args.dataset_id, args.duration)


if __name__ == "__main__":
    main()
