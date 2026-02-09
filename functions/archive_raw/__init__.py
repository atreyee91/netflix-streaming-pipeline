"""
Azure Function: Archive Raw Events to Data Lake Storage Gen2

Trigger : Azure Event Hub (netflix-events, consumer group cg-azure-functions)
Output  : Azure Data Lake Storage Gen2 (raw container)

Writes raw events in line-delimited JSON format, partitioned by
date and hour for efficient downstream querying.
"""

import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO
from typing import List

import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContentSettings

logger = logging.getLogger(__name__)

_blob_service_client = None

JSON_CONTENT_SETTINGS = ContentSettings(content_type="application/x-ndjson")


def _get_blob_service() -> BlobServiceClient:
    global _blob_service_client
    if _blob_service_client is None:
        conn_str = os.environ["DATALAKE_CONNECTION_STRING"]
        _blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    return _blob_service_client


def _build_blob_path(timestamp: datetime) -> str:
    """Partition path: raw/netflix-events/YYYY-MM-DD/HH/<batch_id>.json"""
    return (
        f"netflix-events/"
        f"{timestamp.strftime('%Y-%m-%d')}/"
        f"{timestamp.strftime('%H')}/"
        f"{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.json"
    )


def main(events: List[func.EventHubEvent]) -> None:
    """Archive a batch of raw events to ADLS Gen2."""
    logger.info("Archiving batch of %d events", len(events))

    container_name = os.environ.get("DATALAKE_CONTAINER", "raw")
    now = datetime.now(timezone.utc)
    blob_path = _build_blob_path(now)

    lines = []
    for event in events:
        raw = event.get_body().decode("utf-8")
        # Ensure each line is valid JSON (best effort)
        try:
            parsed = json.loads(raw)
            # Add archival metadata
            parsed["_archived_at"] = now.isoformat()
            parsed["_partition_key"] = event.partition_key
            parsed["_sequence_number"] = event.sequence_number
            parsed["_enqueued_time"] = (
                event.enqueued_time.isoformat() if event.enqueued_time else None
            )
            lines.append(json.dumps(parsed, separators=(",", ":")))
        except json.JSONDecodeError:
            # Still archive malformed events for forensics
            lines.append(json.dumps({
                "_raw": raw[:8192],
                "_archived_at": now.isoformat(),
                "_parse_error": True,
            }, separators=(",", ":")))

    payload = "\n".join(lines).encode("utf-8")

    try:
        blob_service = _get_blob_service()
        container_client = blob_service.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_path)

        blob_client.upload_blob(
            data=BytesIO(payload),
            length=len(payload),
            overwrite=True,
            content_settings=JSON_CONTENT_SETTINGS,
        )
        logger.info("Archived %d events to %s/%s (%d bytes)",
                     len(events), container_name, blob_path, len(payload))
    except Exception:
        logger.exception("Failed to archive batch to Data Lake")
        raise  # Let the Functions runtime handle retry
