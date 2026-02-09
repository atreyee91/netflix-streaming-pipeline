"""
Azure Function: Process Netflix Streaming Events

Trigger : Azure Event Hub (netflix-events, consumer group cg-azure-functions)
Output  : Cosmos DB (processed-events container)
DLQ     : Malformed / unprocessable events are forwarded to the dead-letter hub.

The function validates each event, enriches it with processing metadata,
and writes it to Cosmos DB with retry logic.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import List

import azure.functions as func
from azure.cosmos import CosmosClient, PartitionKey, exceptions as cosmos_exc
from azure.eventhub import EventData, EventHubProducerClient

logger = logging.getLogger(__name__)

# ── Cosmos DB client (module-level singleton) ────────────────────────────────

_cosmos_client = None
_cosmos_container = None
_dlq_producer = None

VALID_EVENT_TYPES = {
    "video_start", "video_pause", "video_stop",
    "video_complete", "buffer_event",
}
VALID_DEVICE_TYPES = {
    "smart_tv", "mobile", "tablet",
    "desktop", "game_console", "streaming_stick",
}


def _get_cosmos_container():
    global _cosmos_client, _cosmos_container
    if _cosmos_container is None:
        conn_str = os.environ["COSMOS_CONNECTION_STRING"]
        db_name = os.environ.get("COSMOS_DATABASE", "netflix-streaming")
        container_name = os.environ.get("COSMOS_CONTAINER_EVENTS", "processed-events")
        _cosmos_client = CosmosClient.from_connection_string(conn_str)
        db = _cosmos_client.get_database_client(db_name)
        _cosmos_container = db.get_container_client(container_name)
    return _cosmos_container


def _get_dlq_producer():
    global _dlq_producer
    if _dlq_producer is None:
        conn = os.environ.get("DEAD_LETTER_EVENTHUB_CONNECTION", "")
        name = os.environ.get("DEAD_LETTER_EVENTHUB_NAME", "netflix-events-dlq")
        if conn:
            _dlq_producer = EventHubProducerClient.from_connection_string(
                conn_str=conn, eventhub_name=name,
            )
    return _dlq_producer


# ── Validation ───────────────────────────────────────────────────────────────

def _validate_event(event: dict) -> List[str]:
    """Return a list of validation errors (empty = valid)."""
    errors = []
    for field in ("event_id", "event_type", "user_id", "content_id", "timestamp"):
        if not event.get(field):
            errors.append(f"missing required field: {field}")

    if event.get("event_type") and event["event_type"] not in VALID_EVENT_TYPES:
        errors.append(f"invalid event_type: {event['event_type']}")

    if event.get("device_type") and event["device_type"] not in VALID_DEVICE_TYPES:
        errors.append(f"invalid device_type: {event['device_type']}")

    return errors


# ── Enrichment ───────────────────────────────────────────────────────────────

def _enrich_event(event: dict) -> dict:
    """Add processing metadata to a validated event."""
    event["id"] = event.get("event_id", str(uuid.uuid4()))
    event["processed_at"] = datetime.now(timezone.utc).isoformat()
    event["processing_version"] = "1.0.0"

    # Derive hour bucket for time-series analysis
    try:
        ts = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        event["hour_bucket"] = ts.strftime("%Y-%m-%dT%H:00:00Z")
    except (ValueError, KeyError):
        event["hour_bucket"] = None

    return event


# ── Dead-letter ──────────────────────────────────────────────────────────────

def _send_to_dlq(raw_body: str, errors: List[str]) -> None:
    producer = _get_dlq_producer()
    if producer is None:
        logger.warning("DLQ producer not configured – dropping invalid event")
        return
    try:
        dlq_payload = json.dumps({
            "original_event": raw_body[:8192],
            "validation_errors": errors,
            "rejected_at": datetime.now(timezone.utc).isoformat(),
        })
        batch = producer.create_batch()
        batch.add(EventData(dlq_payload))
        producer.send_batch(batch)
        logger.info("Sent invalid event to DLQ")
    except Exception:
        logger.exception("Failed to send event to DLQ")


# ── Main function ────────────────────────────────────────────────────────────

def main(events: List[func.EventHubEvent]) -> None:
    """Process a batch of Event Hub events."""
    logger.info("Received batch of %d events", len(events))
    container = _get_cosmos_container()

    success_count = 0
    error_count = 0

    for event in events:
        raw_body = event.get_body().decode("utf-8")
        try:
            data = json.loads(raw_body)
        except json.JSONDecodeError:
            logger.error("Malformed JSON in event")
            _send_to_dlq(raw_body, ["invalid JSON"])
            error_count += 1
            continue

        validation_errors = _validate_event(data)
        if validation_errors:
            logger.warning("Validation failed: %s", validation_errors)
            _send_to_dlq(raw_body, validation_errors)
            error_count += 1
            continue

        enriched = _enrich_event(data)

        # Write to Cosmos DB with retry (SDK handles transient retries)
        try:
            container.upsert_item(body=enriched)
            success_count += 1
        except cosmos_exc.CosmosHttpResponseError as exc:
            logger.error(
                "Cosmos write failed (status=%s): %s", exc.status_code, exc.message
            )
            error_count += 1
        except Exception:
            logger.exception("Unexpected error writing to Cosmos DB")
            error_count += 1

    logger.info(
        "Batch complete: %d succeeded, %d failed out of %d",
        success_count, error_count, len(events),
    )
