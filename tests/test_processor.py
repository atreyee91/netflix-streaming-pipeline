"""Tests for the Azure Function event processor logic."""

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import sys
from pathlib import Path

# Add functions directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "functions"))

from process_events import _validate_event, _enrich_event


class TestValidation:
    """Tests for event validation logic."""

    def test_valid_event_passes(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "video_start",
            "user_id": "U0000001",
            "content_id": "NF001",
            "timestamp": "2025-01-15T14:30:00Z",
            "device_type": "smart_tv",
        }
        errors = _validate_event(event)
        assert errors == []

    def test_missing_event_id(self):
        event = {
            "event_type": "video_start",
            "user_id": "U0000001",
            "content_id": "NF001",
            "timestamp": "2025-01-15T14:30:00Z",
        }
        errors = _validate_event(event)
        assert any("event_id" in e for e in errors)

    def test_missing_multiple_fields(self):
        event = {"event_type": "video_start"}
        errors = _validate_event(event)
        assert len(errors) >= 3

    def test_invalid_event_type(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "video_explode",
            "user_id": "U0000001",
            "content_id": "NF001",
            "timestamp": "2025-01-15T14:30:00Z",
            "device_type": "smart_tv",
        }
        errors = _validate_event(event)
        assert any("invalid event_type" in e for e in errors)

    def test_invalid_device_type(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "video_start",
            "user_id": "U0000001",
            "content_id": "NF001",
            "timestamp": "2025-01-15T14:30:00Z",
            "device_type": "toaster",
        }
        errors = _validate_event(event)
        assert any("invalid device_type" in e for e in errors)

    def test_empty_event(self):
        errors = _validate_event({})
        assert len(errors) >= 5


class TestEnrichment:
    """Tests for event enrichment logic."""

    def test_adds_processing_metadata(self):
        event = {
            "event_id": "abc-123",
            "timestamp": "2025-01-15T14:30:00Z",
        }
        enriched = _enrich_event(event)
        assert enriched["id"] == "abc-123"
        assert "processed_at" in enriched
        assert enriched["processing_version"] == "1.0.0"

    def test_adds_hour_bucket(self):
        event = {
            "event_id": "abc-123",
            "timestamp": "2025-01-15T14:30:00Z",
        }
        enriched = _enrich_event(event)
        assert enriched["hour_bucket"] == "2025-01-15T14:00:00Z"

    def test_missing_event_id_gets_uuid(self):
        event = {"timestamp": "2025-01-15T14:30:00Z"}
        enriched = _enrich_event(event)
        assert enriched["id"]  # should have a generated UUID

    def test_invalid_timestamp_sets_null_bucket(self):
        event = {
            "event_id": "abc-123",
            "timestamp": "not-a-timestamp",
        }
        enriched = _enrich_event(event)
        assert enriched["hour_bucket"] is None
