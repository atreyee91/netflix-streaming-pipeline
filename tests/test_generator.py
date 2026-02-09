"""Tests for the Netflix streaming event data generator."""

import json
import pytest

from data_generator.models import StreamingEvent, EventType, DeviceType
from data_generator.generator import NetflixEventGenerator


class TestStreamingEvent:
    """Tests for the StreamingEvent data model."""

    def test_create_valid_event(self):
        event = StreamingEvent(
            event_type="video_start",
            user_id="U0000001",
            content_id="NF001",
            timestamp="2025-01-15T14:30:00Z",
            device_type="smart_tv",
        )
        assert event.event_type == "video_start"
        assert event.user_id == "U0000001"

    def test_validate_valid_event(self):
        event = StreamingEvent(
            event_type="video_start",
            user_id="U0000001",
            content_id="NF001",
            timestamp="2025-01-15T14:30:00Z",
            device_type="smart_tv",
        )
        assert event.validate() is True

    def test_validate_missing_field(self):
        event = StreamingEvent(
            event_type="video_start",
            user_id="",
            content_id="NF001",
            timestamp="2025-01-15T14:30:00Z",
            device_type="smart_tv",
        )
        with pytest.raises(ValueError, match="Missing required field"):
            event.validate()

    def test_validate_invalid_event_type(self):
        event = StreamingEvent(
            event_type="invalid_event",
            user_id="U0000001",
            content_id="NF001",
            timestamp="2025-01-15T14:30:00Z",
            device_type="smart_tv",
        )
        with pytest.raises(ValueError, match="Invalid event_type"):
            event.validate()

    def test_validate_invalid_device_type(self):
        event = StreamingEvent(
            event_type="video_start",
            user_id="U0000001",
            content_id="NF001",
            timestamp="2025-01-15T14:30:00Z",
            device_type="fridge",
        )
        with pytest.raises(ValueError, match="Invalid device_type"):
            event.validate()

    def test_to_json_produces_valid_json(self):
        event = StreamingEvent(
            event_type="video_start",
            user_id="U0000001",
            content_id="NF001",
            timestamp="2025-01-15T14:30:00Z",
            device_type="smart_tv",
        )
        parsed = json.loads(event.to_json())
        assert parsed["event_type"] == "video_start"
        assert parsed["user_id"] == "U0000001"

    def test_to_json_excludes_none(self):
        event = StreamingEvent(
            event_type="video_start",
            user_id="U0000001",
            content_id="NF001",
            timestamp="2025-01-15T14:30:00Z",
            device_type="smart_tv",
            buffer_duration_ms=None,
        )
        parsed = json.loads(event.to_json())
        assert "buffer_duration_ms" not in parsed

    def test_to_dict_returns_dict(self):
        event = StreamingEvent(
            event_type="buffer_event",
            user_id="U0000001",
            content_id="NF001",
            timestamp="2025-01-15T14:30:00Z",
            device_type="mobile",
            buffer_duration_ms=2500.0,
        )
        d = event.to_dict()
        assert isinstance(d, dict)
        assert d["buffer_duration_ms"] == 2500.0


class TestNetflixEventGenerator:
    """Tests for the event generator."""

    def test_init_defaults(self):
        gen = NetflixEventGenerator(dry_run=True)
        assert gen.events_per_second == 100
        assert gen.num_users == 10_000
        assert len(gen.users) == 10_000

    def test_init_custom_users(self):
        gen = NetflixEventGenerator(dry_run=True, num_users=50)
        assert len(gen.users) == 50

    def test_generate_batch_returns_correct_size(self):
        gen = NetflixEventGenerator(dry_run=True, num_users=100)
        batch = gen.generate_batch(25)
        assert len(batch) == 25

    def test_generated_events_are_valid(self):
        gen = NetflixEventGenerator(dry_run=True, num_users=100)
        batch = gen.generate_batch(50)
        for event in batch:
            assert event.validate() is True

    def test_generated_events_have_required_fields(self):
        gen = NetflixEventGenerator(dry_run=True, num_users=100)
        batch = gen.generate_batch(10)
        for event in batch:
            d = event.to_dict()
            assert d["event_type"] in [e.value for e in EventType]
            assert d["device_type"] in [dt.value for dt in DeviceType]
            assert d["user_id"].startswith("U")
            assert d["content_id"].startswith("NF")
            assert d["timestamp"]

    def test_buffer_events_have_duration(self):
        gen = NetflixEventGenerator(dry_run=True, num_users=100)
        # Generate enough events to get at least one buffer_event
        batch = gen.generate_batch(500)
        buffer_events = [e for e in batch if e.event_type == "buffer_event"]
        assert len(buffer_events) > 0, "Expected at least one buffer event in 500"
        for be in buffer_events:
            assert be.buffer_duration_ms is not None
            assert be.buffer_duration_ms > 0

    def test_stats_initial(self):
        gen = NetflixEventGenerator(dry_run=True)
        assert gen.stats == {"total_sent": 0, "errors": 0}

    def test_user_pool_has_valid_structure(self):
        gen = NetflixEventGenerator(dry_run=True, num_users=10)
        for user in gen.users:
            assert "user_id" in user
            assert "device_type" in user
            assert "location" in user
            assert "subscription_tier" in user
            assert user["subscription_tier"] in ("basic", "standard", "premium")


class TestSampleData:
    """Tests for the sample data fixture."""

    def test_sample_data_loads(self, sample_events):
        assert len(sample_events) == 5

    def test_sample_events_are_valid(self, sample_events):
        for data in sample_events:
            event = StreamingEvent(**data)
            assert event.validate() is True

    def test_sample_has_all_event_types(self, sample_events):
        types = {e["event_type"] for e in sample_events}
        assert types == {
            "video_start", "video_pause", "video_stop",
            "video_complete", "buffer_event",
        }
