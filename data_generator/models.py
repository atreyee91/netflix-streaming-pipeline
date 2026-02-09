"""Data models for Netflix streaming events."""

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class EventType(str, Enum):
    VIDEO_START = "video_start"
    VIDEO_PAUSE = "video_pause"
    VIDEO_STOP = "video_stop"
    VIDEO_COMPLETE = "video_complete"
    BUFFER_EVENT = "buffer_event"


class DeviceType(str, Enum):
    SMART_TV = "smart_tv"
    MOBILE = "mobile"
    TABLET = "tablet"
    DESKTOP = "desktop"
    GAME_CONSOLE = "game_console"
    STREAMING_STICK = "streaming_stick"


class ContentType(str, Enum):
    MOVIE = "movie"
    TV_EPISODE = "tv_episode"
    DOCUMENTARY = "documentary"
    SPECIAL = "special"
    TRAILER = "trailer"


class QualityLevel(str, Enum):
    SD = "480p"
    HD = "720p"
    FULL_HD = "1080p"
    UHD = "4K"
    HDR = "4K_HDR"


@dataclass
class QualitySettings:
    resolution: str
    bitrate_kbps: int
    audio_codec: str = "AAC"
    video_codec: str = "H.265"
    hdr_enabled: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class StreamingEvent:
    """Represents a single Netflix streaming event."""

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    user_id: str = ""
    session_id: str = ""
    content_id: str = ""
    content_title: str = ""
    content_type: str = ""
    timestamp: str = ""
    duration_seconds: float = 0.0
    playback_position_seconds: float = 0.0
    device_type: str = ""
    device_id: str = ""
    location: dict = field(default_factory=dict)
    quality_settings: dict = field(default_factory=dict)
    buffer_duration_ms: Optional[float] = None
    error_code: Optional[str] = None
    profile_id: str = ""
    subscription_tier: str = "standard"

    def to_json(self) -> str:
        data = asdict(self)
        # Remove None values for cleaner output
        data = {k: v for k, v in data.items() if v is not None}
        return json.dumps(data)

    def to_dict(self) -> dict:
        data = asdict(self)
        return {k: v for k, v in data.items() if v is not None}

    def validate(self) -> bool:
        """Basic schema validation."""
        required = [
            "event_id", "event_type", "user_id", "content_id",
            "timestamp", "device_type",
        ]
        data = self.to_dict()
        for field_name in required:
            if field_name not in data or not data[field_name]:
                raise ValueError(f"Missing required field: {field_name}")

        valid_events = [e.value for e in EventType]
        if self.event_type not in valid_events:
            raise ValueError(f"Invalid event_type: {self.event_type}")

        valid_devices = [d.value for d in DeviceType]
        if self.device_type not in valid_devices:
            raise ValueError(f"Invalid device_type: {self.device_type}")

        return True
