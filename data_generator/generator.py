"""
Netflix Streaming Event Generator

Simulates realistic Netflix streaming events and sends them to Azure Event Hubs.
Supports configurable throughput from 100 to 1000+ events per second.
"""

import asyncio
import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

from data_generator.models import (
    StreamingEvent,
    EventType,
    DeviceType,
    ContentType,
    QualityLevel,
)

logger = logging.getLogger(__name__)

# ─── Catalog & User Pool ────────────────────────────────────────────────────

CONTENT_CATALOG = [
    {"id": "NF001", "title": "Stranger Things S4", "type": "tv_episode", "duration": 4500},
    {"id": "NF002", "title": "Wednesday S1", "type": "tv_episode", "duration": 2700},
    {"id": "NF003", "title": "Glass Onion", "type": "movie", "duration": 8400},
    {"id": "NF004", "title": "All Quiet on the Western Front", "type": "movie", "duration": 8880},
    {"id": "NF005", "title": "The Crown S5", "type": "tv_episode", "duration": 3600},
    {"id": "NF006", "title": "Dahmer", "type": "tv_episode", "duration": 3000},
    {"id": "NF007", "title": "Our Planet II", "type": "documentary", "duration": 3000},
    {"id": "NF008", "title": "Squid Game S1", "type": "tv_episode", "duration": 3300},
    {"id": "NF009", "title": "The Witcher S3", "type": "tv_episode", "duration": 3600},
    {"id": "NF010", "title": "Heart of Stone", "type": "movie", "duration": 7500},
    {"id": "NF011", "title": "Dave Chappelle: The Closer", "type": "special", "duration": 4200},
    {"id": "NF012", "title": "Extraction 2", "type": "movie", "duration": 7200},
    {"id": "NF013", "title": "Black Mirror S6", "type": "tv_episode", "duration": 4200},
    {"id": "NF014", "title": "The Diplomat S1", "type": "tv_episode", "duration": 2700},
    {"id": "NF015", "title": "You S4", "type": "tv_episode", "duration": 3000},
    {"id": "NF016", "title": "Behind the Curve", "type": "documentary", "duration": 5700},
    {"id": "NF017", "title": "Rebel Moon", "type": "movie", "duration": 8100},
    {"id": "NF018", "title": "One Piece S1", "type": "tv_episode", "duration": 3300},
    {"id": "NF019", "title": "The Night Agent S1", "type": "tv_episode", "duration": 2700},
    {"id": "NF020", "title": "Luther: The Fallen Sun", "type": "movie", "duration": 7800},
]

LOCATIONS = [
    {"country": "US", "region": "California", "city": "Los Angeles", "lat": 34.05, "lon": -118.24},
    {"country": "US", "region": "New York", "city": "New York", "lat": 40.71, "lon": -74.00},
    {"country": "US", "region": "Texas", "city": "Houston", "lat": 29.76, "lon": -95.36},
    {"country": "US", "region": "Illinois", "city": "Chicago", "lat": 41.88, "lon": -87.63},
    {"country": "UK", "region": "England", "city": "London", "lat": 51.51, "lon": -0.13},
    {"country": "DE", "region": "Bavaria", "city": "Munich", "lat": 48.14, "lon": 11.58},
    {"country": "JP", "region": "Kanto", "city": "Tokyo", "lat": 35.68, "lon": 139.69},
    {"country": "BR", "region": "Sao Paulo", "city": "Sao Paulo", "lat": -23.55, "lon": -46.63},
    {"country": "IN", "region": "Maharashtra", "city": "Mumbai", "lat": 19.08, "lon": 72.88},
    {"country": "AU", "region": "NSW", "city": "Sydney", "lat": -33.87, "lon": 151.21},
    {"country": "CA", "region": "Ontario", "city": "Toronto", "lat": 43.65, "lon": -79.38},
    {"country": "FR", "region": "Ile-de-France", "city": "Paris", "lat": 48.86, "lon": 2.35},
    {"country": "KR", "region": "Seoul", "city": "Seoul", "lat": 37.57, "lon": 126.98},
    {"country": "MX", "region": "CDMX", "city": "Mexico City", "lat": 19.43, "lon": -99.13},
    {"country": "ES", "region": "Madrid", "city": "Madrid", "lat": 40.42, "lon": -3.70},
]

SUBSCRIPTION_TIERS = ["basic", "standard", "premium"]

QUALITY_MAP = {
    "basic": [("480p", 2500)],
    "standard": [("720p", 5000), ("1080p", 8000)],
    "premium": [("1080p", 8000), ("4K", 16000), ("4K_HDR", 20000)],
}

DEVICE_WEIGHTS = {
    DeviceType.SMART_TV.value: 0.35,
    DeviceType.MOBILE.value: 0.28,
    DeviceType.TABLET.value: 0.10,
    DeviceType.DESKTOP.value: 0.12,
    DeviceType.GAME_CONSOLE.value: 0.08,
    DeviceType.STREAMING_STICK.value: 0.07,
}

# Event type weights simulate realistic distribution:
# most events are starts/stops, buffers are rarer
EVENT_WEIGHTS = {
    EventType.VIDEO_START.value: 0.30,
    EventType.VIDEO_PAUSE.value: 0.15,
    EventType.VIDEO_STOP.value: 0.20,
    EventType.VIDEO_COMPLETE.value: 0.25,
    EventType.BUFFER_EVENT.value: 0.10,
}


class NetflixEventGenerator:
    """Generates simulated Netflix streaming events and publishes to Event Hubs."""

    def __init__(
        self,
        connection_string: Optional[str] = None,
        eventhub_name: Optional[str] = None,
        events_per_second: int = 100,
        num_users: int = 10_000,
        dry_run: bool = False,
    ):
        self.connection_string = connection_string or os.getenv("EVENTHUB_CONNECTION_STRING", "")
        self.eventhub_name = eventhub_name or os.getenv("EVENTHUB_NAME", "netflix-events")
        self.events_per_second = events_per_second
        self.num_users = num_users
        self.dry_run = dry_run
        self._running = False
        self._total_sent = 0
        self._errors = 0

        # Pre-generate user pool
        self.users = self._build_user_pool()

        logger.info(
            "Generator initialised: eps=%d, users=%d, dry_run=%s",
            self.events_per_second, self.num_users, self.dry_run,
        )

    # ── User pool ────────────────────────────────────────────────────────

    def _build_user_pool(self) -> list[dict]:
        users = []
        for i in range(self.num_users):
            tier = random.choices(
                SUBSCRIPTION_TIERS, weights=[0.20, 0.50, 0.30], k=1
            )[0]
            users.append({
                "user_id": f"U{i:07d}",
                "profile_id": f"P{i:07d}_{random.randint(1,5)}",
                "device_type": random.choices(
                    list(DEVICE_WEIGHTS.keys()),
                    weights=list(DEVICE_WEIGHTS.values()),
                    k=1,
                )[0],
                "device_id": str(uuid.uuid4()),
                "location": random.choice(LOCATIONS),
                "subscription_tier": tier,
            })
        return users

    # ── Single event creation ────────────────────────────────────────────

    def _generate_event(self) -> StreamingEvent:
        user = random.choice(self.users)
        content = random.choice(CONTENT_CATALOG)
        event_type = random.choices(
            list(EVENT_WEIGHTS.keys()),
            weights=list(EVENT_WEIGHTS.values()),
            k=1,
        )[0]

        tier = user["subscription_tier"]
        quality = random.choice(QUALITY_MAP[tier])

        playback_pos = random.uniform(0, content["duration"])
        duration = (
            random.uniform(0, content["duration"])
            if event_type in (EventType.VIDEO_STOP.value, EventType.VIDEO_COMPLETE.value)
            else 0.0
        )

        buffer_ms = None
        if event_type == EventType.BUFFER_EVENT.value:
            buffer_ms = round(random.uniform(500, 15000), 1)

        event = StreamingEvent(
            event_type=event_type,
            user_id=user["user_id"],
            session_id=str(uuid.uuid4()),
            content_id=content["id"],
            content_title=content["title"],
            content_type=content["type"],
            timestamp=datetime.now(timezone.utc).isoformat(),
            duration_seconds=round(duration, 2),
            playback_position_seconds=round(playback_pos, 2),
            device_type=user["device_type"],
            device_id=user["device_id"],
            location=user["location"],
            quality_settings={
                "resolution": quality[0],
                "bitrate_kbps": quality[1],
                "audio_codec": "AAC",
                "video_codec": "H.265",
                "hdr_enabled": "HDR" in quality[0],
            },
            buffer_duration_ms=buffer_ms,
            profile_id=user["profile_id"],
            subscription_tier=tier,
        )
        return event

    # ── Batch helper ─────────────────────────────────────────────────────

    def generate_batch(self, size: int) -> list[StreamingEvent]:
        return [self._generate_event() for _ in range(size)]

    # ── Async publishing ─────────────────────────────────────────────────

    async def _publish_batch(
        self,
        producer: EventHubProducerClient,
        events: list[StreamingEvent],
    ) -> int:
        batch = await producer.create_batch()
        added = 0
        for event in events:
            try:
                batch.add(EventData(event.to_json()))
                added += 1
            except ValueError:
                # Batch full – send what we have and start a new one
                await producer.send_batch(batch)
                batch = await producer.create_batch()
                batch.add(EventData(event.to_json()))
                added += 1
        if added:
            await producer.send_batch(batch)
        return added

    async def run(self) -> None:
        """Main loop: generate and publish events at the configured rate."""
        self._running = True
        batch_size = max(1, self.events_per_second // 10)  # 10 batches / sec
        interval = 1.0 / 10  # send 10 batches per second

        if self.dry_run:
            logger.info("DRY RUN mode – events will be generated but not sent.")
            await self._dry_run_loop(batch_size, interval)
            return

        if not self.connection_string:
            raise ValueError(
                "EVENTHUB_CONNECTION_STRING must be set (or pass connection_string=)."
            )

        producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string,
            eventhub_name=self.eventhub_name,
        )

        logger.info("Connected to Event Hub '%s'. Starting publish loop.", self.eventhub_name)
        try:
            while self._running:
                loop_start = time.monotonic()
                events = self.generate_batch(batch_size)
                try:
                    sent = await self._publish_batch(producer, events)
                    self._total_sent += sent
                except Exception:
                    self._errors += 1
                    logger.exception("Publish error (total errors: %d)", self._errors)

                elapsed = time.monotonic() - loop_start
                sleep_time = max(0, interval - elapsed)
                if sleep_time:
                    await asyncio.sleep(sleep_time)

                if self._total_sent % 1000 == 0 and self._total_sent:
                    logger.info(
                        "Published %d events (%d errors)", self._total_sent, self._errors
                    )
        finally:
            await producer.close()
            logger.info(
                "Generator stopped. Total sent: %d, errors: %d",
                self._total_sent, self._errors,
            )

    async def _dry_run_loop(self, batch_size: int, interval: float) -> None:
        while self._running:
            events = self.generate_batch(batch_size)
            self._total_sent += len(events)
            if self._total_sent % 500 == 0:
                sample = events[0].to_dict()
                logger.info(
                    "[DRY RUN] Generated %d events. Sample:\n%s",
                    self._total_sent,
                    json.dumps(sample, indent=2),
                )
            await asyncio.sleep(interval)

    def stop(self) -> None:
        self._running = False

    @property
    def stats(self) -> dict:
        return {"total_sent": self._total_sent, "errors": self._errors}


# ─── CLI entry-point ─────────────────────────────────────────────────────────

def main() -> None:
    """Run the generator from the command line."""
    import argparse

    parser = argparse.ArgumentParser(description="Netflix Streaming Event Generator")
    parser.add_argument("--eps", type=int, default=100, help="Events per second (default: 100)")
    parser.add_argument("--users", type=int, default=10_000, help="User pool size (default: 10000)")
    parser.add_argument("--dry-run", action="store_true", help="Generate events without sending")
    parser.add_argument("--duration", type=int, default=0, help="Run for N seconds (0=unlimited)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    generator = NetflixEventGenerator(
        events_per_second=args.eps,
        num_users=args.users,
        dry_run=args.dry_run,
    )

    loop = asyncio.new_event_loop()

    # Graceful shutdown on Ctrl+C
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, generator.stop)

    async def _run_with_timeout():
        task = asyncio.create_task(generator.run())
        if args.duration > 0:
            await asyncio.sleep(args.duration)
            generator.stop()
        await task

    try:
        loop.run_until_complete(_run_with_timeout())
    finally:
        loop.close()
        logger.info("Final stats: %s", generator.stats)


if __name__ == "__main__":
    main()
