import asyncio
import logging
import os
import random
import uuid
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")
EVENT_COUNT = int(os.getenv("EVENT_COUNT", "6000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.25"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

TOPICS = ["logs.auth", "logs.payment", "logs.order", "logs.inventory", "logs.notification"]
SOURCES = ["service-auth", "service-payment", "service-order", "service-inventory", "service-notification"]


def _make_event(topic: str, source: str, event_id: str | None = None) -> dict:
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": {
            "level": random.choice(["INFO", "WARNING", "ERROR"]),
            "message": f"Log entry from {source}",
            "request_id": str(uuid.uuid4()),
        },
    }


async def generate_and_send() -> dict:
    unique_events: list[dict] = []
    for _ in range(EVENT_COUNT):
        topic = random.choice(TOPICS)
        source = SOURCES[TOPICS.index(topic)]
        unique_events.append(_make_event(topic, source))

    duplicate_count = int(EVENT_COUNT * DUPLICATE_RATE)
    duplicates = [random.choice(unique_events).copy() for _ in range(duplicate_count)]
    for dup in duplicates:
        dup["timestamp"] = datetime.now(timezone.utc).isoformat()

    all_events = unique_events + duplicates
    random.shuffle(all_events)

    total_sent = 0
    total_failed = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(0, len(all_events), BATCH_SIZE):
            batch = all_events[i : i + BATCH_SIZE]
            try:
                resp = await client.post(
                    f"{AGGREGATOR_URL}/publish",
                    json={"events": batch},
                )
                resp.raise_for_status()
                total_sent += len(batch)
            except Exception as exc:
                logger.error("Batch %d failed: %s", i // BATCH_SIZE, exc)
                total_failed += len(batch)

    return {
        "total_events": len(all_events),
        "unique_events": len(unique_events),
        "duplicate_events": duplicate_count,
        "sent": total_sent,
        "failed": total_failed,
    }
