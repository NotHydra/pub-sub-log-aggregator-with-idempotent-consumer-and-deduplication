import asyncio
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from .models import PublishRequest

router = APIRouter()


def _consumer(request: Request):
    return request.app.state.consumer


def _queue(request: Request) -> asyncio.Queue:
    return request.app.state.queue


def _start_time(request: Request) -> datetime:
    return request.app.state.start_time


@router.post("/publish", status_code=202)
async def publish(body: PublishRequest, request: Request) -> dict[str, Any]:
    queue = _queue(request)
    for event in body.events:
        await queue.put(event)
    return {"queued": len(body.events)}


@router.get("/events")
async def get_events(request: Request, topic: str = Query(...)) -> dict[str, Any]:
    consumer = _consumer(request)
    events = consumer.event_store.get(topic, [])
    return {"topic": topic, "count": len(events), "events": events}


@router.get("/stats")
async def get_stats(request: Request) -> dict[str, Any]:
    consumer = _consumer(request)
    stats = consumer.stats
    uptime = (datetime.now(timezone.utc) - _start_time(request)).total_seconds()
    return {
        "received": stats.received,
        "unique_processed": stats.unique_processed,
        "duplicate_dropped": stats.duplicate_dropped,
        "topics": list(consumer.event_store.keys()),
        "uptime_seconds": round(uptime, 2),
    }
