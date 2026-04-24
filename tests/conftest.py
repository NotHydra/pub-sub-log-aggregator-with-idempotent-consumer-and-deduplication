import asyncio
import sys
import os
import tempfile
import pytest
import pytest_asyncio

from httpx import AsyncClient, ASGITransport

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "aggregator"))

from src.main import app
from src.dedup_store import DedupStore


@pytest.fixture(scope="session")
def event_loop_policy():
    return asyncio.DefaultEventLoopPolicy()


@pytest_asyncio.fixture
async def client(tmp_path):
    db_path = str(tmp_path / "test_dedup.db")
    os.environ["DEDUP_DB_PATH"] = db_path

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        async with app.router.lifespan_context(app):
            yield ac


@pytest_asyncio.fixture
async def fresh_dedup_store(tmp_path):
    db_path = str(tmp_path / "dedup.db")
    store = DedupStore(db_path=db_path)
    return store, db_path


def make_event(
    topic: str = "logs.auth",
    event_id: str = "evt-001",
    source: str = "service-auth",
    timestamp: str = "2026-04-24T10:00:00+00:00",
    payload: dict | None = None,
) -> dict:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": timestamp,
        "source": source,
        "payload": payload or {"level": "INFO", "message": "test"},
    }
