import asyncio
import pytest
import pytest_asyncio

from .conftest import make_event


pytestmark = pytest.mark.asyncio


async def _wait_processed(client, expected_unique: int, timeout: float = 5.0):
    for _ in range(int(timeout / 0.1)):
        resp = await client.get("/stats")
        if resp.json()["unique_processed"] >= expected_unique:
            return
        await asyncio.sleep(0.1)


# ---------------------------------------------------------------------------
# Test 1: Publish single event returns 202
# ---------------------------------------------------------------------------
async def test_publish_single_event(client):
    event = make_event(event_id="single-001")
    resp = await client.post("/publish", json={"events": [event]})
    assert resp.status_code == 202
    assert resp.json()["queued"] == 1


# ---------------------------------------------------------------------------
# Test 2: Duplicate event is dropped — only 1 unique processed
# ---------------------------------------------------------------------------
async def test_dedup_duplicate_dropped(client):
    event = make_event(event_id="dup-001")
    await client.post("/publish", json={"events": [event, event]})
    await _wait_processed(client, expected_unique=1)

    resp = await client.get("/stats")
    data = resp.json()
    assert data["unique_processed"] >= 1
    assert data["duplicate_dropped"] >= 1


# ---------------------------------------------------------------------------
# Test 3: Batch publish — multiple events queued at once
# ---------------------------------------------------------------------------
async def test_batch_publish(client):
    events = [make_event(event_id=f"batch-{i}") for i in range(10)]
    resp = await client.post("/publish", json={"events": events})
    assert resp.status_code == 202
    assert resp.json()["queued"] == 10


# ---------------------------------------------------------------------------
# Test 4: Schema validation — missing required field event_id
# ---------------------------------------------------------------------------
async def test_schema_validation_missing_field(client):
    bad_event = {
        "topic": "logs.auth",
        "timestamp": "2026-04-24T10:00:00+00:00",
        "source": "service-auth",
        "payload": {},
    }
    resp = await client.post("/publish", json={"events": [bad_event]})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 5: Schema validation — invalid ISO8601 timestamp
# ---------------------------------------------------------------------------
async def test_schema_validation_invalid_timestamp(client):
    bad_event = make_event(event_id="ts-invalid-001", timestamp="not-a-timestamp")
    resp = await client.post("/publish", json={"events": [bad_event]})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test 6: GET /events returns only events for the requested topic
# ---------------------------------------------------------------------------
async def test_get_events_by_topic(client):
    events = [
        make_event(topic="logs.payment", event_id="pay-001", source="service-payment"),
        make_event(topic="logs.order", event_id="ord-001", source="service-order"),
    ]
    await client.post("/publish", json={"events": events})
    await _wait_processed(client, expected_unique=2)

    resp = await client.get("/events", params={"topic": "logs.payment"})
    data = resp.json()
    assert data["topic"] == "logs.payment"
    assert data["count"] >= 1
    assert all(e["topic"] == "logs.payment" for e in data["events"])


# ---------------------------------------------------------------------------
# Test 7: Stats are consistent — received = unique + duplicate
# ---------------------------------------------------------------------------
async def test_stats_consistency(client):
    event = make_event(event_id="stat-001")
    await client.post("/publish", json={"events": [event, event, event]})
    await _wait_processed(client, expected_unique=1)

    resp = await client.get("/stats")
    data = resp.json()
    assert data["received"] == data["unique_processed"] + data["duplicate_dropped"]


# ---------------------------------------------------------------------------
# Test 8: Dedup store persists — reusing same store detects old events
# ---------------------------------------------------------------------------
async def test_dedup_persistence(fresh_dedup_store):
    store, db_path = fresh_dedup_store

    await store.mark_processed("logs.auth", "persist-001")
    assert await store.is_duplicate("logs.auth", "persist-001") is True
    assert await store.is_duplicate("logs.auth", "persist-002") is False

    reloaded = __import__(
        "aggregator.dedup_store", fromlist=["DedupStore"]
    ).DedupStore(db_path=db_path)
    assert await reloaded.is_duplicate("logs.auth", "persist-001") is True
    assert await reloaded.is_duplicate("logs.auth", "persist-002") is False


# ---------------------------------------------------------------------------
# Test 9: Stress test — 5000 events with 25% duplicates processed within limit
# ---------------------------------------------------------------------------
async def test_stress_batch_performance(client):
    import uuid
    import time

    unique_ids = [str(uuid.uuid4()) for _ in range(5000)]
    dup_ids = unique_ids[:1250]
    all_ids = unique_ids + dup_ids

    events = [make_event(event_id=eid, topic="logs.stress") for eid in all_ids]
    batch_size = 200

    start = time.monotonic()
    for i in range(0, len(events), batch_size):
        resp = await client.post("/publish", json={"events": events[i : i + batch_size]})
        assert resp.status_code == 202
    elapsed_publish = time.monotonic() - start

    await _wait_processed(client, expected_unique=5000, timeout=30.0)
    elapsed_total = time.monotonic() - start

    resp = await client.get("/stats")
    data = resp.json()

    assert data["unique_processed"] >= 5000
    assert data["duplicate_dropped"] >= 1250
    assert elapsed_publish < 30.0, f"Publishing took too long: {elapsed_publish:.2f}s"
    assert elapsed_total < 60.0, f"Total processing took too long: {elapsed_total:.2f}s"
