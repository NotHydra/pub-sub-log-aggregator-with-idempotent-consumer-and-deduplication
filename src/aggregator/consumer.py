import asyncio
import logging
from collections import defaultdict

from .dedup_store import DedupStore
from .models import Event

logger = logging.getLogger(__name__)


class Stats:
    def __init__(self) -> None:
        self.received: int = 0
        self.unique_processed: int = 0
        self.duplicate_dropped: int = 0


class Consumer:
    def __init__(self, queue: asyncio.Queue, dedup_store: DedupStore) -> None:
        self._queue = queue
        self._dedup_store = dedup_store
        self.stats = Stats()
        self.event_store: dict[str, list[dict]] = defaultdict(list)

    async def run(self) -> None:
        while True:
            event: Event = await self._queue.get()
            try:
                await self._process(event)
            finally:
                self._queue.task_done()

    async def _process(self, event: Event) -> None:
        self.stats.received += 1

        if await self._dedup_store.is_duplicate(event.topic, event.event_id):
            logger.warning("DUPLICATE DROPPED: topic=%s event_id=%s", event.topic, event.event_id)
            self.stats.duplicate_dropped += 1
            return

        await self._dedup_store.mark_processed(event.topic, event.event_id)
        self.event_store[event.topic].append(event.model_dump(mode="json"))
        self.stats.unique_processed += 1
        logger.info("PROCESSED: topic=%s event_id=%s", event.topic, event.event_id)
