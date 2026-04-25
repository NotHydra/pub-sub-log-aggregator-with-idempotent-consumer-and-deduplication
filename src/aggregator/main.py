import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI

from .consumer import Consumer
from .dedup_store import DedupStore
from .routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_path = os.getenv("DEDUP_DB_PATH", "/app/data/dedup.db")
    queue: asyncio.Queue = asyncio.Queue()
    dedup_store = DedupStore(db_path=db_path)
    consumer = Consumer(queue=queue, dedup_store=dedup_store)

    app.state.queue = queue
    app.state.consumer = consumer
    app.state.start_time = datetime.now(timezone.utc)

    task = asyncio.create_task(consumer.run())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Log Aggregator", version="1.0.0", lifespan=lifespan)
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
        reload=False,
    )
