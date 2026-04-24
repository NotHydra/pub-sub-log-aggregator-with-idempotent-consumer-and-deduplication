import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from .generator import generate_and_send, AGGREGATOR_URL, EVENT_COUNT, DUPLICATE_RATE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "Publisher ready | aggregator=%s event_count=%d duplicate_rate=%.0f%%",
        AGGREGATOR_URL,
        EVENT_COUNT,
        DUPLICATE_RATE * 100,
    )
    yield


app = FastAPI(title="Log Publisher", version="1.0.0", lifespan=lifespan)


@app.post("/run", status_code=202)
async def run_publisher() -> dict:
    result = await generate_and_send()
    logger.info("Publish run complete: %s", result)
    return result


@app.get("/status")
async def status() -> dict:
    return {
        "aggregator_url": AGGREGATOR_URL,
        "event_count": EVENT_COUNT,
        "duplicate_rate": DUPLICATE_RATE,
    }


if __name__ == "__main__":
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8081")),
        reload=False,
    )
