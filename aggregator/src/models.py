from datetime import datetime
from typing import Any
from pydantic import BaseModel, field_validator


class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: dict[str, Any]

    @field_validator("topic", "event_id", "source")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("field must not be empty")
        return v


class PublishRequest(BaseModel):
    events: list[Event]
