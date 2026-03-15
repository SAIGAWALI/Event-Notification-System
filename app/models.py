from enum import Enum
from pydantic import BaseModel
from typing import Dict


class EventType(str, Enum):
    EMAIL = "EMAIL"
    SMS = "SMS"
    PUSH = "PUSH"


class EventRequest(BaseModel):
    eventType: EventType
    payload: Dict
    callbackUrl: str