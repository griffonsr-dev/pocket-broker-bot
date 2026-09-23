from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class Direction(StrEnum):
    CALL = "call"
    PUT = "put"


class Account(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str = Field(min_length=1, max_length=80)
    credential_ref: str = Field(min_length=1)
    is_master: bool = False
    enabled: bool = True


class BrokerSession(BaseModel):
    account_id: UUID
    key: str
    connected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Trade(BaseModel):
    asset: str = Field(min_length=1, max_length=40)
    direction: Direction
    amount: Decimal = Field(gt=0)
    duration_seconds: int = Field(gt=0, le=86_400)
    correlation_id: UUID = Field(default_factory=uuid4)
    opened_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("asset")
    @classmethod
    def normalize_asset(cls, value: str) -> str:
        return value.strip().upper()


class TradeRequest(BaseModel):
    asset: str = Field(min_length=1, max_length=40)
    direction: Direction
    amount: Decimal = Field(gt=0)
    duration_seconds: int = Field(gt=0, le=86_400)


class Execution(BaseModel):
    account_id: UUID
    broker_trade_id: str
    accepted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))