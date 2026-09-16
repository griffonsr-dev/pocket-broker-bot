from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Direction(StrEnum):
    CALL = "call"
    PUT = "put"


class Account(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(min_length=1, max_length=80)
    is_master: bool = False
    enabled: bool = True
    broker_account_id: str | None = None
    credential_ref: str | None = None


class BrokerSession(BaseModel):
    account_id: UUID
    session_id: str
    connected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AssetInfo(BaseModel):
    symbol: str
    durations_seconds: list[int]


class PositionResult(BaseModel):
    account_id: UUID
    broker_position_id: str
    status: str
    profit: Decimal | None = None
    settled_at: datetime | None = None


class OpenPosition(BaseModel):
    asset: str = Field(min_length=1, max_length=40)
    direction: Direction
    amount: Decimal = Field(gt=0)
    duration_seconds: int = Field(gt=0, le=86_400)
    opened_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: UUID = Field(default_factory=uuid4)

    @field_validator("asset")
    @classmethod
    def normalize_asset(cls, value: str) -> str:
        return value.strip().upper()


class ExecutionResult(BaseModel):
    account_id: UUID
    broker_position_id: str
    accepted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ChildAccountCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    broker_account_id: str | None = None
    credential_ref: str | None = None


class PositionOpenRequest(BaseModel):
    asset: str = Field(min_length=1, max_length=40)
    direction: Direction
    amount: Decimal = Field(gt=0)
    duration_seconds: int = Field(gt=0, le=86_400)


class CopyResult(BaseModel):
    master_position: ExecutionResult
    child_positions: list[ExecutionResult]
