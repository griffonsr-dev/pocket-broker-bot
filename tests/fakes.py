from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

from app.models import (
    Account,
    AssetInfo,
    BrokerSession,
    ExecutionResult,
    OpenPosition,
    PositionResult,
)


class RecordingBroker:
    def __init__(self) -> None:
        self.positions: list[Mapping[str, object]] = []

    async def connect(self, account: Account) -> BrokerSession:
        return BrokerSession(account_id=account.id, session_id=f"test-session-{account.id}")

    async def list_assets(self, session: BrokerSession) -> list[AssetInfo]:
        return [AssetInfo(symbol="EURUSD", durations_seconds=[30, 60, 300])]

    async def open_position(
        self,
        session: BrokerSession,
        *,
        asset: str,
        direction: str,
        amount: Decimal,
        duration_seconds: int,
        correlation_id: UUID,
    ) -> ExecutionResult:
        if getattr(self, "fail_next_open", False):
            self.fail_next_open = False
            raise RuntimeError("temporary child order failure")
        self.positions.append(
            {
                "account_id": session.account_id,
                "asset": asset,
                "direction": direction,
                "amount": amount,
                "duration_seconds": duration_seconds,
                "correlation_id": correlation_id,
            }
        )
        return ExecutionResult(
            account_id=session.account_id,
            broker_position_id=f"test-{uuid4()}",
            accepted_at=datetime.now(timezone.utc),
        )

    async def get_position_result(
        self, session: BrokerSession, broker_position_id: str
    ) -> PositionResult:
        return PositionResult(
            account_id=session.account_id,
            broker_position_id=broker_position_id,
            status="open",
        )

    async def watch_open_positions(self, session: BrokerSession, callback) -> None:
        self.callback = callback

    async def close(self) -> None:
        return None
