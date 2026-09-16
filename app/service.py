import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone
import logging
from uuid import UUID

from .broker import BrokerAdapter
from .models import (
    Account,
    AssetInfo,
    BrokerSession,
    CopyResult,
    ExecutionResult,
    OpenPosition,
    PositionResult,
)


logger = logging.getLogger(__name__)


class CopyTradingService:
    def __init__(self, broker: BrokerAdapter, master: Account) -> None:
        if not master.is_master:
            raise ValueError("The controlling account must be marked as master")
        self._broker = broker
        self._master = master
        self._children: dict[UUID, Account] = {}
        self._sessions: dict[UUID, BrokerSession] = {}

    @property
    def master(self) -> Account:
        return self._master

    def list_accounts(self) -> list[Account]:
        return [self._master, *self._children.values()]

    def add_child(self, child: Account) -> Account:
        if child.is_master:
            raise ValueError("A child account cannot be marked as master")
        self._children[child.id] = child
        logger.info("Child account added name=%s enabled=%s", child.name, child.enabled)
        return child

    async def connect_account(self, account_id: UUID) -> BrokerSession:
        account = next(
            (candidate for candidate in self.list_accounts() if candidate.id == account_id),
            None,
        )
        if account is None:
            raise ValueError("Unknown account")
        session = await self._broker.connect(account)
        self._sessions[account.id] = session
        logger.info("Account connected name=%s master=%s", account.name, account.is_master)
        return session

    async def list_assets(self, account_id: UUID) -> list[AssetInfo]:
        session = self._sessions.get(account_id) or await self.connect_account(account_id)
        return await self._broker.list_assets(session)

    async def get_position_result(
        self, account_id: UUID, broker_position_id: str
    ) -> PositionResult:
        session = self._sessions.get(account_id) or await self.connect_account(account_id)
        return await self._broker.get_position_result(session, broker_position_id)

    def remove_child(self, account_id: UUID) -> None:
        self._children.pop(account_id, None)

    async def open_from_master(self, position: OpenPosition) -> CopyResult:
        """Open the master order and fan out identical child orders concurrently."""
        master_result, child_results = await asyncio.gather(
            self._open(self._master, position),
            self._open_children(position),
        )
        return CopyResult(
            master_position=master_result,
            child_positions=child_results,
        )

    async def open_master_only(self, position: OpenPosition) -> ExecutionResult:
        return await self._open(self._master, position)

    async def start_master_position_monitor(self) -> None:
        await asyncio.gather(
            *(self.connect_account(child.id) for child in self._children.values() if child.enabled)
        )
        session = self._sessions.get(self._master.id) or await self.connect_account(
            self._master.id
        )

        async def handle_master_position(position: OpenPosition) -> None:
            try:
                await self.copy_to_children(position)
            except Exception:
                logger.exception(
                    "Failed to copy master position asset=%s direction=%s",
                    position.asset,
                    position.direction.value,
                )
                raise
            return None

        await self._broker.watch_open_positions(session, handle_master_position)

    async def close(self) -> None:
        await self._broker.close()

    async def copy_to_children(self, position: OpenPosition) -> list[ExecutionResult]:
        started_at = datetime.now(timezone.utc)
        results = await self._open_children(position)
        logger.info(
            "Master position copied asset=%s direction=%s amount=%s duration_seconds=%s children=%s latency_ms=%s",
            position.asset,
            position.direction.value,
            position.amount,
            position.duration_seconds,
            len(results),
            round((datetime.now(timezone.utc) - started_at).total_seconds() * 1000),
        )
        return results

    async def _open_children(self, position: OpenPosition) -> list[ExecutionResult]:
        return await asyncio.gather(
            *(
                self._open(child, position)
                for child in self._children.values()
                if child.enabled
            )
        )

    async def _open(self, account: Account, position: OpenPosition) -> ExecutionResult:
        session = self._sessions.get(account.id)
        if session is None:
            session = await self.connect_account(account.id)
        try:
            result = await self._broker.open_position(
                session,
                asset=position.asset,
                direction=position.direction.value,
                amount=position.amount,
                duration_seconds=position.duration_seconds,
                correlation_id=position.correlation_id,
            )
        except Exception:
            logger.exception(
                "Position open failed account=%s asset=%s direction=%s amount=%s",
                account.name,
                position.asset,
                position.direction.value,
                position.amount,
            )
            raise
        logger.info(
            "Position accepted account=%s broker_position_id=%s accepted_at=%s",
            account.name,
            result.broker_position_id,
            result.accepted_at.isoformat(),
        )
        return result
