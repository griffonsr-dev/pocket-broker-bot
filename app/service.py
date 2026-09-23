import asyncio
import logging
from uuid import UUID

from .broker import Broker
from .models import Account, BrokerSession, Execution, Trade

logger = logging.getLogger(__name__)


class MirrorService:
    def __init__(self, broker: Broker, master: Account, children: list[Account]) -> None:
        if not master.is_master:
            raise ValueError("master account must be marked as master")
        if any(child.is_master for child in children):
            raise ValueError("child accounts cannot be masters")
        self.broker = broker
        self.master = master
        self.children = {
            child.id: child for child in children if child.enabled
        }
        self.sessions: dict[UUID, BrokerSession] = {}
        self._inflight: set[UUID] = set()

    async def start(self) -> None:
        accounts = [self.master, *self.children.values()]
        await asyncio.gather(*(self._connect(account) for account in accounts))
        await self.broker.watch_master(
            self.sessions[self.master.id], self.mirror_trade
        )

    async def _connect(self, account: Account) -> BrokerSession:
        session = await self.broker.connect(account)
        self.sessions[account.id] = session
        return session

    async def mirror_trade(self, trade: Trade) -> list[Execution]:
        if trade.correlation_id in self._inflight:
            return []
        self._inflight.add(trade.correlation_id)
        try:
            outcomes = await asyncio.gather(
                *(
                    self.broker.open(self.sessions[child.id], trade)
                    for child in self.children.values()
                ),
                return_exceptions=True,
            )
            executions = [
                result for result in outcomes if isinstance(result, Execution)
            ]
            failures = [result for result in outcomes if isinstance(result, Exception)]
            if failures:
                logger.error(
                    "child copy failures=%s correlation_id=%s",
                    len(failures),
                    trade.correlation_id,
                )
            return executions
        finally:
            self._inflight.discard(trade.correlation_id)

    async def open_master(self, trade: Trade) -> Execution:
        return await self.broker.open(self.sessions[self.master.id], trade)

    def accounts(self) -> list[Account]:
        return [self.master, *self.children.values()]

    async def close(self) -> None:
        await self.broker.close()