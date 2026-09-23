import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from app.broker import PocketOptionBroker
from app.models import Account, BrokerSession, Direction, Execution, Trade
from app.service import MirrorService


class FakeBroker:
    def __init__(self) -> None:
        self.connected: list[Account] = []
        self.opened: list[tuple[UUID, Trade]] = []
        self.callback = None

    async def connect(self, account: Account) -> BrokerSession:
        self.connected.append(account)
        return BrokerSession(account_id=account.id, key=account.name)

    async def open(self, session: BrokerSession, trade: Trade) -> Execution:
        self.opened.append((session.account_id, trade))
        return Execution(account_id=session.account_id, broker_trade_id=session.key)

    async def watch_master(self, session: BrokerSession, callback) -> None:
        self.callback = callback

    async def close(self) -> None:
        return None


def test_credentials_support_legacy_42_serialized_payload() -> None:
    raw = '42["auth",{"session":"abc123","isDemo":1,"uid":84037571,"platform":2,"isFastHistory":true,"isOptimized":true}]'

    parsed = PocketOptionBroker._credentials_raw(raw)

    assert parsed["session"] == "abc123"
    assert parsed["uid"] == 84037571
    assert parsed["is_demo"] is True
    assert parsed["platform"] == 2


def test_duplicate_master_deal_ids_are_ignored_in_short_window() -> None:
    broker = PocketOptionBroker()

    assert broker._remember_master_deal("deal-1") is True
    assert broker._remember_master_deal("deal-1") is False
    assert broker._remember_master_deal("deal-2") is True


@pytest.mark.asyncio
async def test_start_warms_master_and_children_before_listener() -> None:
    broker = FakeBroker()
    master = Account(name="master", credential_ref="MASTER", is_master=True)
    child = Account(name="child", credential_ref="CHILD")
    service = MirrorService(broker, master, [child])

    await service.start()

    assert broker.connected == [master, child]
    assert broker.callback is not None


@pytest.mark.asyncio
async def test_trade_is_sent_to_all_children_concurrently() -> None:
    broker = FakeBroker()
    master = Account(name="master", credential_ref="MASTER", is_master=True)
    children = [
        Account(name="child-1", credential_ref="CHILD_1"),
        Account(name="child-2", credential_ref="CHILD_2"),
    ]
    service = MirrorService(broker, master, children)
    await service.start()

    trade = Trade(
        asset="eurusd",
        direction=Direction.CALL,
        amount=Decimal("10"),
        duration_seconds=60,
    )
    results = await service.mirror_trade(trade)

    assert len(results) == 2
    assert len(broker.opened) == 2
    assert {item[1].asset for item in broker.opened} == {"EURUSD"}
    assert {item[1].correlation_id for item in broker.opened} == {trade.correlation_id}


@pytest.mark.asyncio
async def test_duplicate_trade_is_ignored_while_first_copy_is_in_flight() -> None:
    broker = FakeBroker()
    master = Account(name="master", credential_ref="MASTER", is_master=True)
    child = Account(name="child", credential_ref="CHILD")
    service = MirrorService(broker, master, [child])
    await service.start()
    trade = Trade(
        asset="EURUSD",
        direction=Direction.PUT,
        amount=Decimal("5"),
        duration_seconds=30,
    )

    first, second = await asyncio.gather(
        service.mirror_trade(trade), service.mirror_trade(trade)
    )

    assert len(first) + len(second) == 1
    assert len(broker.opened) == 1