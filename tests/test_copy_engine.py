from decimal import Decimal

import pytest

from app.broker import (
    BrokerNotConfiguredError,
    BrokerOrderUncertainError,
    PocketOptionAdapter,
    PocketOptionSdkAdapter,
)
from app.models import Account, Direction, OpenPosition
from app.service import CopyTradingService
from tests.fakes import RecordingBroker


@pytest.mark.asyncio
async def test_master_event_retries_after_child_copy_failure() -> None:
    broker = RecordingBroker()
    service = CopyTradingService(broker, Account(name="master", is_master=True))
    child = service.add_child(Account(name="child"))
    await service.start_master_position_monitor()

    position = OpenPosition(
        asset="EURUSD",
        direction=Direction.CALL,
        amount=Decimal("10"),
        duration_seconds=60,
    )
    broker.fail_next_open = True
    with pytest.raises(RuntimeError, match="temporary"):
        await broker.callback(position)
    await broker.callback(position)

    assert [item["account_id"] for item in broker.positions] == [child.id]


@pytest.mark.asyncio
async def test_partial_child_failure_keeps_successful_children_opening() -> None:
    class FailingChildBroker(RecordingBroker):
        def __init__(self) -> None:
            super().__init__()
            self.fail_on_account = None

        async def open_position(
            self,
            session,
            *,
            asset,
            direction,
            amount,
            duration_seconds,
            correlation_id,
        ):
            if self.fail_on_account is not None and session.account_id == self.fail_on_account:
                raise RuntimeError("temporary child order failure")
            return await super().open_position(
                session,
                asset=asset,
                direction=direction,
                amount=amount,
                duration_seconds=duration_seconds,
                correlation_id=correlation_id,
            )

    broker = FailingChildBroker()
    service = CopyTradingService(broker, Account(name="master", is_master=True))
    first = service.add_child(Account(name="first"))
    second = service.add_child(Account(name="second"))
    broker.fail_on_account = first.id

    position = OpenPosition(
        asset="EURUSD",
        direction=Direction.CALL,
        amount=Decimal("10"),
        duration_seconds=60,
    )

    result = await service.open_from_master(position)

    assert len(result.child_positions) == 1
    assert result.child_positions[0].account_id == second.id
    assert len(broker.positions) == 2
    assert {item["account_id"] for item in broker.positions} == {service.master.id, second.id}


@pytest.mark.asyncio
async def test_master_position_is_copied_to_enabled_children() -> None:
    broker = RecordingBroker()
    service = CopyTradingService(broker, Account(name="master", is_master=True))
    first = service.add_child(Account(name="child-one"))
    second = service.add_child(Account(name="child-two"))
    disabled = service.add_child(Account(name="disabled", enabled=False))

    position = OpenPosition(
        asset="eurusd",
        direction=Direction.CALL,
        amount=Decimal("12.50"),
        duration_seconds=60,
    )
    result = await service.open_from_master(position)

    assert len(result.child_positions) == 2
    assert {item.account_id for item in result.child_positions} == {first.id, second.id}
    assert disabled.id not in {item.account_id for item in result.child_positions}
    assert len(broker.positions) == 3
    assert {item["asset"] for item in broker.positions} == {"EURUSD"}
    assert {item["direction"] for item in broker.positions} == {"call"}
    assert {item["amount"] for item in broker.positions} == {Decimal("12.50")}
    assert {item["duration_seconds"] for item in broker.positions} == {60}
    assert {item["correlation_id"] for item in broker.positions} == {position.correlation_id}


@pytest.mark.asyncio
async def test_master_event_is_copied_to_children() -> None:
    broker = RecordingBroker()
    service = CopyTradingService(broker, Account(name="master", is_master=True))
    child = service.add_child(Account(name="child"))

    await service.start_master_position_monitor()
    position = OpenPosition(
        asset="EURUSD",
        direction=Direction.PUT,
        amount=Decimal("10"),
        duration_seconds=60,
    )
    await broker.callback(position)

    assert [item["account_id"] for item in broker.positions] == [child.id]
    assert broker.positions[0]["direction"] == "put"


@pytest.mark.asyncio
async def test_child_cannot_become_the_authority() -> None:
    broker = RecordingBroker()
    with pytest.raises(ValueError, match="master"):
        CopyTradingService(broker, Account(name="child", is_master=False))


@pytest.mark.asyncio
async def test_live_adapter_fails_closed_until_documented_connector_is_supplied() -> None:
    adapter = PocketOptionAdapter()

    with pytest.raises(BrokerNotConfiguredError):
        await adapter.connect(Account(name="live", broker_account_id="broker-1"))


def test_sdk_adapter_requires_secret_reference_value() -> None:
    with pytest.raises(BrokerNotConfiguredError, match="not set"):
        PocketOptionSdkAdapter._load_from_environment("MISSING_TEST_CREDENTIAL")


def test_sdk_adapter_accepts_socket_auth_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "SOCKET_AUTH_TEST",
        '42["auth",{"session":"session-value","uid":123456}]',
    )

    credentials = PocketOptionSdkAdapter._load_from_environment("SOCKET_AUTH_TEST")

    assert credentials["session"] == "session-value"
    assert credentials["uid"] == 123456


@pytest.mark.parametrize(
    ("command", "expected"),
    [(0, Direction.CALL), (1, Direction.PUT), ("call", Direction.CALL), ("put", Direction.PUT)],
)
def test_sdk_adapter_normalizes_broker_direction(command: object, expected: Direction) -> None:
    assert PocketOptionSdkAdapter._normalize_direction(command) == expected


@pytest.mark.parametrize(
    ("asset", "expected"),
    [("AUDCAD_OTC", "AUDCAD_otc"), ("EURUSD", "EURUSD")],
)
def test_sdk_adapter_normalizes_broker_asset(asset: str, expected: str) -> None:
    assert PocketOptionSdkAdapter._normalize_asset(asset) == expected
