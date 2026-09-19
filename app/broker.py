import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from importlib import import_module
import logging
from typing import Any, Awaitable, Callable, Protocol
from uuid import UUID, uuid4

import aiohttp

from .models import (
    Account,
    AssetInfo,
    BrokerSession,
    Direction,
    ExecutionResult,
    OpenPosition,
    PositionResult,
)


broker_logger = logging.getLogger(__name__)


class BrokerNotConfiguredError(RuntimeError):
    """Raised when live execution is selected without an authorized adapter."""


class BrokerOrderUncertainError(BrokerNotConfiguredError):
    """Raised when the broker may have accepted an order without confirming it."""


class BrokerAdapter(Protocol):
    async def connect(self, account: Account) -> BrokerSession:
        """Create or restore an authenticated broker session."""

    async def list_assets(self, session: BrokerSession) -> list[AssetInfo]:
        """Return assets and durations currently supported by the broker."""

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
        """Open one position using the broker's supported API."""

    async def get_position_result(
        self, session: BrokerSession, broker_position_id: str
    ) -> PositionResult:
        """Fetch the broker's current or final result for a position."""

    async def watch_open_positions(
        self, session: BrokerSession, callback: Callable[[OpenPosition], Awaitable[None]]
    ) -> None:
        """Forward newly opened positions from a broker session to a callback."""

    async def close(self) -> None:
        """Close broker clients and background watchers."""


class PocketOptionAdapter:
    """Integration boundary for an authorized Pocket Option connector.

    This intentionally does not implement undocumented web-socket or private
    endpoints. Supply an official, permitted API implementation here.
    """

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
        raise BrokerNotConfiguredError(
            "Configure an authorized Pocket Option API adapter before live use"
        )

    async def connect(self, account: Account) -> BrokerSession:
        raise BrokerNotConfiguredError(
            "Configure an authorized Pocket Option API adapter before live use"
        )

    async def list_assets(self, session: BrokerSession) -> list[AssetInfo]:
        raise BrokerNotConfiguredError(
            "Configure an authorized Pocket Option API adapter before live use"
        )

    async def get_position_result(
        self, session: BrokerSession, broker_position_id: str
    ) -> PositionResult:
        raise BrokerNotConfiguredError(
            "Configure an authorized Pocket Option API adapter before live use"
        )

    async def watch_open_positions(
        self, session: BrokerSession, callback: Callable[[OpenPosition], Awaitable[None]]
    ) -> None:
        raise BrokerNotConfiguredError(
            "Configure an authorized Pocket Option API adapter before live use"
        )

    async def close(self) -> None:
        raise BrokerNotConfiguredError(
            "Configure an authorized Pocket Option API adapter before live use"
        )


class PocketOptionSdkAdapter:
    """Adapter for the unofficial ``pocket-option`` SDK.

    This uses one Socket.IO client and one browser-derived session per account.
    Credentials are loaded by reference at runtime and never stored in Account.
    The SDK is intentionally optional because it is not an official broker API.
    """

    def __init__(
        self,
        credential_loader: Callable[[str], dict[str, Any]] | None = None,
        poll_interval_seconds: float | None = None,
    ) -> None:
        self._credential_loader = credential_loader or self._load_from_environment
        configured_interval = (
            poll_interval_seconds
            if poll_interval_seconds is not None
            else float(
            os.getenv("POSITION_POLL_INTERVAL_SECONDS", "0.5")
            )
        )
        self._poll_interval_seconds = max(0.25, configured_interval)
        self._clients: dict[str, Any] = {}
        self._deals: dict[str, tuple[str, Any]] = {}
        self._pending_requests: dict[tuple[str, UUID], int] = {}
        self._http_sessions: dict[str, aiohttp.ClientSession] = {}
        self._account_names: dict[str, str] = {}
        self._auth_timeout_seconds = max(
            1.0,
            float(os.getenv("BROKER_AUTH_TIMEOUT_SECONDS", "10")),
        )
        self._forwarded_master_deals: set[str] = set()
        self._processing_master_deals: set[str] = set()
        self._watch_tasks: set[asyncio.Task[None]] = set()

    async def connect(self, account: Account) -> BrokerSession:
        if not account.credential_ref:
            raise BrokerNotConfiguredError(
                f"Account {account.name!r} has no credential_ref"
            )
        broker_logger.info("Connecting account name=%s master=%s", account.name, account.is_master)
        credentials: dict[str, Any] = self._credential_loader(account.credential_ref)
        try:
            pocket_option = import_module("pocket_option")
            models = import_module("pocket_option.models")
            regions = import_module("pocket_option.constants").Regions
            default_init = import_module(
                "pocket_option.contrib.default_init"
            ).default_init
        except ImportError as exc:
            raise BrokerNotConfiguredError(
                "Install the optional unofficial pocket-option SDK before live use"
            ) from exc

        authorization = models.AuthorizationData.model_validate(
            {
                "session": credentials["session"],
                "isDemo": int(credentials.get("is_demo", True)),
                "uid": int(credentials["uid"]),
                "platform": int(credentials.get("platform", 2)),
                "isFastHistory": True,
                "isOptimized": True,
            }
        )
        http_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
        )
        client = pocket_option.PocketOptionClient(logger=False, http_session=http_session)
        default_init(client, authorization=authorization)
        region = regions.DEMO if authorization.is_demo else regions.REAL
        try:
            await client.connect(region)
            await asyncio.wait_for(
                client.authorized_event.wait(),
                timeout=self._auth_timeout_seconds,
            )
        except asyncio.TimeoutError as exc:
            await http_session.close()
            raise BrokerNotConfiguredError(
                "Broker authorization timed out; check credentials and network connectivity"
            ) from exc
        except Exception:
            await http_session.close()
            raise

        session = BrokerSession(account_id=account.id, session_id=str(uuid4()))
        self._clients[session.session_id] = client
        self._http_sessions[session.session_id] = http_session
        self._account_names[session.session_id] = account.name

        async def handle_balance_update(event: Any) -> None:
            broker_logger.info(
                "Balance updated account=%s demo=%s balance=%s",
                account.name,
                event.is_demo,
                event.balance,
            )

        client.on.balance_success_update(handle_balance_update)
        await client.emit.update_balance()
        broker_logger.info("Broker account authorized name=%s demo=%s", account.name, authorization.is_demo)
        return session

    async def list_assets(self, session: BrokerSession) -> list[AssetInfo]:
        client: Any = self._get_client(session)
        assets = await client.assets.get_assets()
        return [
            AssetInfo(
                symbol=str(item.asset),
                durations_seconds=[int(value) for value in (item.timeframes or [])],
            )
            for item in assets
        ]

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
        client: Any = self._get_client(session)
        requested_at = datetime.now(timezone.utc)
        account_name = self._account_names.get(session.session_id, str(session.account_id))
        broker_logger.info(
            "Opening position account=%s asset=%s direction=%s amount=%s duration_seconds=%s requested_at=%s",
            account_name,
            self._normalize_asset(asset),
            direction,
            amount,
            duration_seconds,
            requested_at.isoformat(),
        )
        try:
            models = import_module("pocket_option.models")
            generate_request_id = import_module(
                "pocket_option.utils"
            ).generate_request_id
            deal_errors = import_module("pocket_option.errors")
        except ImportError as exc:
            raise BrokerNotConfiguredError("Pocket Option SDK is not installed") from exc

        request_key = (session.session_id, correlation_id)
        request_id = self._pending_requests.get(request_key)
        if request_id is not None:
            await client.emit.deals_update_opened()
            await asyncio.sleep(0.25)
            deal = await client.deals.get_deal(request_id=request_id)
            if deal is None:
                raise BrokerOrderUncertainError(
                    f"Order request {request_id} has no broker confirmation yet"
                )
            self._pending_requests.pop(request_key, None)
            broker_logger.info(
                "Uncertain order reconciled account=%s request_id=%s broker_position_id=%s",
                account_name,
                request_id,
                deal.id,
            )
        else:
            request_id = generate_request_id()
            self._pending_requests[request_key] = request_id
            try:
                deal = await client.deals.open_deal(
                    asset=models.Asset(self._normalize_asset(asset)),
                    amount=amount,
                    action=models.DealAction(direction),
                    is_demo=int(client.authorization_data.is_demo),
                    time=duration_seconds,
                    request_id=request_id,
                )
            except deal_errors.DealError as exc:
                if exc.code != "timeout":
                    self._pending_requests.pop(request_key, None)
                raise

        broker_position_id = str(deal.id)
        self._deals[broker_position_id] = (session.session_id, deal)
        self._pending_requests.pop(request_key, None)
        opened_at = getattr(deal, "open_time", None) or requested_at
        broker_logger.info(
            "Position opened account=%s broker_position_id=%s opened_at=%s",
            account_name,
            broker_position_id,
            opened_at.isoformat(),
        )
        return ExecutionResult(
            account_id=session.account_id,
            broker_position_id=broker_position_id,
        )

    async def get_position_result(
        self, session: BrokerSession, broker_position_id: str
    ) -> PositionResult:
        client: Any = self._get_client(session)
        deal_record = self._deals.get(broker_position_id)
        deal = deal_record[1] if deal_record else None
        if deal is None:
            try:
                from uuid import UUID as UuidValue

                deal = await client.deals.get_deal(deal_id=UuidValue(broker_position_id))
            except (ValueError, TypeError):
                deal = None
        if deal is None:
            raise ValueError(f"Unknown broker position {broker_position_id}")

        if not deal.closed:
            try:
                deal = await client.deals.check_deal_result(wait_time=1, deal=deal)
            except Exception:
                pass
        result = PositionResult(
            account_id=session.account_id,
            broker_position_id=broker_position_id,
            status="closed" if deal.closed else "open",
            profit=deal.profit if deal.closed else None,
            settled_at=deal.close_time if deal.closed else None,
        )
        broker_logger.info(
            "Position result account=%s broker_position_id=%s status=%s profit=%s settled_at=%s",
            self._account_names.get(session.session_id, str(session.account_id)),
            broker_position_id,
            result.status,
            result.profit,
            result.settled_at.isoformat() if result.settled_at else None,
        )
        return result

    async def watch_open_positions(
        self, session: BrokerSession, callback: Callable[[OpenPosition], Awaitable[None]]
    ) -> None:
        client: Any = self._get_client(session)
        baseline_ready = False

        async def forward_deal(deal: Any) -> None:
            deal_id = str(deal.id)
            if (
                deal_id in self._forwarded_master_deals
                or deal_id in self._processing_master_deals
            ):
                return
            self._processing_master_deals.add(deal_id)
            close_timestamp = getattr(deal, "close_timestamp", None)
            open_timestamp = getattr(deal, "open_timestamp", None)
            duration_seconds = (
                max(1, round(close_timestamp - open_timestamp))
                if close_timestamp is not None and open_timestamp is not None
                else 60
            )
            direction = self._normalize_direction(deal.command)
            asset = getattr(deal.asset, "value", str(deal.asset))
            broker_logger.info(
                "Master position detected asset=%s direction=%s amount=%s duration_seconds=%s",
                asset,
                direction,
                deal.amount,
                duration_seconds,
            )
            try:
                correlation_id = UUID(str(deal_id))
            except (ValueError, TypeError, AttributeError):
                correlation_id = uuid4()
            try:
                await callback(
                    OpenPosition(
                        asset=asset,
                        direction=direction,
                        amount=deal.amount,
                        duration_seconds=duration_seconds,
                        correlation_id=correlation_id,
                    )
                )
            except Exception:
                broker_logger.exception("Master position copy failed deal_id=%s", deal_id)
            else:
                self._forwarded_master_deals.add(deal_id)
                broker_logger.info("Master position copy completed deal_id=%s", deal_id)
            finally:
                self._processing_master_deals.discard(deal_id)

        async def handle_opened_deals(deals: list[Any]) -> None:
            nonlocal baseline_ready
            if not baseline_ready:
                self._forwarded_master_deals.update(str(deal.id) for deal in deals)
                baseline_ready = True
                broker_logger.info("Master open-position baseline captured count=%s", len(deals))
                return
            for deal in deals:
                await forward_deal(deal)

        client.on.deals_success_open(forward_deal)
        client.on.deals_update_opened(handle_opened_deals)
        watch_task = asyncio.create_task(self._poll_open_deals(client))
        self._watch_tasks.add(watch_task)
        watch_task.add_done_callback(self._watch_tasks.discard)
        broker_logger.info("Master position event listeners registered")

    @staticmethod
    def _normalize_asset(asset: str) -> str:
        normalized = asset.strip()
        if normalized.upper().endswith("_OTC"):
            return normalized[:-4] + "_otc"
        return normalized.upper()

    @staticmethod
    def _normalize_direction(command: Any) -> Direction:
        value = getattr(command, "value", command)
        if isinstance(value, int):
            value = {0: Direction.CALL.value, 1: Direction.PUT.value}.get(value)
        if isinstance(value, str):
            value = value.lower()
            if value in {"0", "call"}:
                return Direction.CALL
            if value in {"1", "put"}:
                return Direction.PUT
        raise ValueError(f"Unsupported broker direction: {command!r}")

    async def close(self) -> None:
        for task in self._watch_tasks:
            task.cancel()
        if self._watch_tasks:
            await asyncio.gather(*self._watch_tasks, return_exceptions=True)
        await asyncio.gather(
            *(session.close() for session in self._http_sessions.values()),
            return_exceptions=True,
        )
        self._watch_tasks.clear()
        self._http_sessions.clear()
        self._clients.clear()
        self._processing_master_deals.clear()
        self._pending_requests.clear()

    async def _poll_open_deals(self, client: Any) -> None:
        consecutive_failures = 0
        while True:
            try:
                await client.emit.deals_update_opened()
                consecutive_failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                consecutive_failures += 1
                broker_logger.exception(
                    "Failed to poll master open positions attempt=%s",
                    consecutive_failures,
                )
                if consecutive_failures >= 3:
                    raise RuntimeError(
                        "Master position monitor lost broker connection; reconnect required"
                    ) from exc
            await asyncio.sleep(self._poll_interval_seconds)

    def _get_client(self, session: BrokerSession) -> Any:
        try:
            return self._clients[session.session_id]
        except KeyError as exc:
            raise BrokerNotConfiguredError("Broker session is not connected") from exc

    @staticmethod
    def _load_from_environment(credential_ref: str) -> dict[str, Any]:
        raw = os.environ.get(credential_ref)
        if not raw:
            raise BrokerNotConfiguredError(
                f"Credential reference {credential_ref!r} is not set"
            )
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            if not raw.startswith("42"):
                raise BrokerNotConfiguredError(
                    f"Credential reference {credential_ref!r} must contain JSON"
                ) from exc
            try:
                frame = json.loads(raw[2:])
            except json.JSONDecodeError as frame_exc:
                raise BrokerNotConfiguredError(
                    f"Credential reference {credential_ref!r} must contain JSON"
                ) from frame_exc
            if (
                not isinstance(frame, list)
                or len(frame) != 2
                or frame[0] != "auth"
                or not isinstance(frame[1], dict)
            ):
                raise BrokerNotConfiguredError(
                    f"Credential reference {credential_ref!r} must contain auth JSON"
                )
            value = frame[1]
        if not isinstance(value, dict) or not value.get("session") or not value.get("uid"):
            raise BrokerNotConfiguredError(
                f"Credential reference {credential_ref!r} must contain session and uid"
            )
        return value

