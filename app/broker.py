import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from importlib import import_module
from typing import Any, Awaitable, Callable, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

import aiohttp

from .models import Account, BrokerSession, Direction, Execution, Trade

logger = logging.getLogger(__name__)


class BrokerUnavailable(RuntimeError):
    """The configured broker cannot accept work."""


class Broker(Protocol):
    async def connect(self, account: Account) -> BrokerSession: ...

    async def open(self, session: BrokerSession, trade: Trade) -> Execution: ...

    async def watch_master(
        self, session: BrokerSession, callback: Callable[[Trade], Awaitable[None]]
    ) -> None: ...

    async def close(self) -> None: ...


class PocketOptionBroker:
    """Persistent event-driven adapter for the installed Pocket Option SDK."""

    def __init__(self) -> None:
        self._clients: dict[str, Any] = {}
        self._http_sessions: dict[str, aiohttp.ClientSession] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._seen_master_deals: dict[str, datetime] = {}
        self._seen_master_deals_ttl_seconds = float(
            os.getenv("MASTER_DEAL_DEDUPE_SECONDS", "30")
        )
        self._balance_refresh_pending: dict[str, bool] = {}

    def _remember_master_deal(self, deal_id: str) -> bool:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=self._seen_master_deals_ttl_seconds)
        for seen_key, seen_at in list(self._seen_master_deals.items()):
            if seen_at < cutoff:
                del self._seen_master_deals[seen_key]
        if deal_id in self._seen_master_deals:
            logger.warning("duplicate master deal ignored deal_id=%s", deal_id)
            return False
        self._seen_master_deals[deal_id] = now
        return True

    async def connect(self, account: Account) -> BrokerSession:
        credentials = self._credentials(account.credential_ref)
        try:
            pocket_option = import_module("pocket_option")
            models = import_module("pocket_option.models")
            regions = import_module("pocket_option.constants").Regions
            default_init = import_module(
                "pocket_option.contrib.default_init"
            ).default_init
        except ImportError as exc:
            raise BrokerUnavailable("pocket-option is not installed") from exc

        authorization = models.AuthorizationData.model_validate(
            {
                "session": credentials["session"],
                "uid": int(credentials["uid"]),
                "isDemo": int(credentials.get("is_demo", True)),
                "platform": int(credentials.get("platform", 2)),
                "isFastHistory": True,
                "isOptimized": True,
            }
        )
        http_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                resolver=aiohttp.resolver.ThreadedResolver(),
                limit=0,
                ttl_dns_cache=300,
                keepalive_timeout=30,
            )
        )
        client = pocket_option.PocketOptionClient(
            logger=False, http_session=http_session
        )
        default_init(client, authorization=authorization)

        async def on_balance_updated(event: Any) -> None:
            if not self._balance_refresh_pending.get(account.name, False):
                return
            self._balance_refresh_pending[account.name] = False
            logger.info(
                "account balance updated account=%s balance=%s is_demo=%s",
                account.name,
                getattr(event, "balance", None),
                getattr(event, "is_demo", None),
            )

        client.on.balance_success_update(on_balance_updated)

        async def on_deal_closed(event: Any) -> None:
            self._balance_refresh_pending[account.name] = True
            logger.info(
                "trade finished account=%s profit=%s",
                account.name,
                getattr(event, "profit", None),
            )
            await client.emit.update_balance()

        client.on.deals_success_close(on_deal_closed)
        try:
            region = regions.DEMO if authorization.is_demo else regions.REAL
            await client.connect(region)
            await asyncio.wait_for(
                client.authorized_event.wait(),
                timeout=float(os.getenv("BROKER_AUTH_TIMEOUT_SECONDS", "15")),
            )
        except Exception:
            await http_session.close()
            raise

        session = BrokerSession(account_id=account.id, key=account.name)
        self._clients[session.key] = client
        self._http_sessions[session.key] = http_session
        logger.info("broker connected account=%s demo=%s", account.name, authorization.is_demo)
        return session

    async def open(self, session: BrokerSession, trade: Trade) -> Execution:
        client = self._client(session)
        try:
            models = import_module("pocket_option.models")
            request_id_factory = import_module(
                "pocket_option.utils"
            ).generate_request_id
        except ImportError as exc:
            raise BrokerUnavailable("pocket-option is not installed") from exc

        started = datetime.now(timezone.utc)
        deal = await client.deals.open_deal(
            asset=models.Asset(self._broker_asset(trade.asset)),
            amount=trade.amount,
            action=models.DealAction(trade.direction.value),
            is_demo=int(client.authorization_data.is_demo),
            time=trade.duration_seconds,
            request_id=request_id_factory(),
        )
        latency_ms = (datetime.now(timezone.utc) - started).total_seconds() * 1000
        logger.info(
            "trade accepted account=%s asset=%s direction=%s latency_ms=%.1f",
            session.key,
            trade.asset,
            trade.direction.value,
            latency_ms,
        )
        return Execution(account_id=session.account_id, broker_trade_id=str(deal.id))

    async def watch_master(
        self, session: BrokerSession, callback: Callable[[Trade], Awaitable[None]]
    ) -> None:
        client = self._client(session)

        async def on_opened(deal: Any) -> None:
            deal_id = str(deal.id)
            if not self._remember_master_deal(deal_id):
                return
            opened = getattr(deal, "open_timestamp", None)
            closed = getattr(deal, "close_timestamp", None)
            duration = max(1, round(closed - opened)) if opened and closed else 60
            asset = getattr(deal.asset, "value", str(deal.asset))
            direction = self._direction(deal.command)
            logger.info(
                "master deal received deal_id=%s asset=%s direction=%s amount=%s",
                deal_id,
                asset,
                direction.value,
                getattr(deal, "amount", None),
            )
            task = asyncio.create_task(
                callback(
                    Trade(
                        asset=asset,
                        direction=direction,
                        amount=deal.amount,
                        duration_seconds=duration,
                        correlation_id=self._stable_uuid(deal_id),
                    )
                )
            )
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        client.on.deals_success_open(on_opened)
        logger.info("master event listener ready account=%s", session.key)

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await asyncio.gather(
            *(session.close() for session in self._http_sessions.values()),
            return_exceptions=True,
        )
        self._tasks.clear()
        self._clients.clear()
        self._http_sessions.clear()

    def _client(self, session: BrokerSession) -> Any:
        try:
            return self._clients[session.key]
        except KeyError as exc:
            raise BrokerUnavailable(f"account {session.key!r} is not connected") from exc

    @staticmethod
    def _credentials_raw(raw: str) -> dict[str, Any]:
        value: Any
        payload = raw.strip()
        if not payload:
            raise BrokerUnavailable("credential payload is empty")
        if payload.startswith("42["):
            payload = payload[2:]

        try:
            value = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise BrokerUnavailable("credential payload is not valid JSON") from exc

        if isinstance(value, list) and len(value) == 2 and isinstance(value[1], dict):
            value = value[1]

        if not isinstance(value, dict):
            raise BrokerUnavailable("credential payload must decode to an object")

        normalized = dict(value)
        if "is_demo" not in normalized and "isDemo" in normalized:
            normalized["is_demo"] = bool(normalized["isDemo"])
        return normalized

    @staticmethod
    def _credentials(reference: str) -> dict[str, Any]:
        raw = os.getenv(reference)
        if not raw:
            raise BrokerUnavailable(f"credential variable {reference!r} is missing")

        try:
            value = PocketOptionBroker._credentials_raw(raw)
        except BrokerUnavailable:
            raise

        if not value.get("session") or not value.get("uid"):
            raise BrokerUnavailable(f"credential variable {reference!r} is incomplete")
        return value

    @staticmethod
    def _broker_asset(asset: str) -> str:
        if asset.upper().endswith("_OTC"):
            return asset[:-4] + "_otc"
        return asset

    @staticmethod
    def _direction(value: Any) -> Direction:
        value = getattr(value, "value", value)
        if value in (0, "0", "call"):
            return Direction.CALL
        if value in (1, "1", "put"):
            return Direction.PUT
        raise ValueError(f"unsupported broker direction {value!r}")

    @staticmethod
    def _stable_uuid(value: str) -> UUID:
        try:
            return UUID(value)
        except ValueError:
            return uuid5(NAMESPACE_URL, value)