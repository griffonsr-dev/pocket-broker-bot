import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException

from .broker import BrokerUnavailable, PocketOptionBroker
from .models import Account, Trade, TradeRequest
from .service import MirrorService

project_root = Path(__file__).resolve().parent.parent
environment = os.getenv("APP_ENV", "demo").lower()
load_dotenv(project_root / f".env.{environment}")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


def load_account(name: str, credential_ref: str, *, is_master: bool) -> Account:
    return Account(name=name, credential_ref=credential_ref, is_master=is_master)


def load_children() -> list[Account]:
    try:
        raw_children = json.loads(os.getenv("CHILD_ACCOUNTS_JSON", "[]"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("CHILD_ACCOUNTS_JSON must be valid JSON") from exc
    if not isinstance(raw_children, list):
        raise RuntimeError("CHILD_ACCOUNTS_JSON must be a JSON list")
    return [
        load_account(str(item["name"]), str(item["credential_ref"]), is_master=False)
        for item in raw_children
    ]


master = load_account(
    os.getenv("MASTER_NAME", "master"),
    os.getenv("MASTER_CREDENTIAL_REF", "PO_MASTER_CREDENTIALS"),
    is_master=True,
)
broker = PocketOptionBroker()
service = MirrorService(broker, master, load_children())
api_token = os.getenv("MASTER_API_TOKEN")
if not api_token:
    raise RuntimeError("MASTER_API_TOKEN must be configured")


def require_token(
    token: str | None = Header(default=None, alias="X-Master-Token"),
) -> None:
    if token is None or not hmac.compare_digest(token, api_token):
        raise HTTPException(status_code=401, detail="invalid master token")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await service.start()
    try:
        yield
    finally:
        await service.close()


app = FastAPI(title="Railway Trade Mirror", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, object]:
    return {"status": "ok", "connected_accounts": len(service.sessions)}


@app.get("/accounts", response_model=list[Account])
async def accounts(_: None = Depends(require_token)) -> list[Account]:
    return service.accounts()


@app.post("/master/trades", response_model=object)
async def open_master_trade(
    payload: TradeRequest, _: None = Depends(require_token)
) -> object:
    try:
        return await service.open_master(Trade(**payload.model_dump()))
    except BrokerUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/accounts/{account_id}/trades", status_code=403)
async def reject_child_trade(account_id: str) -> None:
    raise HTTPException(status_code=403, detail="only the master can open trades")