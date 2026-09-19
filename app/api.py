import hmac
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, status

project_root = Path(__file__).resolve().parent.parent
requested_environment = os.getenv("APP_ENV")
load_dotenv(project_root / ".env")
environment = requested_environment or os.getenv("APP_ENV", "demo")
load_dotenv(project_root / f".env.{environment}", override=True)
application_logger = logging.getLogger("app")
application_logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
if not application_logger.handlers:
    application_handler = logging.StreamHandler()
    application_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    application_logger.addHandler(application_handler)
application_logger.propagate = False

from .broker import BrokerAdapter, BrokerNotConfiguredError, PocketOptionSdkAdapter
from .models import Account, ChildAccountCreate, OpenPosition, PositionOpenRequest
from .service import CopyTradingService


logger = logging.getLogger(__name__)


master = Account(
    name="master",
    is_master=True,
    broker_account_id=os.getenv("MASTER_BROKER_ACCOUNT_ID"),
    credential_ref=os.getenv("MASTER_CREDENTIAL_REF", "PO_MASTER_CREDENTIALS"),
)
broker_mode = os.getenv("BROKER_MODE", "unofficial_sdk").lower()
broker: BrokerAdapter
if broker_mode == "unofficial_sdk":
    broker = PocketOptionSdkAdapter()
else:
    raise RuntimeError(f"Unsupported BROKER_MODE: {broker_mode}")
service = CopyTradingService(broker, master)
master_api_token = os.getenv("MASTER_API_TOKEN", "local-master-token")


def require_master_token(x_master_token: str | None = Header(default=None)) -> str:
    if x_master_token is None:
        raise HTTPException(status_code=401, detail="Valid master token required")
    try:
        if not hmac.compare_digest(x_master_token, master_api_token):
            raise HTTPException(status_code=401, detail="Valid master token required")
    except TypeError as exc:
        raise HTTPException(status_code=401, detail="Valid master token required") from exc
    return x_master_token


async def start_master_position_monitor() -> None:
    first_child_credential_ref = os.getenv("CHILD_CREDENTIAL_REF")
    if not first_child_credential_ref:
        first_child_credential_ref = (
            "PO_CHILD_CREDENTIALS"
            if os.getenv("PO_CHILD_CREDENTIALS")
            else "PO_CHILD_1_CREDENTIALS"
        )
    child_configs = [
        (
            os.getenv("CHILD_ACCOUNT_NAME", "child"),
            os.getenv("CHILD_BROKER_ACCOUNT_ID"),
            first_child_credential_ref,
        ),
        (
            os.getenv("CHILD_2_ACCOUNT_NAME", "child-2"),
            os.getenv("CHILD_2_BROKER_ACCOUNT_ID"),
            os.getenv("CHILD_2_CREDENTIAL_REF", "PO_CHILD_2_CREDENTIALS"),
        ),
    ]
    for child_name, broker_account_id, child_credential_ref in child_configs:
        if child_credential_ref and os.getenv(child_credential_ref):
            service.add_child(
                Account(
                    name=child_name,
                    broker_account_id=broker_account_id,
                    credential_ref=child_credential_ref,
                )
            )
            logger.info("Child account loaded name=%s", child_name)
    logger.info("Starting broker mode=%s environment=%s", broker_mode, environment)
    if broker_mode == "unofficial_sdk":
        try:
            await service.start_master_position_monitor()
            logger.info("Master position monitor started")
        except BrokerNotConfiguredError as exc:
            logger.warning(
                "Broker startup failed; continuing in degraded mode: %s",
                exc,
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_master_position_monitor()
    try:
        yield
    finally:
        await service.close()


app = FastAPI(title="Pocket Copy Master", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "mode": broker_mode}


@app.get("/accounts", response_model=list[Account])
async def accounts(_token: str = Depends(require_master_token)) -> list[Account]:
    return service.list_accounts()


@app.post("/accounts/children", response_model=Account, status_code=status.HTTP_201_CREATED)
async def add_child(
    payload: ChildAccountCreate,
    _token: str = Depends(require_master_token),
) -> Account:
    return service.add_child(Account(**payload.model_dump()))


@app.delete("/accounts/children/{account_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_child(
    account_id: str,
    _token: str = Depends(require_master_token),
) -> None:
    from uuid import UUID

    try:
        service.remove_child(UUID(account_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid account id") from exc


@app.post("/master/positions")
async def open_master_position(
    payload: PositionOpenRequest,
    _token: str = Depends(require_master_token),
) -> object:
    position = OpenPosition(**payload.model_dump())
    try:
        if broker_mode == "unofficial_sdk":
            return await service.open_master_only(position)
        return await service.open_from_master(position)
    except BrokerNotConfiguredError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Live broker is not configured; install the optional SDK or switch to demo mode",
        ) from exc


@app.post("/accounts/{account_id}/connect")
async def connect_account(
    account_id: str,
    _token: str = Depends(require_master_token),
) -> object:
    from uuid import UUID

    try:
        return await service.connect_account(UUID(account_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Unknown account") from exc
    except BrokerNotConfiguredError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Live broker is not configured; install the optional SDK or switch to demo mode",
        ) from exc


@app.get("/accounts/{account_id}/assets")
async def list_assets(
    account_id: str,
    _token: str = Depends(require_master_token),
) -> object:
    from uuid import UUID

    try:
        return await service.list_assets(UUID(account_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Unknown account") from exc
    except BrokerNotConfiguredError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Live broker is not configured; install the optional SDK or switch to demo mode",
        ) from exc


@app.get("/accounts/{account_id}/positions/{broker_position_id}/result")
async def position_result(
    account_id: str,
    broker_position_id: str,
    _token: str = Depends(require_master_token),
) -> object:
    from uuid import UUID

    try:
        return await service.get_position_result(UUID(account_id), broker_position_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Unknown account") from exc
    except BrokerNotConfiguredError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Live broker is not configured; install the optional SDK or switch to demo mode",
        ) from exc


@app.post("/accounts/{account_id}/positions", status_code=status.HTTP_403_FORBIDDEN)
async def reject_child_position(account_id: str) -> None:
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Only the master account can open positions",
    )
