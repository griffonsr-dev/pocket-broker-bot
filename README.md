# Pocket Copy Master

A small master-authorized copy-trading service. The master opens a position once; every enabled child receives the same asset, direction, amount, duration, and correlation ID concurrently.

## Important integration boundary

This repository uses the opt-in unofficial Pocket Option SDK adapter. It does not include a claim of zero-latency execution, and actual broker/network scheduling can never guarantee literally zero delay.

## Run locally

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pytest
python main.py
```

The application uses `.env` to select an environment. `APP_ENV=demo` loads `.env.demo`; `APP_ENV=live` loads `.env.live`. Both environments use the unofficial SDK; the demo file must contain demo sessions and the live file must contain live sessions. Keep both environment files local and rotate sessions after testing.

Master positions are detected immediately through broker events, with a configurable polling fallback. `POSITION_POLL_INTERVAL_SECONDS` defaults to `0.5`; values below `0.25` are clamped to avoid unnecessary broker pressure. Child orders are sent concurrently, but each remains broker-confirmed before the copy is marked successful.

The API is available at `http://127.0.0.1:8000/docs`.

## Logs

Run `python main.py` with `LOG_LEVEL=INFO` to see account authorization, balance updates, master detection, child order acceptance or failure, broker position IDs, open times, copy latency, and final profit/settlement results. Session credentials and API tokens are never written to logs.

## API

- `GET /health` checks the service.
- `GET /accounts` lists the master and children.
- In `unofficial_sdk` mode, set `MASTER_BROKER_ACCOUNT_ID` and `MASTER_CREDENTIAL_REF` for the master account before starting the service.
- `POST /accounts/children` adds a child. Store only a broker account ID and a reference to a secret-vault entry, for example:

```json
{
  "name": "child-1",
  "broker_account_id": "broker-child-1",
  "credential_ref": "vault/pocket/child-1"
}
```

- `DELETE /accounts/children/{id}` removes a child.
- `POST /accounts/{id}/connect` creates or restores that broker session.
- `GET /accounts/{id}/assets` returns supported assets and durations.
- `GET /accounts/{id}/positions/{broker_position_id}/result` returns the current or final position result.
- `POST /master/positions` opens and copies a position. Send the configured `X-Master-Token` header. The local default is `local-master-token`; set `MASTER_API_TOKEN` before any shared or live deployment. Example:

```json
{
  "asset": "EURUSD",
  "direction": "call",
  "amount": "10",
  "duration_seconds": 60
}
```

- `POST /accounts/{id}/positions` always returns `403`; child accounts cannot initiate positions.

## Production requirements

Before connecting real accounts, add authentication, encrypted credential storage, idempotency and retry policy, broker acknowledgement/reconciliation, audit logs, rate-limit handling, risk limits, and a kill switch. Test with paper/demo accounts first and verify the broker's terms and supported API.

The methods to implement for a permitted live connector are `connect`, `list_assets`, `open_position`, and `get_position_result` on `PocketOptionAdapter` in `app/broker.py`. The adapter currently fails closed with `BrokerNotConfiguredError`; this is intentional because the verified Pocket Option pages do not provide a public developer contract for these operations. The adapter must use a provider-documented API and resolve `credential_ref` through a secret manager; do not put passwords or session tokens in `Account` or source files.

## Unofficial SDK adapter

The repository also includes an opt-in `PocketOptionSdkAdapter` based on the public but unofficial `pocket-option` SDK. Install it with `pip install -e ".[live]"` only after reviewing and pinning the dependency. Set each account's `credential_ref` to an environment variable or secret-manager key containing JSON with `session`, `uid`, `is_demo`, and optional `platform`. The adapter creates one client per account and maps `open_deal` and `check_deal_result` into this service.

Use demo accounts first. Browser SSIDs are session credentials: rotate them after testing, never commit them, and never send them through chat.
