# Railway Trade Mirror

This service mirrors master trades to enabled child accounts using persistent broker sessions and concurrent order submission.

The latency path is intentionally small: the master broker event is received, converted to a `Trade`, and all child `open_deal` calls are started together with `asyncio.gather`. Sessions are connected before the listener is registered, DNS is cached, HTTP connections are kept alive, and there is no polling loop, database round trip, queue, or intentional sleep. Literal zero latency is impossible because broker and network execution time still applies.

## Environment files and Railway variables

`.env.demo` and `.env.live` are included as local templates. Replace their placeholder values locally, and never commit real broker sessions. Select one with `APP_ENV=demo` or `APP_ENV=live`. Railway should use the same variables in its service Variables settings instead of uploading these files.

```text
MASTER_API_TOKEN=<long-random-value>
MASTER_CREDENTIAL_REF=PO_MASTER_CREDENTIALS
PO_MASTER_CREDENTIALS={"session":"...","uid":123,"is_demo":true}
CHILD_ACCOUNTS_JSON=[{"name":"child-1","credential_ref":"PO_CHILD_1_CREDENTIALS"}]
PO_CHILD_1_CREDENTIALS={"session":"...","uid":456,"is_demo":true}
```

Add more children to `CHILD_ACCOUNTS_JSON`. Use demo credentials first. Credentials are read from environment variables and never logged.

## Start locally

```powershell
python -m pip install -r requirements.txt
$env:MASTER_API_TOKEN = "local-token"
python main.py
```

Health: `GET /health`.

Open a master trade with `POST /master/trades` and the `X-Master-Token` header:

```json
{"asset":"EURUSD","direction":"call","amount":"10","duration_seconds":60}
```

The master endpoint opens the master trade. The broker's open-trade event then mirrors it to the children. Direct child trade requests are rejected.

## Important limitation

`pocket-option` is an unofficial SDK. Use it only where permitted. A provider-supported API, a Railway region close to the broker, and a colocated execution service are required for stronger latency guarantees.