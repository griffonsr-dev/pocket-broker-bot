from fastapi.testclient import TestClient

from app.api import app, master_api_token, service
from tests.fakes import RecordingBroker


service._broker = RecordingBroker()
client = TestClient(app)


def test_master_position_requires_master_token() -> None:
    response = client.post(
        "/master/positions",
        json={
            "asset": "EURUSD",
            "direction": "call",
            "amount": "10",
            "duration_seconds": 60,
        },
    )

    assert response.status_code == 401


def test_child_position_endpoint_is_forbidden() -> None:
    response = client.post(
        "/accounts/not-the-master/positions",
        json={
            "asset": "EURUSD",
            "direction": "call",
            "amount": "10",
            "duration_seconds": 60,
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Only the master account can open positions"


def test_master_position_opens_master_in_live_mode() -> None:
    child = client.post("/accounts/children", json={"name": "api-child"})
    assert child.status_code == 201

    response = client.post(
        "/master/positions",
        json={
            "asset": "GBPUSD",
            "direction": "put",
            "amount": "5",
            "duration_seconds": 30,
        },
        headers={"X-Master-Token": master_api_token},
    )

    assert response.status_code == 200
    assert response.json()["broker_position_id"].startswith("test-")
