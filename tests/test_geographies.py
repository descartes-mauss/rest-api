import datetime
from typing import Generator, List, Optional, Tuple

import pytest
from fastapi.testclient import TestClient

from database.public_models.models import ClientGeography, Geography
from jwt_validator import validate_jwt
from main import app
from routes.geography_router import get_geography_service
from services.geography_service import GeographyService


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides = {}


def make_geography(geography_id: str = "US", name: str = "United States") -> Geography:
    return Geography(
        geography_id=geography_id,
        name=name,
        is_active=True,
        is_region=False,
        created_on=datetime.datetime.now(datetime.UTC),
        updated_on=datetime.datetime.now(datetime.UTC),
    )


def make_client_geography(
    geography_id: str = "US", business_category: Optional[str] = None
) -> ClientGeography:
    return ClientGeography(
        id=1,
        client_id=10,
        geography_id=geography_id,
        business_category=business_category,
    )


def test_get_geographies_success(client: TestClient) -> None:
    pairs = [
        (make_client_geography("US", "Technology"), make_geography("US", "United States")),
        (make_client_geography("GB"), make_geography("GB", "United Kingdom")),
    ]

    class FakeRepo:
        def get_client_id(self, org_id: str) -> int:
            return 10

        def get_active_geographies(self, client_id: int) -> List[Tuple[ClientGeography, Geography]]:
            return pairs

    app.dependency_overrides[get_geography_service] = lambda: GeographyService(FakeRepo())

    resp = client.get("/api/v2/geographies")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 2

    us = data[0]
    assert us["geography_id"] == "US"
    assert us["name"] == "United States"
    assert us["is_active"] is True
    assert us["business_category"] == "Technology"

    gb = data[1]
    assert gb["geography_id"] == "GB"
    assert gb["business_category"] is None


def test_get_geographies_client_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_client_id(self, org_id: str) -> None:
            return None

        def get_active_geographies(self, client_id: int) -> List[Tuple[ClientGeography, Geography]]:
            return []

    app.dependency_overrides[get_geography_service] = lambda: GeographyService(FakeRepo())

    resp = client.get("/api/v2/geographies")
    assert resp.status_code == 404
    assert resp.json().get("detail") == "Client not found"


def test_get_geographies_empty(client: TestClient) -> None:
    class FakeRepo:
        def get_client_id(self, org_id: str) -> int:
            return 10

        def get_active_geographies(self, client_id: int) -> List[Tuple[ClientGeography, Geography]]:
            return []

    app.dependency_overrides[get_geography_service] = lambda: GeographyService(FakeRepo())

    resp = client.get("/api/v2/geographies")
    assert resp.status_code == 200
    assert resp.json() == []
