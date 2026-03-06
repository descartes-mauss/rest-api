import datetime
from typing import Generator, List, Optional, Tuple

import pytest
from fastapi.testclient import TestClient

from database.public_models.models import Geography, PublicSow
from database.tenant_models.models import TenantSow
from jwt_validator import validate_jwt
from main import app
from routes.sow_router import get_sow_service
from services.sow_service import SowService


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides = {}


def make_tenant_sow(
    sid: int = 1,
    sow_name: str = "Project Alpha",
    sow_status: str = "live",
    cs_sow_id: Optional[str] = "cs-001",
) -> TenantSow:
    return TenantSow(
        sid=sid,
        load_date=datetime.datetime.now(datetime.UTC),
        sow_name=sow_name,
        sow_status=sow_status,
        sow_description="A test SOW",
        cs_sow_id=cs_sow_id,
        masterfile_version=1,
    )


def make_public_sow(
    cs_sow_id: str = "cs-001",
    geography_id: Optional[str] = "US",
    sow_status: str = "live",
) -> PublicSow:
    return PublicSow(
        id=10,
        client_id=1,
        name="Project Alpha",
        sow_id=cs_sow_id,
        sow_status=sow_status,
        geography_id=geography_id,
    )


def make_geography(geography_id: str = "US", name: str = "United States") -> Geography:
    return Geography(geography_id=geography_id, name=name)


def test_get_sows_success(client: TestClient) -> None:
    sow = make_tenant_sow()
    pub_sow = make_public_sow()
    geo = make_geography()

    class FakeRepo:
        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return [sow]

        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return sow

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return [(pub_sow, geo)]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())

    resp = client.get("/api/v2/sows")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["id"] == 1
    assert data[0]["name"] == "Project Alpha"
    assert data[0]["geography_id"] == "US"
    assert data[0]["geography_name"] == "United States"
    assert "load_date" in data[0]


def test_get_sows_empty(client: TestClient) -> None:
    class FakeRepo:
        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return []

        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return None

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return None

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())

    resp = client.get("/api/v2/sows")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_sows_default_geography(client: TestClient) -> None:
    """SOW without a matching geography entry falls back to 'ALL' / 'Worldwide'."""
    sow = make_tenant_sow()

    class FakeRepo:
        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return [sow]

        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return sow

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []  # no geography data available

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())

    resp = client.get("/api/v2/sows")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["geography_id"] == "ALL"
    assert data[0]["geography_name"] == "Worldwide"


def test_get_sow_success(client: TestClient) -> None:
    sow = make_tenant_sow()
    pub_sow = make_public_sow()
    geo = make_geography()

    class FakeRepo:
        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return [sow]

        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return sow

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return [(pub_sow, geo)]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())

    resp = client.get("/api/v2/sows/1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == 1
    assert data["name"] == "Project Alpha"
    assert data["geography_id"] == "US"
    assert data["geography_name"] == "United States"


def test_get_sow_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return []

        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return None

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return None

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())

    resp = client.get("/api/v2/sows/999")
    assert resp.status_code == 404
    assert resp.json().get("detail") == "SOW not available"


def test_get_sow_inactive(client: TestClient) -> None:
    """A SOW superseded by a newer live version should return 404."""
    old_sow = make_tenant_sow(sid=5)
    newer_sow = make_tenant_sow(sid=6)  # newer live version for same cs_sow_id

    class FakeRepo:
        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return [newer_sow]

        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return old_sow

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return newer_sow  # latest is not the requested one

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())

    resp = client.get("/api/v2/sows/5")
    assert resp.status_code == 404
    assert resp.json().get("detail") == "SOW not available"
