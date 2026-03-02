import datetime
from typing import Generator, List, Optional

import pytest
from fastapi.testclient import TestClient

from database.public_models.enums import ExperimentType
from database.public_models.models import Experiment
from database.tenant_models.models import TenantSow
from jwt_validator import validate_jwt
from main import app
from routes.permissions_router import get_permissions_service
from services.permissions_service import PermissionsService


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides = {}


def make_sow() -> TenantSow:
    return TenantSow(
        sid=42,
        load_date=datetime.datetime.now(datetime.UTC),
        sow_name="Test SOW",
        sow_status="live",
        sow_description="Test description",
        cs_sow_id="cs-sow-123",
        masterfile_version=1,
        for_deletion=False,
    )


def make_experiment() -> Experiment:
    return Experiment(
        experiment_id=1,
        sow_id=5,
        experiment_name="Test Experiment",
        experiment_url="https://example.com/exp",
        experiment_type=ExperimentType.EXPERIMENT,
        created_at=datetime.datetime.now(datetime.UTC),
        updated_at=datetime.datetime.now(datetime.UTC),
    )


def test_get_permissions_success(client: TestClient) -> None:
    class FakeRepo:
        def get_sow(self, tenant_schema: str, sow_id: int) -> TenantSow:  # type: ignore[return]
            return make_sow()

        def get_client_tier_id(self, org_id: str) -> int:
            return 1

        def get_feature_codes(self, tier_id: int) -> List[str]:
            return ["displays_growth_opportunities", "api_access"]

        def get_experiments(self, cs_sow_id: Optional[str]) -> List[Experiment]:
            return [make_experiment()]

        def has_opportunity_platforms(self, tenant_schema: str, sow_sid: int) -> bool:
            return True

    app.dependency_overrides[get_permissions_service] = lambda: PermissionsService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/permissions/42")
    assert resp.status_code == 200
    data = resp.json()
    assert "experiments" in data
    assert "permissions" in data
    assert "opportunity_platforms" in data
    assert data["opportunity_platforms"] is True
    assert data["permissions"] == {"displays_growth_opportunities": True, "api_access": True}
    assert len(data["experiments"]) == 1
    assert data["experiments"][0]["experiment_name"] == "Test Experiment"
    assert data["experiments"][0]["sow_id"] == 5


def test_get_permissions_sow_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_sow(self, tenant_schema: str, sow_id: int) -> None:
            return None

        def get_client_tier_id(self, org_id: str) -> int:
            return 1

        def get_feature_codes(self, tier_id: int) -> List[str]:
            return []

        def get_experiments(self, cs_sow_id: Optional[str]) -> List[Experiment]:
            return []

        def has_opportunity_platforms(self, tenant_schema: str, sow_sid: int) -> bool:
            return False

    app.dependency_overrides[get_permissions_service] = lambda: PermissionsService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/permissions/999")
    assert resp.status_code == 404
    assert resp.json().get("detail") == "SowModel not available"


def test_get_permissions_no_features(client: TestClient) -> None:
    class FakeRepo:
        def get_sow(self, tenant_schema: str, sow_id: int) -> TenantSow:  # type: ignore[return]
            return make_sow()

        def get_client_tier_id(self, org_id: str) -> None:
            return None

        def get_feature_codes(self, tier_id: int) -> List[str]:
            return []

        def get_experiments(self, cs_sow_id: Optional[str]) -> List[Experiment]:
            return []

        def has_opportunity_platforms(self, tenant_schema: str, sow_sid: int) -> bool:
            return False

    app.dependency_overrides[get_permissions_service] = lambda: PermissionsService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/permissions/42")
    assert resp.status_code == 200
    data = resp.json()
    assert data["permissions"] == {}
    assert data["opportunity_platforms"] is False
    assert data["experiments"] == []
