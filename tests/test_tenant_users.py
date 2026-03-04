from datetime import datetime, timezone
from typing import Generator, List, Optional
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from database.tenant_models.enums import TenantUserStatus
from database.tenant_models.models import TenantUser
from jwt_validator import validate_jwt
from main import app
from routes.tenant_user_router import get_tenant_user_service
from services.tenant_user_service import TenantUserService

USER_ID = uuid4()


def make_tenant_user(
    user_id: Optional[UUID] = None,
    display_name: str = "Test User",
    email: str = "test@example.com",
) -> TenantUser:
    return TenantUser(
        id=user_id or USER_ID,
        email=email,
        display_name=display_name,
        status=TenantUserStatus.ACTIVE,
        is_active=True,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        locale="en",
        timezone="UTC",
        job_title="Engineer",
        extra_metadata=None,
    )


@pytest.fixture()
def client() -> Generator[TestClient, None, None]:
    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides = {}


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


def test_list_tenant_users(client: TestClient) -> None:
    users = [make_tenant_user(uuid4(), "Alice"), make_tenant_user(uuid4(), "Bob")]

    class FakeRepo:
        def get_all(self, tenant_schema: str) -> List[TenantUser]:
            return users

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get("/api/v2/users/")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["display_name"] == "Alice"
    assert data[1]["display_name"] == "Bob"


def test_list_tenant_users_empty(client: TestClient) -> None:
    class FakeRepo:
        def get_all(self, tenant_schema: str) -> List[TenantUser]:
            return []

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get("/api/v2/users/")
    assert resp.status_code == 200
    assert resp.json() == []


# ---------------------------------------------------------------------------
# Get by ID
# ---------------------------------------------------------------------------


def test_get_tenant_user_found(client: TestClient) -> None:
    user = make_tenant_user()

    class FakeRepo:
        def get_by_id(self, tenant_schema: str, user_id: UUID) -> Optional[TenantUser]:
            return user

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get(f"/api/v2/users/{USER_ID}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == str(USER_ID)
    assert data["display_name"] == "Test User"


def test_get_tenant_user_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_by_id(self, tenant_schema: str, user_id: UUID) -> Optional[TenantUser]:
            return None

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get(f"/api/v2/users/{uuid4()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Tenant user not found"


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


def test_create_tenant_user(client: TestClient) -> None:
    created_user = make_tenant_user()

    class FakeRepo:
        def create(self, tenant_schema: str, user: TenantUser) -> TenantUser:
            return created_user

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.post(
        "/api/v2/users/",
        json={"display_name": "Test User", "email": "test@example.com"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["display_name"] == "Test User"
    assert data["email"] == "test@example.com"


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------


def test_update_tenant_user(client: TestClient) -> None:
    original = make_tenant_user()
    updated = make_tenant_user()
    updated.display_name = "Updated Name"

    class FakeRepo:
        def get_by_id(self, tenant_schema: str, user_id: UUID) -> Optional[TenantUser]:
            return original

        def update(self, tenant_schema: str, user: TenantUser) -> TenantUser:
            return updated

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.put(
        f"/api/v2/users/{USER_ID}",
        json={
            "display_name": "Updated Name",
            "status": "Active",
            "is_active": True,
            "locale": "en",
            "timezone": "UTC",
            "job_title": "Engineer",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["display_name"] == "Updated Name"


def test_update_tenant_user_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_by_id(self, tenant_schema: str, user_id: UUID) -> Optional[TenantUser]:
            return None

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.put(
        f"/api/v2/users/{uuid4()}",
        json={
            "display_name": "Nope",
            "status": "Active",
            "is_active": True,
            "locale": "en",
            "timezone": "UTC",
            "job_title": "Engineer",
        },
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


def test_delete_tenant_user(client: TestClient) -> None:
    class FakeRepo:
        def delete(self, tenant_schema: str, user_id: UUID) -> bool:
            return True

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.delete(f"/api/v2/users/{USER_ID}")
    assert resp.status_code == 204


def test_delete_tenant_user_not_found(client: TestClient) -> None:
    class FakeRepo:
        def delete(self, tenant_schema: str, user_id: UUID) -> bool:
            return False

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.delete(f"/api/v2/users/{uuid4()}")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Auth missing orgId
# ---------------------------------------------------------------------------


def test_missing_org_id(client: TestClient) -> None:
    app.dependency_overrides[validate_jwt] = lambda: {}

    class FakeRepo:
        def get_all(self, tenant_schema: str) -> List[TenantUser]:
            return []

    app.dependency_overrides[get_tenant_user_service] = lambda: TenantUserService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get("/api/v2/users/")
    assert resp.status_code == 400
    assert "tenant schema" in resp.json()["detail"].lower()
