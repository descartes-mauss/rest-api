from datetime import datetime, timezone
from typing import Generator, List, Optional, Tuple
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from database.tenant_models.enums import ConversationStatus, TenantUserStatus
from database.tenant_models.models import Conversation, TenantUser
from jwt_validator import validate_jwt
from main import app
from routes.conversation_router import get_conversation_service
from services.conversation_service import ConversationService

CONVERSATION_ID = uuid4()
USER_ID = uuid4()
NOW = datetime.now(timezone.utc)


def make_tenant_user(user_id: Optional[UUID] = None) -> TenantUser:
    return TenantUser(
        id=user_id or USER_ID,
        email="user@example.com",
        display_name="Test User",
        status=TenantUserStatus.ACTIVE,
        is_active=True,
        created_at=NOW,
        updated_at=NOW,
        locale="en",
        timezone="UTC",
        job_title="Engineer",
        extra_metadata=None,
    )


def make_conversation(
    conversation_id: Optional[UUID] = None,
    title: str = "Test Conversation",
    user_id: Optional[UUID] = None,
) -> Conversation:
    return Conversation(
        id=conversation_id or CONVERSATION_ID,
        title=title,
        status=ConversationStatus.ACTIVE,
        created_at=NOW,
        updated_at=NOW,
        user_id=user_id or USER_ID,
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


def test_list_conversations(client: TestClient) -> None:
    user = make_tenant_user()
    convs = [
        (make_conversation(uuid4(), "Conv 1"), user),
        (make_conversation(uuid4(), "Conv 2"), user),
    ]

    class FakeRepo:
        def get_all(
            self, tenant_schema: str
        ) -> List[Tuple[Conversation, TenantUser]]:
            return convs

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get("/api/v2/conversations/")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["title"] == "Conv 1"
    assert data[0]["user"]["display_name"] == "Test User"


def test_list_conversations_empty(client: TestClient) -> None:
    class FakeRepo:
        def get_all(
            self, tenant_schema: str
        ) -> List[Tuple[Conversation, TenantUser]]:
            return []

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get("/api/v2/conversations/")
    assert resp.status_code == 200
    assert resp.json() == []


# ---------------------------------------------------------------------------
# Get by ID
# ---------------------------------------------------------------------------


def test_get_conversation_found(client: TestClient) -> None:
    user = make_tenant_user()
    conv = make_conversation()

    class FakeRepo:
        def get_by_id(
            self, tenant_schema: str, conversation_id: UUID
        ) -> Optional[Tuple[Conversation, TenantUser]]:
            return (conv, user)

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get(f"/api/v2/conversations/{CONVERSATION_ID}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == str(CONVERSATION_ID)
    assert data["title"] == "Test Conversation"
    assert data["user"]["id"] == str(USER_ID)


def test_get_conversation_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_by_id(
            self, tenant_schema: str, conversation_id: UUID
        ) -> Optional[Tuple[Conversation, TenantUser]]:
            return None

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get(f"/api/v2/conversations/{uuid4()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Conversation not found"


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


def test_create_conversation(client: TestClient) -> None:
    user = make_tenant_user()
    conv = make_conversation()

    class FakeRepo:
        def create(
            self, tenant_schema: str, conversation: Conversation
        ) -> Conversation:
            return conv

        def get_by_id(
            self, tenant_schema: str, conversation_id: UUID
        ) -> Optional[Tuple[Conversation, TenantUser]]:
            return (conv, user)

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.post(
        "/api/v2/conversations/",
        json={"title": "Test Conversation", "user_id": str(USER_ID)},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["title"] == "Test Conversation"
    assert data["user"]["id"] == str(USER_ID)


# ---------------------------------------------------------------------------
# Update (PUT)
# ---------------------------------------------------------------------------


def test_update_conversation(client: TestClient) -> None:
    user = make_tenant_user()
    original = make_conversation()
    updated = make_conversation()
    updated.title = "Updated Title"

    class FakeRepo:
        def get_raw_by_id(
            self, tenant_schema: str, conversation_id: UUID
        ) -> Optional[Conversation]:
            return original

        def update(
            self, tenant_schema: str, conversation: Conversation
        ) -> Conversation:
            return updated

        def get_by_id(
            self, tenant_schema: str, conversation_id: UUID
        ) -> Optional[Tuple[Conversation, TenantUser]]:
            return (updated, user)

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.put(
        f"/api/v2/conversations/{CONVERSATION_ID}",
        json={"title": "Updated Title", "status": "Active"},
    )
    assert resp.status_code == 200
    assert resp.json()["title"] == "Updated Title"
    assert resp.json()["user"]["display_name"] == "Test User"


def test_update_conversation_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_raw_by_id(
            self, tenant_schema: str, conversation_id: UUID
        ) -> Optional[Conversation]:
            return None

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.put(
        f"/api/v2/conversations/{uuid4()}",
        json={"title": "Nope", "status": "Active"},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


def test_delete_conversation(client: TestClient) -> None:
    class FakeRepo:
        def delete(self, tenant_schema: str, conversation_id: UUID) -> bool:
            return True

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.delete(f"/api/v2/conversations/{CONVERSATION_ID}")
    assert resp.status_code == 204


def test_delete_conversation_not_found(client: TestClient) -> None:
    class FakeRepo:
        def delete(self, tenant_schema: str, conversation_id: UUID) -> bool:
            return False

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.delete(f"/api/v2/conversations/{uuid4()}")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Auth missing orgId
# ---------------------------------------------------------------------------


def test_missing_org_id(client: TestClient) -> None:
    app.dependency_overrides[validate_jwt] = lambda: {}

    class FakeRepo:
        def get_all(
            self, tenant_schema: str
        ) -> List[Tuple[Conversation, TenantUser]]:
            return []

    app.dependency_overrides[get_conversation_service] = lambda: ConversationService(
        FakeRepo()  # type: ignore[arg-type]
    )

    resp = client.get("/api/v2/conversations/")
    assert resp.status_code == 400
    assert "tenant schema" in resp.json()["detail"].lower()