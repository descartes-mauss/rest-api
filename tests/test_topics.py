import datetime
from typing import Generator

import pytest
from fastapi.testclient import TestClient

from database.tenant_models.models import Topic
from jwt_validator import validate_jwt
from main import app
from routes.topic_router import get_topic_service
from services.topic_services import TopicService


@pytest.fixture
def client() -> Generator[TestClient, None, None]:

    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    client = TestClient(app)

    yield client

    app.dependency_overrides = {}


def make_topic(topic_id: str = "t1") -> Topic:
    return Topic(
        tid=1,
        sid=10,
        load_date=datetime.datetime.utcnow(),
        topic_id=topic_id,
        topic_name="Test Topic",
        topic_status=1,
        topic_description="desc",
        topic_image_s3_uri=None,
        ssid=None,
        industry_sizing=None,
        society_sizing=None,
        consumer_sizing=None,
        average_sizing=None,
        average_sizing_label="",
        timeline=None,
        timeline_display=None,
        timeline_label="",
        topic_growth=None,
        topic_growth_normalized=None,
        topic_consensus=None,
        topic_consensus_normalized=None,
        topic_consensus_label="",
        topic_consensus_icon_uri=None,
        industry_impact=None,
        industry_impact_display=None,
        industry_impact_label="",
        industry_impact_icon_uri=None,
        action_required="",
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def test_list_topics(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    topics = [make_topic("topic-1"), make_topic("topic-2")]

    class FakeRepo:
        def get_all(self, tenant_schema):  # type: ignore[no-untyped-def]
            return topics

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics")
    print(resp.json())
    assert resp.status_code == 200
    data = resp.json()
    assert "topics" in data
    assert isinstance(data["topics"], list)
    assert len(data["topics"]) == 2
    assert data["topics"][0]["topic_id"] == "topic-1"


def test_get_topic_found(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    t = make_topic("topic-42")

    class FakeRepo:
        def get_by_topic_id(self, tenant_schema, topic_id):  # type: ignore[no-untyped-def]
            return t

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-42")
    assert resp.status_code == 200
    data = resp.json()
    assert "topic" in data
    assert data["topic"]["topic_id"] == "topic-42"


def test_get_topic_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_by_topic_id(self, tenant_schema: str, topic_id: str):  # type: ignore[no-untyped-def]
            return None

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/does-not-exist")
    assert resp.status_code == 404
    assert resp.json().get("error") == "Topic not found"
