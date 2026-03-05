import datetime
from typing import Generator, List, Optional, Tuple

import pytest
from fastapi.testclient import TestClient

from database.tenant_models.models import (
    Driver,
    MaturityScore,
    MaturityScoreDelta,
    MaturityScoreSource,
    Source,
    Topic,
    Topic2Driver,
    Topic2Source,
    Trend,
)
from jwt_validator import validate_jwt
from main import app
from routes.topic_router import get_topic_service
from services.topic_service import TopicService

NOW = datetime.datetime.now(datetime.UTC)


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
        load_date=NOW,
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


class BaseFakeRepo:
    """Default no-op implementations for all TopicRepository methods."""

    def get_all(self, tenant_schema: str) -> List[Topic]:
        return []

    def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
        return None

    def get_by_id(self, tenant_schema: str, tid: int) -> Optional[Topic]:
        return None

    def get_all_by_sow_id(self, tenant_schema: str, sow_id: int) -> List[Topic]:
        return []

    def get_sources_for_topic(
        self, tenant_schema: str, tid: int
    ) -> List[Tuple[Topic2Source, Source]]:
        return []

    def update_status(self, tenant_schema: str, tid: int, status_id: int) -> bool:
        return True

    def get_topic2drivers_with_driver(
        self, tenant_schema: str, tid: int
    ) -> List[Tuple[Topic2Driver, Driver]]:
        return []

    def get_maturity_scores_for_topic(self, tenant_schema: str, tid: int) -> List[MaturityScore]:
        return []

    def get_maturity_score_deltas_for_topic(
        self, tenant_schema: str, sow_sid: int, topic_id: str
    ) -> List[MaturityScoreDelta]:
        return []


class BaseFakeSowRepo:
    """Default no-op implementations for SowRepository methods used by TopicService."""

    def get_maturity_score_sources_for_ids(
        self, tenant_schema: str, score_ids: List[int]
    ) -> List[MaturityScoreSource]:
        return []

    def get_trend_by_ssid(self, tenant_schema: str, ssid: int) -> Optional[Trend]:
        return None

    def get_maturity_scores_for_trend(self, tenant_schema: str, ssid: int) -> List[MaturityScore]:
        return []

    def get_maturity_score_deltas_for_trend(
        self, tenant_schema: str, sow_sid: int, trend_id: str
    ) -> List[MaturityScoreDelta]:
        return []


# ---------------------------------------------------------------------------
# GET /api/v2/topics  (list)
# ---------------------------------------------------------------------------


def test_list_topics(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    topics = [make_topic("topic-1"), make_topic("topic-2")]

    class FakeRepo(BaseFakeRepo):
        def get_all(self, tenant_schema: str) -> List[Topic]:
            return topics

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics")
    assert resp.status_code == 200
    data = resp.json()
    assert "topics" in data
    assert isinstance(data["topics"], list)
    assert len(data["topics"]) == 2
    assert data["topics"][0]["topic_id"] == "topic-1"


# ---------------------------------------------------------------------------
# GET /api/v2/topics/{topic_id}  (single)
# ---------------------------------------------------------------------------


def test_get_topic_found(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    t = make_topic("topic-42")

    class FakeRepo(BaseFakeRepo):
        def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
            return t

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-42")
    assert resp.status_code == 200
    data = resp.json()
    # No wrapper — topic fields at the top level
    assert "topic_id" in data
    assert data["topic_id"] == "topic-42"
    assert "topic" not in data  # no extra nesting
    # driver field present and is a list
    assert "driver" in data
    assert isinstance(data["driver"], list)


def test_get_topic_found_with_drivers(client: TestClient) -> None:
    t = make_topic("topic-10")
    driver = Driver(
        did=5,
        sow_sid=10,
        load_date=NOW,
        driver_id="d-5",
        driver_name="Driver Five",
        masterfile_version=1,
        for_deletion=False,
    )
    t2d = Topic2Driver(tdid=1, tid=1, did=5, strength=1.0, polarity=1.0)

    class FakeRepo(BaseFakeRepo):
        def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
            return t

        def get_topic2drivers_with_driver(
            self, tenant_schema: str, tid: int
        ) -> List[Tuple[Topic2Driver, Driver]]:
            return [(t2d, driver)]

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-10")
    assert resp.status_code == 200
    data = resp.json()
    assert data["driver"] == [5]


def test_get_topic_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/does-not-exist")
    assert resp.status_code == 404
    assert resp.json().get("error") == "Topic not found"
