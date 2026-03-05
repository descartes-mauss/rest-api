"""Unit tests for GET /api/v2/trends/{trend_id} and GET /api/v2/trends/{trend_id}/topics."""

import datetime
from typing import Generator, List, Optional

import pytest
from fastapi.testclient import TestClient

from database.tenant_models.models import (
    MaturityScore,
    MaturityScoreDelta,
    MaturityScoreSource,
    Topic,
    Topic2Driver,
    Trend,
)
from jwt_validator import validate_jwt
from main import app
from routes.trend_router import get_trend_service
from services.trend_service import TrendService

NOW = datetime.datetime.now(datetime.UTC)


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    yield TestClient(app)
    app.dependency_overrides = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_trend(ssid: int = 10, sow_sid: int = 1, trend_id: str = "trend-abc") -> Trend:
    return Trend(
        ssid=ssid,
        sid=sow_sid,
        load_date=NOW,
        trend_id=trend_id,
        trend_name="Test Trend",
        trend_description="A trend description",
        shift_id="shift-1",
        shift_name="Shift One",
        shift_description="First shift",
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def make_topic(
    tid: int = 5,
    sow_sid: int = 1,
    trend_ssid: int = 10,
    topic_id: str = "topic-5",
) -> Topic:
    return Topic(
        tid=tid,
        sid=sow_sid,
        load_date=NOW,
        topic_id=topic_id,
        topic_name=f"Topic {tid}",
        topic_status=0,
        ssid=trend_ssid,
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def make_maturity_score(
    score_id: int,
    trend_id: Optional[int] = None,
    topic_id: Optional[int] = None,
    category: str = "global",
    score: float = 0.75,
    threshold: str = "High",
) -> MaturityScore:
    return MaturityScore(
        id=score_id,
        trend_id=trend_id,
        topic_id=topic_id,
        category=category,
        score=score,
        threshold=threshold,
        rationale="Good",
    )


def make_score_source(source_id: int = 1, score_id: int = 1) -> MaturityScoreSource:
    return MaturityScoreSource(
        id=source_id,
        maturity_score_id=score_id,
        source_url="https://example.com",
        source_title="Source A",
    )


def make_score_delta(
    delta_id: int = 1,
    sow_sid: int = 1,
    trend_id: Optional[str] = "trend-abc",
    topic_id: Optional[str] = None,
    category: str = "global",
) -> MaturityScoreDelta:
    return MaturityScoreDelta(
        id=delta_id,
        sow_id=sow_sid,
        trend_id=trend_id,
        topic_id=topic_id,
        category=category,
        absolute_delta=0.05,
        percentage_delta=5.0,
        label="Improving",
        masterfile_version=1,
        for_deletion=False,
        created_at=NOW,
    )


def make_t2d(tdid: int = 1, tid: int = 5, did: int = 99) -> Topic2Driver:
    return Topic2Driver(tdid=tdid, tid=tid, did=did)


# ---------------------------------------------------------------------------
# BaseFakeRepo — all methods return empty/None by default
# ---------------------------------------------------------------------------


class BaseFakeRepo:
    def get_trend_by_trend_id(self, tenant_schema: str, trend_id: str) -> Optional[Trend]:
        return None

    def get_maturity_scores_for_trend_ids(
        self, tenant_schema: str, trend_ssids: List[int]
    ) -> List[MaturityScore]:
        return []

    def get_maturity_score_sources_for_ids(
        self, tenant_schema: str, score_ids: List[int]
    ) -> List[MaturityScoreSource]:
        return []

    def get_maturity_score_deltas_for_sow_trends(
        self, tenant_schema: str, sow_sid: int, trend_id_strings: List[str]
    ) -> List[MaturityScoreDelta]:
        return []

    def get_topics_for_trends(self, tenant_schema: str, trend_ssids: List[int]) -> List[Topic]:
        return []

    def get_maturity_scores_for_topic_ids(
        self, tenant_schema: str, topic_tids: List[int]
    ) -> List[MaturityScore]:
        return []

    def get_maturity_score_deltas_for_sow_topic_ids(
        self, tenant_schema: str, sow_sid: int, topic_id_strings: List[str]
    ) -> List[MaturityScoreDelta]:
        return []

    def get_topic_drivers_by_topic_ids(
        self, tenant_schema: str, topic_tids: List[int]
    ) -> List[Topic2Driver]:
        return []


# ---------------------------------------------------------------------------
# GET /api/v2/trends/{trend_id}
# ---------------------------------------------------------------------------


def test_get_trend_success(client: TestClient) -> None:
    """Returns a TrendSchema with maturity score and related topic."""
    trend = make_trend()
    related_topic = make_topic(tid=5, trend_ssid=10)
    score = make_maturity_score(score_id=1, trend_id=10, category="global", score=0.8)
    source = make_score_source(source_id=1, score_id=1)
    delta = make_score_delta(delta_id=1, trend_id="trend-abc", category="global")

    class FakeRepo(BaseFakeRepo):
        def get_trend_by_trend_id(self, tenant_schema: str, trend_id: str) -> Optional[Trend]:
            return trend

        def get_maturity_scores_for_trend_ids(
            self, tenant_schema: str, trend_ssids: List[int]
        ) -> List[MaturityScore]:
            return [score]

        def get_maturity_score_sources_for_ids(
            self, tenant_schema: str, score_ids: List[int]
        ) -> List[MaturityScoreSource]:
            return [source]

        def get_maturity_score_deltas_for_sow_trends(
            self, tenant_schema: str, sow_sid: int, trend_id_strings: List[str]
        ) -> List[MaturityScoreDelta]:
            return [delta]

        def get_topics_for_trends(self, tenant_schema: str, trend_ssids: List[int]) -> List[Topic]:
            return [related_topic]

    app.dependency_overrides[get_trend_service] = lambda: TrendService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/trends/trend-abc")
    assert resp.status_code == 200
    data = resp.json()
    assert data["trend_id"] == "trend-abc"
    assert data["trend_name"] == "Test Trend"
    assert data["type"] == "Trend"
    assert data["global_maturity_score"]["score"] == pytest.approx(0.8)
    assert len(data["global_maturity_score"]["sources"]) == 1
    assert data["global_maturity_score_delta"]["label"] == "Improving"
    assert len(data["related_topics"]) == 1
    assert data["related_topics"][0]["topic_id"] == "topic-5"


def test_get_trend_not_found(client: TestClient) -> None:
    """Returns 404 when the trend_id does not exist."""

    class FakeRepo(BaseFakeRepo):
        pass  # get_trend_by_trend_id returns None

    app.dependency_overrides[get_trend_service] = lambda: TrendService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/trends/nonexistent")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Trend not available"


def test_get_trend_no_maturity(client: TestClient) -> None:
    """Returns a TrendSchema with null maturity fields when no scores exist."""
    trend = make_trend()

    class FakeRepo(BaseFakeRepo):
        def get_trend_by_trend_id(self, tenant_schema: str, trend_id: str) -> Optional[Trend]:
            return trend

    app.dependency_overrides[get_trend_service] = lambda: TrendService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/trends/trend-abc")
    assert resp.status_code == 200
    data = resp.json()
    assert data["global_maturity_score"] is None
    assert data["global_maturity_score_delta"] is None
    assert data["related_topics"] == []


# ---------------------------------------------------------------------------
# GET /api/v2/trends/{trend_id}/topics
# ---------------------------------------------------------------------------


def test_get_trend_topics_success(client: TestClient) -> None:
    """Returns a list of TopicSchema with the embedded parent trend."""
    trend = make_trend()
    topic = make_topic(tid=5, trend_ssid=10)
    topic_score = make_maturity_score(score_id=1, topic_id=5, category="global", score=0.65)
    topic_delta = make_score_delta(delta_id=1, topic_id="topic-5", trend_id=None, category="global")
    t2d = make_t2d(tid=5, did=42)
    trend_score = make_maturity_score(score_id=2, trend_id=10, category="global", score=0.8)

    class FakeRepo(BaseFakeRepo):
        def get_trend_by_trend_id(self, tenant_schema: str, trend_id: str) -> Optional[Trend]:
            return trend

        def get_topics_for_trends(self, tenant_schema: str, trend_ssids: List[int]) -> List[Topic]:
            return [topic]

        def get_maturity_scores_for_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[MaturityScore]:
            return [topic_score]

        def get_maturity_score_deltas_for_sow_topic_ids(
            self, tenant_schema: str, sow_sid: int, topic_id_strings: List[str]
        ) -> List[MaturityScoreDelta]:
            return [topic_delta]

        def get_topic_drivers_by_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[Topic2Driver]:
            return [t2d]

        def get_maturity_scores_for_trend_ids(
            self, tenant_schema: str, trend_ssids: List[int]
        ) -> List[MaturityScore]:
            return [trend_score]

    app.dependency_overrides[get_trend_service] = lambda: TrendService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/trends/trend-abc/topics")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1

    t = data[0]
    assert t["topic_id"] == "topic-5"
    assert t["type"] == "Topic"
    assert t["driver"] == [42]
    assert t["global_maturity_score"]["score"] == pytest.approx(0.65)
    assert t["global_maturity_score_delta"]["label"] == "Improving"
    assert t["trend"]["trend_id"] == "trend-abc"
    assert t["trend"]["global_maturity_score"]["score"] == pytest.approx(0.8)


def test_get_trend_topics_not_found(client: TestClient) -> None:
    """Returns 404 when the trend does not exist."""

    class FakeRepo(BaseFakeRepo):
        pass

    app.dependency_overrides[get_trend_service] = lambda: TrendService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/trends/nonexistent/topics")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Trend not available"


def test_get_trend_topics_empty(client: TestClient) -> None:
    """Returns an empty list when the trend exists but has no topics."""
    trend = make_trend()

    class FakeRepo(BaseFakeRepo):
        def get_trend_by_trend_id(self, tenant_schema: str, trend_id: str) -> Optional[Trend]:
            return trend

    app.dependency_overrides[get_trend_service] = lambda: TrendService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/trends/trend-abc/topics")
    assert resp.status_code == 200
    assert resp.json() == []
