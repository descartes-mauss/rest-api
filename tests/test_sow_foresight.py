import datetime
from typing import Dict, Generator, List, Optional, Tuple

import pytest
from fastapi.testclient import TestClient

from database.public_models.models import Geography, PublicSow
from database.tenant_models.models import (
    Driver,
    Insight,
    InsightSource,
    MaturityScore,
    MaturityScoreDelta,
    MaturityScoreSource,
    Opportunity,
    TenantSow,
    Topic,
    Topic2Driver,
    Topic2Opportunity,
    Trend,
)
from jwt_validator import validate_jwt
from main import app
from routes.sow_router import get_sow_service
from services.sow_service import SowService

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


def make_sow(sid: int = 1, cs_sow_id: str = "cs-001") -> TenantSow:
    return TenantSow(
        sid=sid,
        load_date=NOW,
        sow_name="Project Alpha",
        sow_status="live",
        sow_description="desc",
        cs_sow_id=cs_sow_id,
        masterfile_version=1,
    )


def make_insight(
    insight_id: int = 1,
    entity_id: str = "topic-5",
    entity_type: str = "topic",
    cs_sow_id: str = "cs-001",
) -> Insight:
    return Insight(
        id=insight_id,
        entity_id=entity_id,
        entity_type=entity_type,
        insight_title=f"Insight {insight_id}",
        insight_description=f"Description {insight_id}",
        created_at=NOW,
        cs_sow_id=cs_sow_id,
    )


def make_insight_source(source_id: int = 1) -> InsightSource:
    return InsightSource(
        id=source_id,
        source_url="https://example.com",
        source_title="Example Source",
        source_favicon="https://example.com/favicon.ico",
    )


def make_topic(
    tid: int = 5,
    sow_sid: int = 1,
    trend_ssid: int = 10,
    topic_id: Optional[str] = None,
) -> Topic:
    return Topic(
        tid=tid,
        sid=sow_sid,
        load_date=NOW,
        topic_id=topic_id or f"topic-{tid}",
        topic_name=f"Topic {tid}",
        topic_status=0,
        ssid=trend_ssid,
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def make_trend(
    ssid: int = 10,
    sow_sid: int = 1,
    trend_id: Optional[str] = None,
) -> Trend:
    return Trend(
        ssid=ssid,
        sid=sow_sid,
        load_date=NOW,
        trend_id=trend_id or f"trend-{ssid}",
        trend_name=f"Trend {ssid}",
        trend_description="A trend",
        shift_id="shift-1",
        shift_name="Shift One",
        shift_description="First shift",
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def make_maturity_score(
    score_id: int,
    topic_id: Optional[int] = None,
    trend_id: Optional[int] = None,
    category: str = "global",
    score: float = 0.7,
    threshold: str = "High",
) -> MaturityScore:
    return MaturityScore(
        id=score_id,
        topic_id=topic_id,
        trend_id=trend_id,
        category=category,  # type: ignore[arg-type]
        score=score,
        threshold=threshold,
        rationale="Good",
    )


def make_t2d(tdid: int = 1, tid: int = 5, did: int = 99) -> Topic2Driver:
    return Topic2Driver(tdid=tdid, tid=tid, did=did)


# ---------------------------------------------------------------------------
# BaseFakeRepo — all methods return empty/None by default
# ---------------------------------------------------------------------------


class BaseFakeRepo:
    def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
        return None

    def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
        return []

    def get_latest_live_sow_for_cs_sow_id(
        self, tenant_schema: str, cs_sow_id: str
    ) -> Optional[TenantSow]:
        return None

    def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]:
        return []

    def get_trends_for_sow(
        self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
    ) -> List[Trend]:
        return []

    def get_maturity_scores_for_trend_ids(
        self, tenant_schema: str, trend_ssids: List[int]
    ) -> List[MaturityScore]:
        return []

    def get_maturity_score_sources(
        self, tenant_schema: str, score_ids: List[int]
    ) -> List[MaturityScoreSource]:
        return []

    def get_maturity_score_deltas_for_sow_trends(
        self, tenant_schema: str, sow_sid: int, trend_id_strings: List[str]
    ) -> List[MaturityScoreDelta]:
        return []

    def get_topics_for_trends(self, tenant_schema: str, trend_ssids: List[int]) -> List[Topic]:
        return []

    def get_topics_for_sow(
        self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
    ) -> List[Topic]:
        return []

    def get_drivers_for_sow(
        self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
    ) -> List[Driver]:
        return []

    def get_topic_counts_for_drivers(
        self, tenant_schema: str, driver_dids: List[int]
    ) -> Dict[int, int]:
        return {}

    def get_sow_geographies(
        self, cs_sow_ids: List[str]
    ) -> List[Tuple[PublicSow, Optional[Geography]]]:
        return []

    def get_opportunities_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Opportunity]:
        return []

    def get_topic2opportunity_rows(
        self, tenant_schema: str, opp_oids: List[int]
    ) -> List[Topic2Opportunity]:
        return []

    def get_topics_by_ids(self, tenant_schema: str, topic_tids: List[int]) -> List[Topic]:
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

    def get_trends_by_ssids(self, tenant_schema: str, trend_ssids: List[int]) -> List[Trend]:
        return []

    # Foresight methods
    def get_insights_for_cs_sow_id(
        self, tenant_schema: str, cs_sow_id: str, offset: int, limit: int
    ) -> Tuple[int, List[Insight]]:
        return 0, []

    def get_insights_filtered(
        self,
        tenant_schema: str,
        cs_sow_id: str,
        entity_ids: List[str],
        offset: int,
        limit: int,
    ) -> Tuple[int, List[Insight]]:
        return 0, []

    def get_insight_sources_for_insight_ids(
        self, tenant_schema: str, insight_ids: List[int]
    ) -> List[Tuple[InsightSource, int]]:
        return []

    def get_topics_by_topic_str_ids(
        self, tenant_schema: str, sow_sid: int, topic_ids: List[str]
    ) -> List[Topic]:
        return []

    def get_trends_by_trend_str_ids(
        self, tenant_schema: str, sow_sid: int, trend_ids: List[str]
    ) -> List[Trend]:
        return []


# ---------------------------------------------------------------------------
# GET /api/v2/sows/{sow_id}/foresight tests
# ---------------------------------------------------------------------------


def test_get_foresight_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass  # get_sow_by_id returns None

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/999/foresight")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "SOW not available"


def test_get_foresight_empty(client: TestClient) -> None:
    sow = make_sow()

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/foresight")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["weeklyInsights"] == []
    assert data["hasNext"] is False
    assert data["hasPrev"] is False


def test_get_foresight_topic_insight_success(client: TestClient) -> None:
    """GET foresight returns an insight with a topic prediction and source."""
    sow = make_sow()
    insight = make_insight(insight_id=1, entity_id="topic-5", entity_type="topic")
    source = make_insight_source(source_id=1)
    topic = make_topic(tid=5, topic_id="topic-5")
    topic_score = make_maturity_score(score_id=1, topic_id=5, category="global", score=0.6)
    t2d = make_t2d(tid=5, did=42)
    trend = make_trend(ssid=10)

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_insights_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str, offset: int, limit: int
        ) -> Tuple[int, List[Insight]]:
            return 1, [insight]

        def get_insight_sources_for_insight_ids(
            self, tenant_schema: str, insight_ids: List[int]
        ) -> List[Tuple[InsightSource, int]]:
            return [(source, 1)]

        def get_topics_by_topic_str_ids(
            self, tenant_schema: str, sow_sid: int, topic_ids: List[str]
        ) -> List[Topic]:
            return [topic]

        def get_maturity_scores_for_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[MaturityScore]:
            return [topic_score]

        def get_topic_drivers_by_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[Topic2Driver]:
            return [t2d]

        def get_trends_by_ssids(self, tenant_schema: str, trend_ssids: List[int]) -> List[Trend]:
            return [trend]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/foresight")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["hasNext"] is False
    assert data["hasPrev"] is False
    assert data["limit"] == 8

    weekly = data["weeklyInsights"]
    assert len(weekly) == 1
    item = weekly[0]
    assert item["insight_title"] == "Insight 1"
    assert item["insight_description"] == "Description 1"
    assert len(item["sources"]) == 1
    assert item["sources"][0]["source_url"] == "https://example.com"
    assert item["sources"][0]["source_favicon"] == "https://example.com/favicon.ico"

    assert len(item["predictions"]) == 1
    pred = item["predictions"][0]
    assert pred["topic_id"] == "topic-5"
    assert pred["type"] == "Topic"
    assert pred["driver"] == [42]
    assert pred["global_maturity_score"]["score"] == pytest.approx(0.6)


def test_get_foresight_trend_insight_success(client: TestClient) -> None:
    """GET foresight returns an insight with a trend prediction."""
    sow = make_sow()
    insight = make_insight(insight_id=2, entity_id="trend-10", entity_type="trend")
    trend = make_trend(ssid=10, trend_id="trend-10")
    trend_score = make_maturity_score(score_id=1, trend_id=10, category="global", score=0.75)
    related_topic = make_topic(tid=5, trend_ssid=10)
    t2d = make_t2d(tid=5, did=99)

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_insights_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str, offset: int, limit: int
        ) -> Tuple[int, List[Insight]]:
            return 1, [insight]

        def get_trends_by_trend_str_ids(
            self, tenant_schema: str, sow_sid: int, trend_ids: List[str]
        ) -> List[Trend]:
            return [trend]

        def get_maturity_scores_for_trend_ids(
            self, tenant_schema: str, trend_ssids: List[int]
        ) -> List[MaturityScore]:
            return [trend_score]

        def get_topics_for_trends(self, tenant_schema: str, trend_ssids: List[int]) -> List[Topic]:
            return [related_topic]

        def get_topic_drivers_by_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[Topic2Driver]:
            return [t2d]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/foresight")
    assert resp.status_code == 200
    data = resp.json()
    weekly = data["weeklyInsights"]
    assert len(weekly) == 1
    pred = weekly[0]["predictions"][0]
    assert pred["trend_id"] == "trend-10"
    assert pred["type"] == "Trend"
    assert pred["driver_count"] == 1
    assert len(pred["related_topics"]) == 1
    assert pred["global_maturity_score"]["score"] == pytest.approx(0.75)


def test_get_foresight_pagination_has_next(client: TestClient) -> None:
    """hasNext is True when there are more results beyond the current page."""
    sow = make_sow()
    insights = [make_insight(insight_id=i) for i in range(1, 9)]  # 8 items

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_insights_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str, offset: int, limit: int
        ) -> Tuple[int, List[Insight]]:
            return 10, insights  # total=10, but page holds 8

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/foresight?page=1&limit=8")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 10
    assert data["hasNext"] is True
    assert data["hasPrev"] is False


def test_get_foresight_pagination_has_prev(client: TestClient) -> None:
    """hasPrev is True when page > 1."""
    sow = make_sow()
    insights = [make_insight(insight_id=i) for i in range(9, 11)]  # 2 items on page 2

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_insights_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str, offset: int, limit: int
        ) -> Tuple[int, List[Insight]]:
            return 10, insights

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/foresight?page=2&limit=8")
    assert resp.status_code == 200
    data = resp.json()
    assert data["hasPrev"] is True
    assert data["hasNext"] is False


# ---------------------------------------------------------------------------
# POST /api/v2/sows/{sow_id}/foresight/search tests
# ---------------------------------------------------------------------------


def test_search_foresight_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.post("/api/v2/sows/999/foresight/search", json={})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "SOW not available"


def test_search_foresight_no_filter_returns_all(client: TestClient) -> None:
    """POST with empty topic/trend IDs returns all insights (same as GET)."""
    sow = make_sow()
    insight = make_insight(insight_id=1)

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_insights_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str, offset: int, limit: int
        ) -> Tuple[int, List[Insight]]:
            return 1, [insight]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.post("/api/v2/sows/1/foresight/search", json={})
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert len(data["weeklyInsights"]) == 1


def test_search_foresight_filter_by_topic_ids(client: TestClient) -> None:
    """POST with topic_ids calls get_insights_filtered with those entity IDs."""
    sow = make_sow()
    insight = make_insight(insight_id=1, entity_id="topic-5", entity_type="topic")

    received_entity_ids: List[str] = []

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_insights_filtered(
            self,
            tenant_schema: str,
            cs_sow_id: str,
            entity_ids: List[str],
            offset: int,
            limit: int,
        ) -> Tuple[int, List[Insight]]:
            received_entity_ids.extend(entity_ids)
            return 1, [insight]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.post(
        "/api/v2/sows/1/foresight/search", json={"topic_ids": ["topic-5", "topic-6"]}
    )
    assert resp.status_code == 200
    assert set(received_entity_ids) == {"topic-5", "topic-6"}
    assert len(resp.json()["weeklyInsights"]) == 1


def test_search_foresight_filter_by_both_ids(client: TestClient) -> None:
    """POST with both topic_ids and trend_ids passes all entity IDs to the repository."""
    sow = make_sow()
    insight = make_insight(insight_id=1, entity_id="trend-10", entity_type="trend")

    received_entity_ids: List[str] = []

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_insights_filtered(
            self,
            tenant_schema: str,
            cs_sow_id: str,
            entity_ids: List[str],
            offset: int,
            limit: int,
        ) -> Tuple[int, List[Insight]]:
            received_entity_ids.extend(entity_ids)
            return 1, [insight]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.post(
        "/api/v2/sows/1/foresight/search",
        json={"topic_ids": ["topic-5"], "trend_ids": ["trend-10"]},
    )
    assert resp.status_code == 200
    assert set(received_entity_ids) == {"topic-5", "trend-10"}
