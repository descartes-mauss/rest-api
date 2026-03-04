import datetime
from typing import Dict, Generator, List, Optional, Tuple

import pytest
from fastapi.testclient import TestClient

from database.public_models.models import Geography, PublicSow
from database.tenant_models.models import (
    Driver,
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


def make_sow(sid: int = 1) -> TenantSow:
    return TenantSow(
        sid=sid,
        load_date=NOW,
        sow_name="Project Alpha",
        sow_status="live",
        sow_description="desc",
        cs_sow_id="cs-001",
        masterfile_version=1,
    )


def make_trend(ssid: int = 10, sow_sid: int = 1, new_discovery: bool = False) -> Trend:
    return Trend(
        ssid=ssid,
        sid=sow_sid,
        load_date=NOW,
        trend_id=f"trend-{ssid}",
        trend_name=f"Trend {ssid}",
        trend_description="A trend",
        shift_id="shift-1",
        shift_name="Shift One",
        shift_description="First shift",
        masterfile_version=1,
        for_deletion=False,
        new_discovery=new_discovery,
    )


def make_topic(
    tid: int = 5,
    sow_sid: int = 1,
    trend_ssid: int = 10,
    name: Optional[str] = None,
    new_discovery: bool = False,
) -> Topic:
    return Topic(
        tid=tid,
        sid=sow_sid,
        load_date=NOW,
        topic_id=f"topic-{tid}",
        topic_name=name or f"Topic {tid}",
        topic_status=0,
        ssid=trend_ssid,
        masterfile_version=1,
        for_deletion=False,
        new_discovery=new_discovery,
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
    topic_id: Optional[str] = "topic-5",
    trend_id: Optional[str] = None,
    category: str = "global",
) -> MaturityScoreDelta:
    return MaturityScoreDelta(
        id=delta_id,
        sow_id=sow_sid,
        topic_id=topic_id,
        trend_id=trend_id,
        category=category,  # type: ignore[arg-type]
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

    def get_trends_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Trend]:
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

    def get_topics_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Topic]:
        return []

    def get_drivers_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Driver]:
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


# ---------------------------------------------------------------------------
# Topics tests
# ---------------------------------------------------------------------------


def test_get_sow_topics_success(client: TestClient) -> None:
    sow = make_sow()
    topic = make_topic(tid=5, trend_ssid=10)
    trend = make_trend(ssid=10)
    t_score = make_maturity_score(score_id=1, topic_id=5, category="global", score=0.65)
    t_source = make_score_source(source_id=1, score_id=1)
    t_delta = make_score_delta(delta_id=1, topic_id="topic-5", category="global")
    t2d = make_t2d(tid=5, did=42)
    tr_score = make_maturity_score(score_id=2, trend_id=10, category="global", score=0.8)

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_topics_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Topic]:
            return [topic]

        def get_maturity_scores_for_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[MaturityScore]:
            return [t_score]

        def get_maturity_score_sources(
            self, tenant_schema: str, score_ids: List[int]
        ) -> List[MaturityScoreSource]:
            return [t_source]

        def get_maturity_score_deltas_for_sow_topic_ids(
            self, tenant_schema: str, sow_sid: int, topic_id_strings: List[str]
        ) -> List[MaturityScoreDelta]:
            return [t_delta]

        def get_topic_drivers_by_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[Topic2Driver]:
            return [t2d]

        def get_trends_by_ssids(self, tenant_schema: str, trend_ssids: List[int]) -> List[Trend]:
            return [trend]

        def get_maturity_scores_for_trend_ids(
            self, tenant_schema: str, trend_ssids: List[int]
        ) -> List[MaturityScore]:
            return [tr_score]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/topics")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1

    t = data[0]
    assert t["tid"] == 5
    assert t["topic_id"] == "topic-5"
    assert t["type"] == "Topic"
    assert t["driver"] == [42]
    assert t["global_maturity_score"]["score"] == pytest.approx(0.65)
    assert len(t["global_maturity_score"]["sources"]) == 1
    assert t["global_maturity_score_delta"]["label"] == "Improving"

    assert t["trend"] is not None
    assert t["trend"]["trend_id"] == "trend-10"
    assert t["trend"]["global_maturity_score"]["score"] == pytest.approx(0.8)
    # related_topics in the embedded trend should be the topics already loaded
    assert isinstance(t["trend"]["related_topics"], list)


def test_get_sow_topics_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass  # get_sow_by_id returns None

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/999/topics")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "SOW not available"


def test_get_sow_topics_empty(client: TestClient) -> None:
    sow = make_sow()

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/topics")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_sow_topics_filter_new(client: TestClient) -> None:
    """maturity_level=New returns only new_discovery topics."""
    sow = make_sow()
    topic_new = make_topic(tid=1, new_discovery=True, name="New Topic")
    topic_old = make_topic(tid=2, new_discovery=False, name="Old Topic")

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_topics_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Topic]:
            return [topic_new, topic_old]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/topics?maturity_level=New")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["topic_id"] == "topic-1"


def test_get_sow_topics_filter_by_threshold(client: TestClient) -> None:
    """maturity_level=Emerging returns only topics whose global score threshold matches."""
    sow = make_sow()
    topic_emerging = make_topic(tid=1)
    topic_mature = make_topic(tid=2)
    score_emerging = make_maturity_score(
        score_id=1, topic_id=1, category="global", score=0.3, threshold="Emerging"
    )
    score_mature = make_maturity_score(
        score_id=2, topic_id=2, category="global", score=0.9, threshold="Mature"
    )

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_topics_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Topic]:
            return [topic_emerging, topic_mature]

        def get_maturity_scores_for_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[MaturityScore]:
            return [score_emerging, score_mature]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/topics?maturity_level=Emerging")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["tid"] == 1


def test_get_sow_topics_sort_name_asc(client: TestClient) -> None:
    """sort=name&order=asc returns topics ordered by topic_name ascending."""
    sow = make_sow()
    topics = [
        make_topic(tid=1, name="Zebra"),
        make_topic(tid=2, name="Alpha"),
        make_topic(tid=3, name="Mango"),
    ]

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_topics_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Topic]:
            return topics

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/topics?sort=name&order=asc")
    assert resp.status_code == 200
    names = [t["topic_name"] for t in resp.json()]
    assert names == ["Alpha", "Mango", "Zebra"]


# ---------------------------------------------------------------------------
# Trends tests
# ---------------------------------------------------------------------------


def test_get_sow_trends_success(client: TestClient) -> None:
    sow = make_sow()
    trend = make_trend(ssid=10)
    related_topic = make_topic(tid=5, trend_ssid=10)
    t2d = make_t2d(tid=5, did=99)
    tr_score = make_maturity_score(score_id=1, trend_id=10, category="global", score=0.75)
    tr_source = make_score_source(source_id=1, score_id=1)
    tr_delta = make_score_delta(delta_id=1, trend_id="trend-10", topic_id=None, category="global")

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_trends_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Trend]:
            return [trend]

        def get_maturity_scores_for_trend_ids(
            self, tenant_schema: str, trend_ssids: List[int]
        ) -> List[MaturityScore]:
            return [tr_score]

        def get_maturity_score_sources(
            self, tenant_schema: str, score_ids: List[int]
        ) -> List[MaturityScoreSource]:
            return [tr_source]

        def get_maturity_score_deltas_for_sow_trends(
            self, tenant_schema: str, sow_sid: int, trend_id_strings: List[str]
        ) -> List[MaturityScoreDelta]:
            return [tr_delta]

        def get_topics_for_trends(self, tenant_schema: str, trend_ssids: List[int]) -> List[Topic]:
            return [related_topic]

        def get_topic_drivers_by_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[Topic2Driver]:
            return [t2d]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/trends")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1

    tr = data[0]
    assert tr["ssid"] == 10
    assert tr["trend_id"] == "trend-10"
    assert tr["type"] == "Trend"
    assert tr["driver_count"] == 1
    assert tr["global_maturity_score"]["score"] == pytest.approx(0.75)
    assert len(tr["global_maturity_score"]["sources"]) == 1
    assert tr["global_maturity_score_delta"]["label"] == "Improving"
    assert len(tr["related_topics"]) == 1
    assert tr["related_topics"][0]["topic_id"] == "topic-5"


def test_get_sow_trends_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass  # get_sow_by_id returns None

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/999/trends")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "SOW not available"


def test_get_sow_trends_empty(client: TestClient) -> None:
    sow = make_sow()

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/trends")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_sow_trends_filter_new(client: TestClient) -> None:
    """maturity_level=New returns only new_discovery trends."""
    sow = make_sow()
    trend_new = make_trend(ssid=1, new_discovery=True)
    trend_old = make_trend(ssid=2, new_discovery=False)

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_trends_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Trend]:
            return [trend_new, trend_old]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/trends?maturity_level=New")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["trend_id"] == "trend-1"


def test_get_sow_trends_driver_count(client: TestClient) -> None:
    """driver_count reflects distinct drivers across all topics of each trend."""
    sow = make_sow()
    trend = make_trend(ssid=10)
    # Two topics for the trend, sharing one driver and each having a unique one → 3 distinct
    t1 = make_topic(tid=1, trend_ssid=10)
    t2 = make_topic(tid=2, trend_ssid=10)
    t2d_shared_1 = make_t2d(tdid=1, tid=1, did=100)
    t2d_shared_2 = make_t2d(tdid=2, tid=2, did=100)  # same driver, different topic
    t2d_unique = make_t2d(tdid=3, tid=2, did=200)

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_trends_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Trend]:
            return [trend]

        def get_topics_for_trends(self, tenant_schema: str, trend_ssids: List[int]) -> List[Topic]:
            return [t1, t2]

        def get_topic_drivers_by_topic_ids(
            self, tenant_schema: str, topic_tids: List[int]
        ) -> List[Topic2Driver]:
            return [t2d_shared_1, t2d_shared_2, t2d_unique]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/trends")
    assert resp.status_code == 200
    assert resp.json()[0]["driver_count"] == 2  # 100 and 200 (100 appears twice but counted once)


def test_get_sow_trends_sort_maturity_desc(client: TestClient) -> None:
    """sort=maturity&order=desc returns trends ordered by global score descending."""
    sow = make_sow()
    trend_low = make_trend(ssid=1)
    trend_high = make_trend(ssid=2)
    score_low = make_maturity_score(score_id=1, trend_id=1, category="global", score=0.3)
    score_high = make_maturity_score(score_id=2, trend_id=2, category="global", score=0.9)

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_trends_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Trend]:
            return [trend_low, trend_high]

        def get_maturity_scores_for_trend_ids(
            self, tenant_schema: str, trend_ssids: List[int]
        ) -> List[MaturityScore]:
            return [score_low, score_high]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/trends?sort=maturity&order=desc")
    assert resp.status_code == 200
    scores = [t["global_maturity_score"]["score"] for t in resp.json()]
    assert scores == pytest.approx([0.9, 0.3])
