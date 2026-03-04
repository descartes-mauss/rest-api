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


def make_opportunity(oid: int = 10, sow_sid: int = 1) -> Opportunity:
    return Opportunity(
        oid=oid,
        sid=sow_sid,
        opportunity_name="Platform A",
        opportunity=1001,
        masterfile_version=1,
        for_deletion=False,
    )


def make_t2o(toid: int = 1, tid: int = 5, oid: int = 10) -> Topic2Opportunity:
    return Topic2Opportunity(toid=toid, tid=tid, oid=oid)


def make_topic(tid: int = 5, sow_sid: int = 1, trend_ssid: int = 20) -> Topic:
    return Topic(
        tid=tid,
        sid=sow_sid,
        load_date=NOW,
        topic_id=f"topic-{tid}",
        topic_name=f"Topic {tid}",
        topic_status=0,
        ssid=trend_ssid,
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def make_trend(ssid: int = 20, sow_sid: int = 1) -> Trend:
    return Trend(
        ssid=ssid,
        sid=sow_sid,
        load_date=NOW,
        trend_id=f"trend-{ssid}",
        trend_name=f"Trend {ssid}",
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
) -> MaturityScore:
    return MaturityScore(
        id=score_id,
        topic_id=topic_id,
        trend_id=trend_id,
        category=category,
        score=score,
        threshold="High",
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
# Full FakeRepo base (all methods required by SowService)
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
# Tests
# ---------------------------------------------------------------------------


def test_get_sow_opportunities_success(client: TestClient) -> None:
    sow = make_sow()
    opp = make_opportunity()
    t2o = make_t2o()
    topic = make_topic()
    trend = make_trend()
    t_score = make_maturity_score(score_id=1, topic_id=5, category="global", score=0.6)
    t_source = make_score_source(source_id=1, score_id=1)
    t_delta = make_score_delta(delta_id=1, topic_id="topic-5", category="global")
    t2d = make_t2d(tid=5, did=99)
    tr_score = make_maturity_score(score_id=2, trend_id=20, category="global", score=0.8)

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_opportunities_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Opportunity]:
            return [opp]

        def get_topic2opportunity_rows(
            self, tenant_schema: str, opp_oids: List[int]
        ) -> List[Topic2Opportunity]:
            return [t2o]

        def get_topics_by_ids(self, tenant_schema: str, topic_tids: List[int]) -> List[Topic]:
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

    resp = client.get("/api/v2/sows/1/opportunities")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1

    o = data[0]
    assert o["oid"] == 10
    assert o["sow"] == 1
    assert o["opportunity_name"] == "Platform A"
    assert o["opportunity"] == 1001
    assert o["topic_ids"] == ["topic-5"]

    assert len(o["topics"]) == 1
    t = o["topics"][0]
    assert t["tid"] == 5
    assert t["topic_id"] == "topic-5"
    assert t["type"] == "Topic"
    assert t["driver"] == [99]
    assert t["global_maturity_score"]["score"] == pytest.approx(0.6)
    assert len(t["global_maturity_score"]["sources"]) == 1
    assert t["global_maturity_score_delta"]["label"] == "Improving"

    assert t["trend"] is not None
    assert t["trend"]["trend_id"] == "trend-20"
    assert t["trend"]["global_maturity_score"]["score"] == pytest.approx(0.8)


def test_get_sow_opportunities_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass  # get_sow_by_id returns None

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/999/opportunities")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "SOW not available"


def test_get_sow_opportunities_empty(client: TestClient) -> None:
    sow = make_sow()

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/opportunities")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_sow_opportunities_no_topics(client: TestClient) -> None:
    """Opportunity exists but has no topics linked."""
    sow = make_sow()
    opp = make_opportunity()

    class FakeRepo(BaseFakeRepo):
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_opportunities_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Opportunity]:
            return [opp]

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/opportunities")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["topics"] == []
    assert data[0]["topic_ids"] == []
