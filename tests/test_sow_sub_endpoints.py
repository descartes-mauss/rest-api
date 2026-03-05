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
    TenantSow,
    Topic,
    Trend,
)
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


NOW = datetime.datetime.now(datetime.UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_sow(sid: int = 1, cs_sow_id: Optional[str] = "cs-001") -> TenantSow:
    return TenantSow(
        sid=sid,
        load_date=NOW,
        sow_name="Project Alpha",
        sow_status="live",
        sow_description="desc",
        cs_sow_id=cs_sow_id,
        masterfile_version=1,
    )


def make_trend(ssid: int = 10, sow_sid: int = 1, shift_id: str = "s1") -> Trend:
    return Trend(
        ssid=ssid,
        sid=sow_sid,
        load_date=NOW,
        trend_id=f"trend-{ssid}",
        trend_name=f"Trend {ssid}",
        trend_description="A trend",
        shift_id=shift_id,
        shift_name="Shift One",
        shift_description="First shift",
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def make_maturity_score(
    score_id: int = 1,
    trend_ssid: int = 10,
    category: str = "global",
    score: Optional[float] = 0.75,
) -> MaturityScore:
    return MaturityScore(
        id=score_id,
        trend_id=trend_ssid,
        category=category,
        score=score,
        threshold="High",
        rationale="Good progress",
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
    trend_id_str: str = "trend-10",
    category: str = "global",
) -> MaturityScoreDelta:
    return MaturityScoreDelta(
        id=delta_id,
        sow_id=sow_sid,
        trend_id=trend_id_str,
        topic_id=None,
        category=category,
        absolute_delta=0.05,
        percentage_delta=5.0,
        label="Improving",
        masterfile_version=1,
        for_deletion=False,
        created_at=NOW,
    )


def make_topic(tid: int = 1, trend_ssid: int = 10) -> Topic:
    return Topic(
        tid=tid,
        sid=1,
        load_date=NOW,
        topic_id=f"topic-{tid}",
        topic_name=f"Topic {tid}",
        topic_status=0,
        ssid=trend_ssid,
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def make_driver(did: int = 1, sow_sid: int = 1) -> Driver:
    return Driver(
        did=did,
        sow_sid=sow_sid,
        load_date=NOW,
        driver_id=f"driver-{did}",
        driver_name=f"Driver {did}",
        driver_description="Drives things",
        masterfile_version=1,
        for_deletion=False,
    )


def make_public_sow(cs_sow_id: str = "cs-001", geography_id: Optional[str] = "US") -> PublicSow:
    return PublicSow(
        id=10,
        client_id=1,
        name="Project Alpha",
        sow_id=cs_sow_id,
        sow_status="live",
        geography_id=geography_id,
    )


def make_geography() -> Geography:
    return Geography(geography_id="US", name="United States")


# ---------------------------------------------------------------------------
# Shifts tests
# ---------------------------------------------------------------------------


def test_get_sow_shifts_success(client: TestClient) -> None:
    sow = make_sow()
    trend = make_trend()
    score = make_maturity_score(score_id=1, trend_ssid=10, category="global", score=0.8)
    source = make_score_source(source_id=1, score_id=1)
    delta = make_score_delta(delta_id=1, sow_sid=1, trend_id_str="trend-10", category="global")
    topic = make_topic(tid=1, trend_ssid=10)

    class FakeRepo:
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return [sow]

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return sow

        def get_trends_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Trend]:
            return [trend]

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
            return [topic]

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []

        def get_drivers_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Driver]:
            return []

        def get_topic_counts_for_drivers(
            self, tenant_schema: str, driver_dids: List[int]
        ) -> Dict[int, int]:
            return {}

        def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]:
            return []

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/shifts")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1

    shift = data[0]
    assert shift["id"] == "s1"
    assert shift["name"] == "Shift One"
    assert len(shift["trends"]) == 1

    t = shift["trends"][0]
    assert t["trend_id"] == "trend-10"
    assert t["type"] == "Trend"
    assert t["sow"] == 1
    assert t["global_maturity_score"] is not None
    assert t["global_maturity_score"]["score"] == pytest.approx(0.8)
    assert len(t["global_maturity_score"]["sources"]) == 1
    assert t["global_maturity_score_delta"] is not None
    assert len(t["related_topics"]) == 1
    assert t["related_topics"][0]["topic_id"] == "topic-1"


def test_get_sow_shifts_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return None

        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return []

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return None

        def get_trends_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Trend]:
            return []

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

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []

        def get_drivers_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Driver]:
            return []

        def get_topic_counts_for_drivers(
            self, tenant_schema: str, driver_dids: List[int]
        ) -> Dict[int, int]:
            return {}

        def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]:
            return []

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/999/shifts")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "SOW not available"


def test_get_sow_shifts_empty_trends(client: TestClient) -> None:
    sow = make_sow()

    class FakeRepo:
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return [sow]

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return sow

        def get_trends_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Trend]:
            return []

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

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []

        def get_drivers_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Driver]:
            return []

        def get_topic_counts_for_drivers(
            self, tenant_schema: str, driver_dids: List[int]
        ) -> Dict[int, int]:
            return {}

        def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]:
            return []

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/shifts")
    assert resp.status_code == 200
    assert resp.json() == []


# ---------------------------------------------------------------------------
# Drivers tests
# ---------------------------------------------------------------------------


def _full_fake_repo_for_drivers(
    sow: TenantSow,
    drivers: List[Driver],
    topic_counts: Dict[int, int],
    geo_rows: Optional[List[Tuple[PublicSow, Optional[Geography]]]] = None,
) -> object:
    class FakeRepo:
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return [sow]

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return sow

        def get_trends_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Trend]:
            return []

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

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return geo_rows or []

        def get_drivers_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Driver]:
            result = list(drivers)
            if name_order:
                result.sort(key=lambda d: d.driver_name.casefold(), reverse=(name_order == "desc"))
            return result

        def get_topic_counts_for_drivers(
            self, tenant_schema: str, driver_dids: List[int]
        ) -> Dict[int, int]:
            return dict(topic_counts)

        def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]:
            return []

    return FakeRepo()


def test_get_sow_drivers_success(client: TestClient) -> None:
    sow = make_sow()
    driver = make_driver(did=1)
    pub_sow = make_public_sow()
    geo = make_geography()

    app.dependency_overrides[get_sow_service] = lambda: SowService(
        _full_fake_repo_for_drivers(sow, [driver], {1: 3}, [(pub_sow, geo)])  # type: ignore[arg-type]
    )

    resp = client.get("/api/v2/sows/1/drivers")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["did"] == 1
    assert data[0]["driver_name"] == "Driver 1"
    assert data[0]["topic_count"] == 3
    assert data[0]["sow"]["id"] == 1
    assert data[0]["sow"]["geography_id"] == "US"


def test_get_sow_drivers_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return None

        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return []

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return None

        def get_trends_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Trend]:
            return []

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

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []

        def get_drivers_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Driver]:
            return []

        def get_topic_counts_for_drivers(
            self, tenant_schema: str, driver_dids: List[int]
        ) -> Dict[int, int]:
            return {}

        def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]:
            return []

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/999/drivers")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "SOW not available"


def test_get_sow_drivers_sorted_by_name(client: TestClient) -> None:
    sow = make_sow()
    driver_b = make_driver(did=2)
    driver_b.driver_name = "Beta Driver"
    driver_a = make_driver(did=1)
    driver_a.driver_name = "Alpha Driver"

    app.dependency_overrides[get_sow_service] = lambda: SowService(
        _full_fake_repo_for_drivers(sow, [driver_b, driver_a], {1: 0, 2: 0})  # type: ignore[arg-type]
    )

    resp = client.get("/api/v2/sows/1/drivers?sort=name&order=asc")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["driver_name"] == "Alpha Driver"
    assert data[1]["driver_name"] == "Beta Driver"


# ---------------------------------------------------------------------------
# Versions tests
# ---------------------------------------------------------------------------


def test_get_sow_versions_success(client: TestClient) -> None:
    sow = make_sow(sid=1)
    older_sow = make_sow(sid=2)
    older_sow.load_date = NOW - datetime.timedelta(days=30)

    class FakeRepo:
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return sow

        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return [sow]

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return sow

        def get_trends_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Trend]:
            return []

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

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []

        def get_drivers_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Driver]:
            return []

        def get_topic_counts_for_drivers(
            self, tenant_schema: str, driver_dids: List[int]
        ) -> Dict[int, int]:
            return {}

        def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]:
            return [sow, older_sow]  # newest first (by -sid, returned from DB)

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/1/versions")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["id"] == 1
    assert data[1]["id"] == 2
    assert data[0]["name"] == "Project Alpha"
    assert data[0]["geography_id"] == "ALL"


def test_get_sow_versions_not_found(client: TestClient) -> None:
    class FakeRepo:
        def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
            return None

        def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
            return []

        def get_latest_live_sow_for_cs_sow_id(
            self, tenant_schema: str, cs_sow_id: str
        ) -> Optional[TenantSow]:
            return None

        def get_trends_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Trend]:
            return []

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

        def get_sow_geographies(
            self, cs_sow_ids: List[str]
        ) -> List[Tuple[PublicSow, Optional[Geography]]]:
            return []

        def get_drivers_for_sow(
            self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
        ) -> List[Driver]:
            return []

        def get_topic_counts_for_drivers(
            self, tenant_schema: str, driver_dids: List[int]
        ) -> Dict[int, int]:
            return {}

        def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]:
            return []

    app.dependency_overrides[get_sow_service] = lambda: SowService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/sows/999/versions")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "SOW not available"
