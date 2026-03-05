import datetime
from typing import Generator, List, Optional, Tuple

import pytest
from fastapi.testclient import TestClient

from database.tenant_models.models import Driver, Source, Topic, Topic2Driver, Topic2Source
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
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_topic(tid: int = 1, topic_id: str = "topic-1") -> Topic:
    return Topic(
        tid=tid,
        sid=10,
        load_date=NOW,
        topic_id=topic_id,
        topic_name="Test Topic",
        topic_status=0,
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def make_source(soid: int = 1) -> Source:
    return Source(
        soid=soid,
        sow_sid=10,
        source_url="https://example.com/article",
        source_title="Article Title",
        internal_classification="primary",
        load_date=datetime.datetime(2024, 6, 15, tzinfo=datetime.UTC),
        masterfile_version=1,
        for_deletion=False,
    )


def make_topic2source(tsid: int = 1, tid: int = 1, soid: int = 1) -> Topic2Source:
    return Topic2Source(tsid=tsid, tid=tid, soid=soid)


def make_driver(did: int = 1) -> Driver:
    return Driver(
        did=did,
        sow_sid=10,
        load_date=NOW,
        driver_id=f"driver-{did}",
        driver_name=f"Driver {did}",
        driver_description="A driver description",
        masterfile_version=1,
        for_deletion=False,
    )


def make_topic2driver(
    tdid: int = 1,
    tid: int = 1,
    did: int = 1,
    strength: Optional[float] = 0.8,
    polarity: Optional[float] = 0.5,
) -> Topic2Driver:
    return Topic2Driver(tdid=tdid, tid=tid, did=did, strength=strength, polarity=polarity)


# ---------------------------------------------------------------------------
# BaseFakeRepo / BaseFakeSowRepo — all repo methods return empty defaults
# ---------------------------------------------------------------------------


class BaseFakeSowRepo:
    pass


class BaseFakeRepo:
    def get_all(self, tenant_schema: str) -> List[Topic]:
        return []

    def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
        return None

    def get_by_id(self, tenant_schema: str, tid: int) -> Optional[Topic]:
        return None

    def get_topics_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Topic]:
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


# ---------------------------------------------------------------------------
# GET /api/v2/topics/{topic_id}/sources
# ---------------------------------------------------------------------------


def test_get_topic_sources_found(client: TestClient) -> None:
    topic = make_topic()
    source = make_source()
    t2s = make_topic2source()

    class FakeRepo(BaseFakeRepo):
        def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
            return topic

        def get_sources_for_topic(
            self, tenant_schema: str, tid: int
        ) -> List[Tuple[Topic2Source, Source]]:
            return [(t2s, source)]

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-1/sources")
    assert resp.status_code == 200
    data = resp.json()
    assert data["last_updated"] == "2024-06-15"
    assert len(data["topic_sources"]) == 1
    src = data["topic_sources"][0]
    assert src["id"] == 1
    assert src["url"] == "https://example.com/article"
    assert src["title"] == "Article Title"
    assert src["internal_classification"] == "primary"


def test_get_topic_sources_topic_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass  # get_by_topic_id returns None by default

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/nonexistent/sources")
    assert resp.status_code == 200
    data = resp.json()
    assert data["last_updated"] is None
    assert data["topic_sources"] == []


def test_get_topic_sources_no_sources(client: TestClient) -> None:
    topic = make_topic()

    class FakeRepo(BaseFakeRepo):
        def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
            return topic

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-1/sources")
    assert resp.status_code == 200
    data = resp.json()
    assert data["last_updated"] is None
    assert data["topic_sources"] == []


# ---------------------------------------------------------------------------
# POST /api/v2/topics/{tid}/status
# ---------------------------------------------------------------------------


def test_update_topic_status_success(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        def update_status(self, tenant_schema: str, tid: int, status_id: int) -> bool:
            return True

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.post("/api/v2/topics/1/status", json={"status_id": 2})
    assert resp.status_code == 200
    assert resp.json() == {"success": True}


def test_update_topic_status_invalid_status(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.post("/api/v2/topics/1/status", json={"status_id": 99})
    assert resp.status_code == 404
    assert "error" in resp.json()


def test_update_topic_status_topic_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        def update_status(self, tenant_schema: str, tid: int, status_id: int) -> bool:
            return False

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.post("/api/v2/topics/999/status", json={"status_id": 1})
    assert resp.status_code == 404


def test_update_topic_status_all_valid_codes(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        def update_status(self, tenant_schema: str, tid: int, status_id: int) -> bool:
            return True

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    for status_id in range(5):  # 0-4 are valid
        resp = client.post("/api/v2/topics/1/status", json={"status_id": status_id})
        assert resp.status_code == 200, f"Expected 200 for status_id={status_id}"


# ---------------------------------------------------------------------------
# GET /api/v2/topics/{tid}/drivers
# ---------------------------------------------------------------------------


def test_get_topic_drivers_success(client: TestClient) -> None:
    driver = make_driver(did=1)
    t2d = make_topic2driver(tdid=1, tid=1, did=1, strength=0.8, polarity=0.5)

    class FakeRepo(BaseFakeRepo):
        def get_topic2drivers_with_driver(
            self, tenant_schema: str, tid: int
        ) -> List[Tuple[Topic2Driver, Driver]]:
            return [(t2d, driver)]

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/1/drivers")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    item = data[0]
    assert item["driver"]["driver_name"] == "Driver 1"
    assert item["driver"]["driver_description"] == "A driver description"
    assert item["driver_influence"] == pytest.approx(0.4)


def test_get_topic_drivers_no_drivers(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/1/drivers")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_topic_drivers_null_influence(client: TestClient) -> None:
    driver = make_driver(did=1)
    t2d = make_topic2driver(tdid=1, tid=1, did=1, strength=None, polarity=None)

    class FakeRepo(BaseFakeRepo):
        def get_topic2drivers_with_driver(
            self, tenant_schema: str, tid: int
        ) -> List[Tuple[Topic2Driver, Driver]]:
            return [(t2d, driver)]

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/1/drivers")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["driver_influence"] is None


def test_get_topic_drivers_multiple(client: TestClient) -> None:
    driver_a = make_driver(did=1)
    driver_a.driver_name = "Alpha Driver"
    driver_b = make_driver(did=2)
    driver_b.driver_name = "Beta Driver"
    t2d_a = make_topic2driver(tdid=1, tid=5, did=1, strength=1.0, polarity=1.0)
    t2d_b = make_topic2driver(tdid=2, tid=5, did=2, strength=0.5, polarity=0.5)

    class FakeRepo(BaseFakeRepo):
        def get_topic2drivers_with_driver(
            self, tenant_schema: str, tid: int
        ) -> List[Tuple[Topic2Driver, Driver]]:
            return [(t2d_a, driver_a), (t2d_b, driver_b)]

    app.dependency_overrides[get_topic_service] = lambda: TopicService(FakeRepo(), BaseFakeSowRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/5/drivers")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["driver"]["driver_name"] == "Alpha Driver"
    assert data[0]["driver_influence"] == pytest.approx(1.0)
    assert data[1]["driver"]["driver_name"] == "Beta Driver"
    assert data[1]["driver_influence"] == pytest.approx(0.25)
