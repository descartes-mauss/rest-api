import datetime
from typing import Generator, List, Optional
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from clients.s3_rest_client import S3RestClient
from database.tenant_models.models import TenantSow, Topic
from jwt_validator import validate_jwt
from main import app
from routes.topic_router import get_deepdive_service
from services.deepdive_service import DeepdiveService

NOW = datetime.datetime.now(datetime.UTC)


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides = {}


def make_topic(topic_id: str = "topic-1", tid: int = 1, sid: int = 10) -> Topic:
    return Topic(
        tid=tid,
        sid=sid,
        load_date=NOW,
        topic_id=topic_id,
        topic_name="Test Topic",
        topic_status=0,
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def make_sow(sid: int = 10, cs_sow_id: str = "sow-42") -> TenantSow:
    return TenantSow(
        sid=sid,
        load_date=NOW,
        sow_name="Test Sow",
        sow_status="live",
        cs_sow_id=cs_sow_id,
        masterfile_version=1,
        for_deletion=False,
    )


class BaseFakeRepo:
    def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
        return None

    def get_sow_by_sid(self, tenant_schema: str, sid: int) -> Optional[TenantSow]:
        return None


def make_fake_s3_client(
    provocations: Optional[List] = None,
    evolution: Optional[List] = None,
    manifestations: Optional[List] = None,
    market_insights: Optional[List] = None,
) -> S3RestClient:
    mock = MagicMock(spec=S3RestClient)
    mock.get_topic_provocations.return_value = provocations or []
    mock.get_topic_evolution.return_value = evolution or []
    mock.get_topic_manifestations.return_value = manifestations or []
    mock.get_topic_market_insights.return_value = market_insights or []
    return mock


# ---------------------------------------------------------------------------
# GET /api/v2/topics/{topic_id}/deepdive
# ---------------------------------------------------------------------------


def test_deepdive_topic_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass  # get_by_topic_id returns None

    s3 = make_fake_s3_client()
    app.dependency_overrides[get_deepdive_service] = lambda: DeepdiveService(FakeRepo(), s3)  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/nonexistent/deepdive")
    assert resp.status_code == 200
    data = resp.json()
    assert data == {"provocations": [], "evolution": [], "manifestations": [], "datapoints": []}


def test_deepdive_returns_all_sections(client: TestClient) -> None:
    topic = make_topic("topic-1")
    sow = make_sow(sid=10, cs_sow_id="sow-42")

    class FakeRepo(BaseFakeRepo):
        def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
            return topic

        def get_sow_by_sid(self, tenant_schema: str, sid: int) -> Optional[TenantSow]:
            return sow

    s3 = make_fake_s3_client(
        provocations=[{"provocation_one": "Q1", "provocation_two": "Q2"}],
        evolution=[
            {
                "year_one": "2020",
                "year_one_description": "desc 1",
                "year_two": "2021",
                "year_two_description": "desc 2",
                "year_three": "2022",
                "year_three_description": "desc 3",
                "year_four": "2023",
                "year_four_description": "desc 4",
                "year_five": "2024",
                "year_five_description": "desc 5",
            }
        ],
        manifestations=[{"startup_names": "A"}, {"startup_names": "B"}],
        market_insights=[{"data_point": "X"}, {"data_point": "Y"}, {"data_point": "Z"}],
    )
    app.dependency_overrides[get_deepdive_service] = lambda: DeepdiveService(FakeRepo(), s3)  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-1/deepdive")
    assert resp.status_code == 200
    data = resp.json()

    assert data["provocations"] == ["Q1", "Q2"]
    assert len(data["evolution"]) == 5
    assert data["evolution"][0] == ["2020", "desc 1"]
    assert len(data["manifestations"]) == 2
    assert len(data["datapoints"]) == 2  # capped at 2


def test_deepdive_manifestations_capped_at_4(client: TestClient) -> None:
    topic = make_topic()
    sow = make_sow()

    class FakeRepo(BaseFakeRepo):
        def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
            return topic

        def get_sow_by_sid(self, tenant_schema: str, sid: int) -> Optional[TenantSow]:
            return sow

    many_manifestations = [{"startup_names": f"S{i}"} for i in range(10)]
    s3 = make_fake_s3_client(manifestations=many_manifestations)
    app.dependency_overrides[get_deepdive_service] = lambda: DeepdiveService(FakeRepo(), s3)  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-1/deepdive")
    assert resp.status_code == 200
    assert len(resp.json()["manifestations"]) == 4


def test_deepdive_empty_s3_responses(client: TestClient) -> None:
    topic = make_topic()
    sow = make_sow()

    class FakeRepo(BaseFakeRepo):
        def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
            return topic

        def get_sow_by_sid(self, tenant_schema: str, sid: int) -> Optional[TenantSow]:
            return sow

    s3 = make_fake_s3_client()
    app.dependency_overrides[get_deepdive_service] = lambda: DeepdiveService(FakeRepo(), s3)  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-1/deepdive")
    assert resp.status_code == 200
    data = resp.json()
    assert data["provocations"] == []
    assert data["evolution"] == []
    assert data["manifestations"] == []
    assert data["datapoints"] == []


def test_deepdive_no_sow(client: TestClient) -> None:
    topic = make_topic()

    class FakeRepo(BaseFakeRepo):
        def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
            return topic

        # get_sow_by_sid returns None

    s3 = make_fake_s3_client(
        provocations=[{"provocation_one": "Q1"}],
    )
    app.dependency_overrides[get_deepdive_service] = lambda: DeepdiveService(FakeRepo(), s3)  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-1/deepdive")
    assert resp.status_code == 200
    # S3 is still called with empty cs_sow_id; result depends on S3 mock
    assert "provocations" in resp.json()


def test_deepdive_provocations_filters_nulls(client: TestClient) -> None:
    topic = make_topic()
    sow = make_sow()

    class FakeRepo(BaseFakeRepo):
        def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
            return topic

        def get_sow_by_sid(self, tenant_schema: str, sid: int) -> Optional[TenantSow]:
            return sow

    # Only provocation_one is set; provocation_two is absent
    s3 = make_fake_s3_client(
        provocations=[{"provocation_one": "Only one", "other_key": "ignored"}],
    )
    app.dependency_overrides[get_deepdive_service] = lambda: DeepdiveService(FakeRepo(), s3)  # type: ignore[arg-type]

    resp = client.get("/api/v2/topics/topic-1/deepdive")
    assert resp.status_code == 200
    assert resp.json()["provocations"] == ["Only one"]
