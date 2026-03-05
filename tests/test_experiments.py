import datetime
from typing import Generator, Optional

import pytest
from fastapi.testclient import TestClient

from database.public_models.enums import ExperimentType
from database.public_models.models import Experiment
from jwt_validator import validate_jwt
from main import app
from routes.experiment_router import get_experiment_service
from services.experiment_service import ExperimentService

NOW = datetime.datetime.now(datetime.UTC)


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    def override_validate_jwt() -> dict[str, str]:
        return {"orgId": "test_schema"}

    app.dependency_overrides[validate_jwt] = override_validate_jwt
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides = {}


def make_experiment(experiment_id: int = 1) -> Experiment:
    return Experiment(
        experiment_id=experiment_id,
        sow_id=10,
        experiment_name="Test Experiment",
        experiment_url="https://example.com/experiment",
        experiment_type=ExperimentType.EXPERIMENT,
        created_at=NOW,
        updated_at=NOW,
    )


class BaseFakeRepo:
    def get_by_id(self, experiment_id: int) -> Optional[Experiment]:
        return None


# ---------------------------------------------------------------------------
# GET /api/v2/experiments/{experiment_id}
# ---------------------------------------------------------------------------


def test_get_experiment_found(client: TestClient) -> None:
    experiment = make_experiment(experiment_id=42)

    class FakeRepo(BaseFakeRepo):
        def get_by_id(self, experiment_id: int) -> Optional[Experiment]:
            return experiment

    app.dependency_overrides[get_experiment_service] = lambda: ExperimentService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/experiments/42")
    assert resp.status_code == 200
    data = resp.json()
    assert data["experiment_id"] == 42
    assert data["experiment_name"] == "Test Experiment"
    assert data["experiment_url"] == "https://example.com/experiment"
    assert data["sow_id"] == 10
    assert "experiment_type" in data
    assert "created_at" in data
    assert "updated_at" in data


def test_get_experiment_not_found(client: TestClient) -> None:
    class FakeRepo(BaseFakeRepo):
        pass  # get_by_id returns None by default

    app.dependency_overrides[get_experiment_service] = lambda: ExperimentService(FakeRepo())  # type: ignore[arg-type]

    resp = client.get("/api/v2/experiments/999")
    assert resp.status_code == 404
    assert resp.json().get("error") == "Experiment not found"


def test_get_experiment_different_types(client: TestClient) -> None:
    for exp_type in ExperimentType:
        experiment = make_experiment(experiment_id=1)
        experiment.experiment_type = exp_type

        class FakeRepo(BaseFakeRepo):
            _experiment = experiment

            def get_by_id(self, experiment_id: int) -> Optional[Experiment]:
                return self._experiment

        app.dependency_overrides[get_experiment_service] = lambda: ExperimentService(FakeRepo())  # type: ignore[arg-type]

        resp = client.get("/api/v2/experiments/1")
        assert resp.status_code == 200, f"Expected 200 for experiment_type={exp_type}"
