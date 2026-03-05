from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.experiment import ExperimentSchema
from jwt_validator import get_tenant_schema
from repositories.experiment_repository import ExperimentRepository
from services.experiment_service import ExperimentService

experiment_router = APIRouter(prefix="/api/v2/experiments", tags=["experiments"])


def get_experiment_repository() -> ExperimentRepository:
    from database import manager as db_manager

    return ExperimentRepository(db_manager.db)


def get_experiment_service(
    repo: ExperimentRepository = Depends(get_experiment_repository),
) -> ExperimentService:
    return ExperimentService(repo)


@experiment_router.get("/{experiment_id}", response_model=ExperimentSchema)
def get_experiment(
    experiment_id: int,
    tenant_schema: str = Depends(get_tenant_schema),
    experiment_service: ExperimentService = Depends(get_experiment_service),
) -> JSONResponse:
    """Return a single Experiment by its primary key."""
    experiment = experiment_service.get_experiment(experiment_id)
    if not experiment:
        return JSONResponse(status_code=404, content={"error": "Experiment not found"})
    return JSONResponse(status_code=200, content=jsonable_encoder(experiment))
