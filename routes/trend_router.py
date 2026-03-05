"""FastAPI router for the trends endpoints."""

from typing import List

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.topic import TopicSchema
from database.schemas.trend import TrendSchema
from jwt_validator import get_tenant_schema
from repositories.sow_repository import SowRepository
from services.trend_service import TrendService

trend_router = APIRouter(prefix="/api/v2/trends", tags=["trends"])


def get_sow_repository() -> SowRepository:
    from database import manager as db_manager

    return SowRepository(db_manager.db)


def get_trend_service(
    repo: SowRepository = Depends(get_sow_repository),
) -> TrendService:
    return TrendService(repo)


@trend_router.get("/{trend_id}", response_model=TrendSchema)
def get_trend(
    trend_id: str,
    tenant_schema: str = Depends(get_tenant_schema),
    service: TrendService = Depends(get_trend_service),
) -> JSONResponse:
    """Return a single trend by its trend_id string (most recent non-deleted)."""
    result = service.get_trend(tenant_schema, trend_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@trend_router.get("/{trend_id}/topics", response_model=List[TopicSchema])
def get_trend_topics(
    trend_id: str,
    tenant_schema: str = Depends(get_tenant_schema),
    service: TrendService = Depends(get_trend_service),
) -> JSONResponse:
    """Return all topics associated with the given trend."""
    result = service.get_trend_topics(tenant_schema, trend_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))
