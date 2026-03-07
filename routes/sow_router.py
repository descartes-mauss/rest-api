from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.driver import DriverSchema
from database.schemas.insight import ForesightResponse, ForesightSearchRequest
from database.schemas.opportunity import OpportunitySchema
from database.schemas.shift import ShiftSchema
from database.schemas.sow import SowSchema
from database.schemas.topic import TopicSchema
from database.schemas.trend import TrendSchema
from jwt_validator import get_tenant_schema
from repositories.sow_repository import SowRepository
from services.foresight_service import ForesightService
from services.sow_service import SowService
from services.sow_sub_resource_service import SowSubResourceService

_VALID_MATURITY_FILTERS = frozenset({"Emerging", "Growing", "Mature", "New", "All"})

sow_router = APIRouter(prefix="/api/v2", tags=["sows"])


def get_sow_repository() -> SowRepository:
    from database import manager as db_manager

    return SowRepository(db_manager.db)


def get_sow_service(
    repo: SowRepository = Depends(get_sow_repository),
) -> SowService:
    return SowService(repo)


def get_sub_resource_service(
    repo: SowRepository = Depends(get_sow_repository),
) -> SowSubResourceService:
    return SowSubResourceService(repo)


def get_foresight_service(
    repo: SowRepository = Depends(get_sow_repository),
) -> ForesightService:
    return ForesightService(repo)


@sow_router.get("/sows", response_model=List[SowSchema])
def get_sows(
    tenant_schema: str = Depends(get_tenant_schema),
    service: SowService = Depends(get_sow_service),
) -> JSONResponse:
    """Return all latest live SOWs for the current tenant."""
    result = service.get_sows(tenant_schema)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/opportunities", response_model=List[OpportunitySchema])
def get_sow_opportunities(
    sow_id: int,
    tenant_schema: str = Depends(get_tenant_schema),
    service: SowSubResourceService = Depends(get_sub_resource_service),
) -> JSONResponse:
    """Return all opportunities (with nested topics) for the given SOW."""
    result = service.get_opportunities(tenant_schema, sow_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/shifts", response_model=List[ShiftSchema])
def get_sow_shifts(
    sow_id: int,
    tenant_schema: str = Depends(get_tenant_schema),
    service: SowSubResourceService = Depends(get_sub_resource_service),
) -> JSONResponse:
    """Return all shifts (with nested trends) for the given SOW."""
    result = service.get_shifts(tenant_schema, sow_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/drivers", response_model=List[DriverSchema])
def get_sow_drivers(
    sow_id: int,
    sort: Optional[str] = Query(default=None),
    order: Optional[str] = Query(default=None),
    tenant_schema: str = Depends(get_tenant_schema),
    service: SowSubResourceService = Depends(get_sub_resource_service),
) -> JSONResponse:
    """Return all drivers for the given SOW, optionally sorted by name."""
    result = service.get_drivers(tenant_schema, sow_id, sort=sort, order=order)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/topics", response_model=List[TopicSchema])
def get_sow_topics(
    sow_id: int,
    maturity_level: str = Query(default="All"),
    sort: Optional[str] = Query(default=None),
    order: Optional[str] = Query(default=None),
    tenant_schema: str = Depends(get_tenant_schema),
    service: SowSubResourceService = Depends(get_sub_resource_service),
) -> JSONResponse:
    """Return all topics for the given SOW, with optional maturity filtering and sorting."""
    effective_maturity = maturity_level if maturity_level in _VALID_MATURITY_FILTERS else "All"
    result = service.get_topics(tenant_schema, sow_id, effective_maturity, sort=sort, order=order)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/trends", response_model=List[TrendSchema])
def get_sow_trends(
    sow_id: int,
    maturity_level: str = Query(default="All"),
    sort: Optional[str] = Query(default=None),
    order: Optional[str] = Query(default=None),
    tenant_schema: str = Depends(get_tenant_schema),
    service: SowSubResourceService = Depends(get_sub_resource_service),
) -> JSONResponse:
    """Return all trends for the given SOW, with optional maturity filtering and sorting."""
    effective_maturity = maturity_level if maturity_level in _VALID_MATURITY_FILTERS else "All"
    result = service.get_trends(tenant_schema, sow_id, effective_maturity, sort=sort, order=order)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/versions", response_model=List[SowSchema])
def get_sow_versions(
    sow_id: int,
    tenant_schema: str = Depends(get_tenant_schema),
    service: SowService = Depends(get_sow_service),
) -> JSONResponse:
    """Return all live versions of the given SOW, newest first."""
    result = service.get_versions(tenant_schema, sow_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/foresight", response_model=ForesightResponse)
def get_sow_foresight(
    sow_id: int,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=8, le=8),
    tenant_schema: str = Depends(get_tenant_schema),
    service: ForesightService = Depends(get_foresight_service),
) -> JSONResponse:
    """Return a paginated list of foresight insights for the given SOW."""
    result = service.get_foresight(tenant_schema, sow_id, page=page, limit=limit)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.post("/sows/{sow_id}/foresight/search", response_model=ForesightResponse)
def search_sow_foresight(
    sow_id: int,
    body: ForesightSearchRequest,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=8, le=8),
    tenant_schema: str = Depends(get_tenant_schema),
    service: ForesightService = Depends(get_foresight_service),
) -> JSONResponse:
    """Return a filtered paginated list of foresight insights, optionally by topic/trend IDs."""
    result = service.get_foresight_search(
        tenant_schema,
        sow_id,
        topic_ids=body.topic_ids or None,
        trend_ids=body.trend_ids or None,
        page=page,
        limit=limit,
    )
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}", response_model=SowSchema)
def get_sow(
    sow_id: int,
    tenant_schema: str = Depends(get_tenant_schema),
    service: SowService = Depends(get_sow_service),
) -> JSONResponse:
    """Return a single active SOW by ID."""
    result = service.get_sow(tenant_schema, sow_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))
