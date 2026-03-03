from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.driver import DriverSchema
from database.schemas.shift import ShiftSchema
from database.schemas.sow import SowSchema
from jwt_validator import validate_jwt
from repositories.sow_repository import SowRepository
from services.sow_service import SowService

sow_router = APIRouter(prefix="/api/v2", tags=["sows"])


def get_sow_repository() -> SowRepository:
    from database import manager as db_manager

    return SowRepository(db_manager.db)


def get_sow_service(
    repo: SowRepository = Depends(get_sow_repository),
) -> SowService:
    return SowService(repo)


@sow_router.get("/sows", response_model=List[SowSchema])
def get_sows(
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: SowService = Depends(get_sow_service),
) -> JSONResponse:
    """Return all latest live SOWs for the current tenant."""
    tenant_schema = authorization.get("orgId")
    result = service.get_sows(tenant_schema)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/shifts", response_model=List[ShiftSchema])
def get_sow_shifts(
    sow_id: int,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: SowService = Depends(get_sow_service),
) -> JSONResponse:
    """Return all shifts (with nested trends) for the given SOW."""
    tenant_schema = authorization.get("orgId")
    result = service.get_shifts(tenant_schema, sow_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/drivers", response_model=List[DriverSchema])
def get_sow_drivers(
    sow_id: int,
    sort: Optional[str] = Query(default=None),
    order: Optional[str] = Query(default=None),
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: SowService = Depends(get_sow_service),
) -> JSONResponse:
    """Return all drivers for the given SOW, optionally sorted by name."""
    tenant_schema = authorization.get("orgId")
    result = service.get_drivers(tenant_schema, sow_id, sort=sort, order=order)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}/versions", response_model=List[SowSchema])
def get_sow_versions(
    sow_id: int,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: SowService = Depends(get_sow_service),
) -> JSONResponse:
    """Return all live versions of the given SOW, newest first."""
    tenant_schema = authorization.get("orgId")
    result = service.get_versions(tenant_schema, sow_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@sow_router.get("/sows/{sow_id}", response_model=SowSchema)
def get_sow(
    sow_id: int,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: SowService = Depends(get_sow_service),
) -> JSONResponse:
    """Return a single active SOW by ID."""
    tenant_schema = authorization.get("orgId")
    result = service.get_sow(tenant_schema, sow_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))
