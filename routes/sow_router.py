from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

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
