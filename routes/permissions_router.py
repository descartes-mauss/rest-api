from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.permissions import PermissionsResponse
from jwt_validator import get_tenant_schema
from repositories.permissions_repository import PermissionsRepository
from services.permissions_service import PermissionsService

permissions_router = APIRouter(prefix="/api/v2", tags=["permissions"])


def get_permissions_repository() -> PermissionsRepository:
    from database import manager as db_manager

    return PermissionsRepository(db_manager.db)


def get_permissions_service(
    repo: PermissionsRepository = Depends(get_permissions_repository),
) -> PermissionsService:
    return PermissionsService(repo)


@permissions_router.get("/permissions/{sow_id}", response_model=PermissionsResponse)
def get_permissions(
    sow_id: int,
    tenant_schema: str = Depends(get_tenant_schema),
    service: PermissionsService = Depends(get_permissions_service),
) -> JSONResponse:
    """Return experiments, feature permissions, and opportunity platform flag for a SOW."""
    result = service.get_permissions(tenant_schema, sow_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))
