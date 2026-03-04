from typing import Any, Dict, List
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.tenant_user import (
    TenantUserCreateSchema,
    TenantUserSchema,
    TenantUserUpdateSchema,
)
from jwt_validator import validate_jwt
from repositories.tenant_user_repository import TenantUserRepository
from services.tenant_user_service import TenantUserService

tenant_user_router = APIRouter(prefix="/api/v2/users", tags=["users"])


def get_tenant_user_repository() -> TenantUserRepository:
    """Deferred loading to allow test overrides."""
    from database import manager as db_manager

    return TenantUserRepository(db_manager.db)


def get_tenant_user_service(
    repo: TenantUserRepository = Depends(get_tenant_user_repository),
) -> TenantUserService:
    return TenantUserService(repo)


@tenant_user_router.get("/", response_model=List[TenantUserSchema])
def list_tenant_users(
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: TenantUserService = Depends(get_tenant_user_service),
) -> JSONResponse:
    """Return all tenant users."""
    tenant_schema = authorization.get("orgId")
    users = service.get_all_users(tenant_schema)
    return JSONResponse(status_code=200, content=jsonable_encoder(users))


@tenant_user_router.get("/{user_id}", response_model=TenantUserSchema)
def get_tenant_user(
    user_id: UUID,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: TenantUserService = Depends(get_tenant_user_service),
) -> JSONResponse:
    """Return a single tenant user by ID."""
    tenant_schema = authorization.get("orgId")
    user = service.get_user(tenant_schema, user_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(user))


@tenant_user_router.post("/", response_model=TenantUserSchema, status_code=201)
def create_tenant_user(
    payload: TenantUserCreateSchema,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: TenantUserService = Depends(get_tenant_user_service),
) -> JSONResponse:
    """Create a new tenant user."""
    tenant_schema = authorization.get("orgId")
    user = service.create_user(tenant_schema, payload)
    return JSONResponse(status_code=201, content=jsonable_encoder(user))


@tenant_user_router.put("/{user_id}", response_model=TenantUserSchema)
def update_tenant_user(
    user_id: UUID,
    payload: TenantUserUpdateSchema,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: TenantUserService = Depends(get_tenant_user_service),
) -> JSONResponse:
    """Fully replace an existing tenant user."""
    tenant_schema = authorization.get("orgId")
    user = service.update_user(tenant_schema, user_id, payload)
    return JSONResponse(status_code=200, content=jsonable_encoder(user))


@tenant_user_router.delete("/{user_id}", status_code=204)
def delete_tenant_user(
    user_id: UUID,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: TenantUserService = Depends(get_tenant_user_service),
) -> JSONResponse:
    """Delete a tenant user by ID."""
    tenant_schema = authorization.get("orgId")
    service.delete_user(tenant_schema, user_id)
    return JSONResponse(status_code=204, content=None)
