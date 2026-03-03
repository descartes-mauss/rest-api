from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.brand import BrandSchema
from jwt_validator import validate_jwt
from repositories.brand_repository import BrandRepository
from services.brand_service import BrandService

brand_router = APIRouter(prefix="/api/v2", tags=["brands"])


def get_brand_repository() -> BrandRepository:
    from database import manager as db_manager

    return BrandRepository(db_manager.db)


def get_brand_service(
    repo: BrandRepository = Depends(get_brand_repository),
) -> BrandService:
    return BrandService(repo)


@brand_router.get("/brands", response_model=List[BrandSchema])
def get_brands(
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: BrandService = Depends(get_brand_service),
) -> JSONResponse:
    """Return all brands for the current tenant, distinct by brand name."""
    tenant_schema = authorization.get("orgId")
    result = service.get_brands(tenant_schema)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@brand_router.get("/brands/{brand_id}", response_model=BrandSchema)
def get_brand(
    brand_id: int,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: BrandService = Depends(get_brand_service),
) -> JSONResponse:
    """Return a single brand by ID."""
    tenant_schema = authorization.get("orgId")
    result = service.get_brand(tenant_schema, brand_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))
