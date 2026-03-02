from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.geography import GeographySchema
from jwt_validator import validate_jwt
from repositories.geography_repository import GeographyRepository
from services.geography_service import GeographyService

geography_router = APIRouter(prefix="/api/v2", tags=["geographies"])


def get_geography_repository() -> GeographyRepository:
    from database import manager as db_manager

    return GeographyRepository(db_manager.db)


def get_geography_service(
    repo: GeographyRepository = Depends(get_geography_repository),
) -> GeographyService:
    return GeographyService(repo)


@geography_router.get("/geographies", response_model=List[GeographySchema])
def get_geographies(
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: GeographyService = Depends(get_geography_service),
) -> JSONResponse:
    """Return active geographies assigned to the current user's client."""
    tenant_schema = authorization.get("orgId")
    result = service.get_geographies(tenant_schema)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))
