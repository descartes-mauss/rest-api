import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.company import CompanyResponse
from jwt_validator import validate_jwt
from repositories.company_repository import CompanyRepository
from services.company_service import CompanyService

company_router = APIRouter(prefix="/api/v2", tags=["company"])

logger = logging.getLogger("uvicorn.error")


def get_company_repository() -> CompanyRepository:
    from database import manager as db_manager

    return CompanyRepository(db_manager.db)


def get_company_service(
    repo: CompanyRepository = Depends(get_company_repository),
) -> CompanyService:
    return CompanyService(repo)


@company_router.get("/companies", response_model=CompanyResponse)
def get_company(
    authorization: Dict[str, Any] = Depends(validate_jwt),
    service: CompanyService = Depends(get_company_service),
) -> JSONResponse:
    """Return the company profile, brands, customer segments, and business categories."""
    tenant_schema = authorization.get("orgId")
    logger.error(tenant_schema)
    result = service.get_company(tenant_schema)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))
