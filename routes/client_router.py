import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends

from database.manager import get_all
from database.public_models.models import PublicSow
from database.tenant_models.models import TenantSow
from jwt_validator import validate_jwt

logger = logging.getLogger("uvicorn.error")

client_router = APIRouter(prefix="/api/v2", dependencies=[Depends(validate_jwt)])


@client_router.post("/demo")
def protected(authorization: Dict[str, Any] = Depends(validate_jwt)) -> dict[str, Any]:
    org_id = authorization.get("orgId", "")
    public_sows: list[PublicSow] = get_all(PublicSow, tenant_schema=org_id)
    tenant_sows: list[TenantSow] = get_all(TenantSow, tenant_schema=org_id)
    return {
        "status": "OK",
        "sows": tenant_sows,
        "public_sows": public_sows,
    }
