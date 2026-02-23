import logging

from fastapi import APIRouter, Depends

from database.manager import get_all, get_topics, get_topics_trends
from database.tenant_models.models import TenantSow, Topic
from jwt_validator import validate_jwt

logger = logging.getLogger("uvicorn.error")

client_router = APIRouter(prefix="/api/v2", dependencies=[Depends(validate_jwt)])


@client_router.post("/protected")
def protected(authorization: dict = Depends(validate_jwt)):
    org_id = authorization.get("orgId")
    sows: list[TenantSow] = get_all(TenantSow, tenant_schema=org_id)
    topics: list[Topic] = get_topics(
        "08a7354801f27007937004e6b219e2d1ff1c5d9c8a4c65cd48c12bf13e6743bd", tenant_schema=org_id
    )
    topics_trends = get_topics_trends(
        "08a7354801f27007937004e6b219e2d1ff1c5d9c8a4c65cd48c12bf13e6743bd", tenant_schema=org_id
    )
    return {
        "status": "OK",
        "topics": topics,
        "sows": sows,
        "topics_trends": [{**topic.model_dump(), "trend": trend} for topic, trend in topics_trends],
    }
