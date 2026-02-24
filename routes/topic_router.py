from typing import Any, Dict

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.topic import TopicItemResponse, TopicsListResponse
from jwt_validator import validate_jwt
from services.topic_services import get_all_topics, get_topic_by_topic_id

topic_router = APIRouter(prefix="/api/v2/topics", tags=["topics"])


@topic_router.get("/", response_model=TopicsListResponse)
def list_topics(authorization: Dict[str, Any] = Depends(validate_jwt)) -> JSONResponse:
    """List all topics. Depends on JWT authentication."""
    tenant_schema = authorization.get("orgId", None)
    topics = get_all_topics(tenant_schema)
    if not tenant_schema:
        return JSONResponse(
            status_code=400,
            content={"error": "Authorization token missing tenant schema information"},
        )
    return JSONResponse(status_code=200, content=jsonable_encoder({"topics": topics}))


@topic_router.get("/{topic_id}", response_model=TopicItemResponse)
def get_topic(topic_id: str, authorization: Dict[str, Any] = Depends(validate_jwt)) -> JSONResponse:
    """Fetch a single topic by `topic_id`. Depends on JWT authentication."""
    tenant_schema = authorization.get("orgId", None)
    topic = get_topic_by_topic_id(tenant_schema, topic_id)
    if not topic:
        return JSONResponse(status_code=404, content={"error": "Topic not found"})

    return JSONResponse(status_code=200, content=jsonable_encoder({"topic": topic}))
