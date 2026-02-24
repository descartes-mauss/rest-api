from typing import Any, Dict

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.topic import TopicItemResponse, TopicsListResponse
from jwt_validator import validate_jwt
from repositories.topic_repository import TopicRepository
from services.topic_services import TopicService

topic_router = APIRouter(prefix="/api/v2/topics", tags=["topics"])


def get_topic_repository() -> TopicRepository:
    """Create a `TopicRepository` using the real DB provider.

    Importing `database.manager` inside the function defers construction of
    the `db` object until the dependency is resolved (request time), and
    allows tests to override the dependency with a mock provider.
    """
    from database import manager as db_manager

    return TopicRepository(db_manager.db)


def get_topic_service(repo: TopicRepository = Depends(get_topic_repository)) -> TopicService:
    return TopicService(repo)


@topic_router.get("/", response_model=TopicsListResponse)
def list_topics(
    authorization: Dict[str, Any] = Depends(validate_jwt),
    topic_service: TopicService = Depends(get_topic_service),
) -> JSONResponse:
    """List all topics. Depends on JWT authentication and injected service."""
    tenant_schema = authorization.get("orgId", None)
    topics = topic_service.get_all_topics(tenant_schema)
    return JSONResponse(status_code=200, content=jsonable_encoder({"topics": topics}))


@topic_router.get("/{topic_id}", response_model=TopicItemResponse)
def get_topic(
    topic_id: str,
    authorization: Dict[str, Any] = Depends(validate_jwt),
    topic_service: TopicService = Depends(get_topic_service),
) -> JSONResponse:
    """Fetch a single topic by `topic_id`. Depends on JWT authentication."""
    tenant_schema = authorization.get("orgId", None)
    topic = topic_service.get_topic_by_topic_id(tenant_schema, topic_id)
    if not topic:
        return JSONResponse(status_code=404, content={"error": "Topic not found"})
    return JSONResponse(status_code=200, content=jsonable_encoder({"topic": topic}))
