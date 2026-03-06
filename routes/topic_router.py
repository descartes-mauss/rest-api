import os
from typing import List

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from database.schemas.deepdive import DeepdiveResponse
from database.schemas.topic import (
    Topic2DriverSchema,
    TopicSchema,
    TopicsListResponse,
    TopicSourcesResponse,
    UpdateTopicStatusRequest,
)
from external.s3_rest_client import S3RestClient
from jwt_validator import get_tenant_schema
from repositories.sow_repository import SowRepository
from repositories.topic_repository import TopicRepository
from services.deepdive_service import DeepdiveService
from services.topic_service import TopicService

topic_router = APIRouter(prefix="/api/v2/topics", tags=["topics"])


def get_topic_repository() -> TopicRepository:
    """Create a `TopicRepository` using the real DB provider.

    Importing `database.manager` inside the function defers construction of
    the `db` object until the dependency is resolved (request time), and
    allows tests to override the dependency with a mock provider.
    """
    from database import manager as db_manager

    return TopicRepository(db_manager.db)


def get_sow_repository() -> SowRepository:
    """Create a `SowRepository` using the real DB provider."""
    from database import manager as db_manager

    return SowRepository(db_manager.db)


def get_topic_service(
    topic_repo: TopicRepository = Depends(get_topic_repository),
    sow_repo: SowRepository = Depends(get_sow_repository),
) -> TopicService:
    return TopicService(topic_repo, sow_repo)


def get_deepdive_service(
    topic_repo: TopicRepository = Depends(get_topic_repository),
    sow_repo: SowRepository = Depends(get_sow_repository),
) -> DeepdiveService:
    s3_client = S3RestClient(
        base_url=os.environ.get("S3_URL", ""),
        api_key=os.environ.get("S3_API_KEY", ""),
    )
    return DeepdiveService(topic_repo, sow_repo, s3_client)


@topic_router.get("/", response_model=TopicsListResponse)
def list_topics(
    tenant_schema: str = Depends(get_tenant_schema),
    topic_service: TopicService = Depends(get_topic_service),
) -> JSONResponse:
    """List all topics. Depends on JWT authentication and injected service."""
    topics = topic_service.get_all_topics(tenant_schema)
    return JSONResponse(status_code=200, content=jsonable_encoder({"topics": topics}))


@topic_router.get("/{topic_id}", response_model=TopicSchema)
def get_topic(
    topic_id: str,
    tenant_schema: str = Depends(get_tenant_schema),
    topic_service: TopicService = Depends(get_topic_service),
) -> JSONResponse:
    """Fetch a single topic by `topic_id`. Depends on JWT authentication."""
    topic = topic_service.get_topic_by_topic_id(tenant_schema, topic_id)
    if not topic:
        return JSONResponse(status_code=404, content={"error": "Topic not found"})
    return JSONResponse(status_code=200, content=jsonable_encoder(topic))


@topic_router.get("/{topic_id}/sources", response_model=TopicSourcesResponse)
def get_topic_sources(
    topic_id: str,
    tenant_schema: str = Depends(get_tenant_schema),
    topic_service: TopicService = Depends(get_topic_service),
) -> JSONResponse:
    """Return all sources for the topic identified by `topic_id`."""
    result = topic_service.get_topic_sources(tenant_schema, topic_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@topic_router.post("/{tid}/status")
def update_topic_status(
    tid: int,
    body: UpdateTopicStatusRequest,
    tenant_schema: str = Depends(get_tenant_schema),
    topic_service: TopicService = Depends(get_topic_service),
) -> JSONResponse:
    """Update the status of a topic by its primary key (`tid`)."""
    success = topic_service.update_topic_status(tenant_schema, tid, body.status_id)
    if not success:
        return JSONResponse(status_code=404, content={"error": "Invalid status or topic not found"})
    return JSONResponse(status_code=200, content={"success": True})


@topic_router.get("/{tid}/drivers", response_model=List[Topic2DriverSchema])
def get_topic_drivers(
    tid: int,
    tenant_schema: str = Depends(get_tenant_schema),
    topic_service: TopicService = Depends(get_topic_service),
) -> JSONResponse:
    """Return all driver relationships for the topic identified by its primary key (`tid`)."""
    result = topic_service.get_topic_drivers(tenant_schema, tid)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))


@topic_router.get("/{topic_id}/deepdive", response_model=DeepdiveResponse)
def get_topic_deepdive(
    topic_id: str,
    tenant_schema: str = Depends(get_tenant_schema),
    deepdive_service: DeepdiveService = Depends(get_deepdive_service),
) -> JSONResponse:
    """Return provocations, evolution, manifestations and market insights for a topic."""
    result = deepdive_service.get_topic_deepdive(tenant_schema, topic_id)
    return JSONResponse(status_code=200, content=jsonable_encoder(result))
