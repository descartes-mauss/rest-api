"""Service layer for the topics endpoint."""

from typing import List, Optional

from database.schemas.topic import (
    Topic2DriverDriverSchema,
    Topic2DriverSchema,
    TopicSourceSchema,
    TopicSourcesResponse,
)
from database.tenant_models.models import Topic
from repositories.topic_repository import TopicRepository

_STATUS_MAP = {
    0: "Undefined",
    1: "Analyze",
    2: "Monitor",
    3: "Important",
    4: "Disregard",
}


class TopicService:
    """Service layer for Topic-related business logic."""

    def __init__(self, topic_repository: TopicRepository) -> None:
        self.topic_repository = topic_repository

    def get_all_topics(self, organization_id: str) -> List[Topic]:
        """List all topics for a given organization."""
        return self.topic_repository.get_all(organization_id)

    def get_topic_by_topic_id(self, organization_id: str, topic_id: str) -> Optional[Topic]:
        """Return a single topic by its string topic_id, or None."""
        return self.topic_repository.get_by_topic_id(organization_id, topic_id)

    def get_topic_sources(self, tenant_schema: str, topic_id: str) -> TopicSourcesResponse:
        """Return sources for the most recent non-deleted topic matching topic_id."""
        topic = self.topic_repository.get_by_topic_id(tenant_schema, topic_id)
        if topic is None or topic.tid is None:
            return TopicSourcesResponse()

        rows = self.topic_repository.get_sources_for_topic(tenant_schema, topic.tid)
        if not rows:
            return TopicSourcesResponse()

        last_updated = rows[0][1].load_date.strftime("%Y-%m-%d")
        sources = [
            TopicSourceSchema(
                id=src.soid or 0,
                url=src.source_url,
                title=src.source_title,
                internal_classification=src.internal_classification,
            )
            for _, src in rows
        ]
        return TopicSourcesResponse(last_updated=last_updated, topic_sources=sources)

    def update_topic_status(self, tenant_schema: str, tid: int, status_id: int) -> bool:
        """Update topic_status. Returns False if status_id is invalid or topic not found."""
        if status_id not in _STATUS_MAP:
            return False
        return self.topic_repository.update_status(tenant_schema, tid, status_id)

    def get_topic_drivers(self, tenant_schema: str, tid: int) -> List[Topic2DriverSchema]:
        """Return driver relationships for the given topic tid."""
        rows = self.topic_repository.get_topic2drivers_with_driver(tenant_schema, tid)
        result = []
        for t2d, driver in rows:
            if t2d.strength is not None and t2d.polarity is not None:
                influence: Optional[float] = t2d.polarity * t2d.strength
            else:
                influence = None
            result.append(
                Topic2DriverSchema(
                    driver=Topic2DriverDriverSchema(
                        driver_name=driver.driver_name,
                        driver_description=driver.driver_description,
                    ),
                    driver_influence=influence,
                )
            )
        return result


__all__ = ["TopicService"]
