"""Topic services handle business logic related to topics, such as fetching and processing topic data."""

from typing import Optional

from fastapi import HTTPException

from database.tenant_models.models import Topic
from repositories.topic_repository import TopicRepository


class TopicService:
    """Service layer for Topic-related business logic."""

    def __init__(self, topic_repository: TopicRepository) -> None:
        self.topic_repository = topic_repository

    def get_all_topics(self, organization_id: str) -> list[Topic]:
        """List all topics for a given organization."""
        if not organization_id:
            raise HTTPException(
                status_code=400, detail="Authorization token missing tenant schema information."
            )
        return self.topic_repository.get_all(organization_id)

    def get_topic_by_topic_id(self, organization_id: str, topic_id: str) -> Optional[Topic]:
        if not organization_id:
            raise HTTPException(
                status_code=400, detail="Authorization token missing tenant schema information."
            )
        return self.topic_repository.get_by_topic_id(organization_id, topic_id)
