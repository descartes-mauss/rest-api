"""Service layer for the topics endpoint."""

from typing import List, Optional

from database.tenant_models.models import Topic
from repositories.topic_repository import TopicRepository


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


__all__ = ["TopicService"]
