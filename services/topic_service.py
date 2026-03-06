"""Service layer for the topics endpoint."""

from collections import defaultdict
from typing import Dict, List, Optional

from database.schemas.topic import (
    Topic2DriverDriverSchema,
    Topic2DriverSchema,
    TopicSchema,
    TopicSourceSchema,
    TopicSourcesResponse,
)
from database.schemas.trend import TrendSchema
from database.tenant_models.models import Topic
from repositories.sow_repository import SowRepository
from repositories.topic_repository import TopicRepository
from services._maturity_helpers import (
    _build_topic_context,
    _build_trend_context,
    _topic_to_schema,
    _trend_to_schema,
)

_STATUS_MAP = {
    0: "Undefined",
    1: "Analyze",
    2: "Monitor",
    3: "Important",
    4: "Disregard",
}


class TopicService:
    """Service layer for Topic-related business logic."""

    def __init__(self, topic_repository: TopicRepository, sow_repository: SowRepository) -> None:
        self.topic_repository = topic_repository
        self.sow_repository = sow_repository

    def get_all_topics(self, organization_id: str) -> List[Topic]:
        """List all topics for a given organization."""
        return self.topic_repository.get_all(organization_id)

    def get_topic_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[TopicSchema]:
        """Return a fully enriched TopicSchema by its string topic_id, or None."""
        topic = self.topic_repository.get_by_topic_id(tenant_schema, topic_id)
        if topic is None or topic.tid is None:
            return None

        tid = topic.tid

        # Driver DIDs
        t2d_rows = self.topic_repository.get_topic2drivers_with_driver(tenant_schema, tid)
        drivers_by_tid: Dict[int, List[int]] = defaultdict(list)
        for t2d, _ in t2d_rows:
            if t2d.did is not None:
                drivers_by_tid[tid].append(t2d.did)

        # Topic maturity scores + deltas
        topic_scores = self.topic_repository.get_maturity_scores_for_topic(tenant_schema, tid)
        score_ids = [ms.id for ms in topic_scores if ms.id is not None]
        topic_sources = self.sow_repository.get_maturity_score_sources(tenant_schema, score_ids)
        topic_deltas = self.topic_repository.get_maturity_score_deltas_for_topic(
            tenant_schema, topic.sid, topic.topic_id
        )

        topic_ctx = _build_topic_context(topic_scores, topic_sources, topic_deltas)

        # Trend (UnlinkedTrendSerializer: related_topics=[])
        trend_schema_by_ssid: Dict[int, TrendSchema] = {}
        if topic.ssid is not None:
            trends = self.sow_repository.get_trends_by_ssids(tenant_schema, [topic.ssid])
            trend = trends[0] if trends else None
            if trend is not None:
                tr_scores = self.sow_repository.get_maturity_scores_for_trend_ids(
                    tenant_schema, [topic.ssid]
                )
                tr_score_ids = [ms.id for ms in tr_scores if ms.id is not None]
                tr_sources = self.sow_repository.get_maturity_score_sources(
                    tenant_schema, tr_score_ids
                )
                tr_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_trends(
                    tenant_schema, topic.sid, [trend.trend_id]
                )
                trend_ctx = _build_trend_context(tr_scores, tr_sources, tr_deltas)
                trend_schema_by_ssid[topic.ssid] = _trend_to_schema(
                    trend,
                    trend_ctx,
                    rel_topics_by_ssid={},  # mirrors UnlinkedTrendSerializer
                )

        return _topic_to_schema(topic, topic_ctx, trend_schema_by_ssid, drivers_by_tid)

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
