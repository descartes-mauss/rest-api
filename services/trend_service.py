"""Service layer for the trends endpoints."""

from collections import defaultdict
from typing import Dict, List

from fastapi import HTTPException

from database.schemas.topic import TopicSchema
from database.schemas.trend import TrendSchema
from database.tenant_models.models import Topic, Trend
from repositories.sow_repository import SowRepository
from services._maturity_helpers import (
    _build_topic_context,
    _build_trend_context,
    _topic_to_schema,
    _trend_to_schema,
)

__all__ = ["TrendService"]


class TrendService:
    """Fetches and assembles single-trend and trend-topics responses."""

    def __init__(self, sow_repository: SowRepository) -> None:
        self.sow_repository = sow_repository

    def _get_trend_or_404(self, tenant_schema: str, trend_id: str) -> Trend:
        trend = self.sow_repository.get_trend_by_trend_id(tenant_schema, trend_id)
        if trend is None:
            raise HTTPException(status_code=404, detail="Trend not available")
        return trend

    def get_trend(self, tenant_schema: str, trend_id: str) -> TrendSchema:
        """Return a single TrendSchema for the most recent matching trend_id."""
        trend = self._get_trend_or_404(tenant_schema, trend_id)
        ssid = trend.ssid or 0
        sow_sid = trend.sid

        scores = self.sow_repository.get_maturity_scores_for_trend_ids(tenant_schema, [ssid])
        score_ids = [ms.id for ms in scores if ms.id is not None]
        sources = self.sow_repository.get_maturity_score_sources(tenant_schema, score_ids)
        deltas = self.sow_repository.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, [trend.trend_id]
        )
        related_topics = self.sow_repository.get_topics_for_trends(tenant_schema, [ssid])

        trend_ctx = _build_trend_context(scores, sources, deltas)

        rel_topics_by_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for t in related_topics:
            if t.ssid is not None:
                rel_topics_by_ssid[t.ssid].append(t)

        return _trend_to_schema(trend, trend_ctx, rel_topics_by_ssid)

    def get_trend_topics(self, tenant_schema: str, trend_id: str) -> List[TopicSchema]:
        """Return all topics belonging to the given trend_id."""
        trend = self._get_trend_or_404(tenant_schema, trend_id)
        ssid = trend.ssid or 0
        sow_sid = trend.sid

        topics = self.sow_repository.get_topics_for_trends(tenant_schema, [ssid])
        if not topics:
            return []

        topic_tids = [t.tid for t in topics if t.tid is not None]
        topic_id_strings = [t.topic_id for t in topics]

        topic_scores = self.sow_repository.get_maturity_scores_for_topic_ids(
            tenant_schema, topic_tids
        )
        topic_score_ids = [ms.id for ms in topic_scores if ms.id is not None]
        topic_sources = self.sow_repository.get_maturity_score_sources(
            tenant_schema, topic_score_ids
        )
        topic_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_topic_ids(
            tenant_schema, sow_sid, topic_id_strings
        )
        t2d_rows = self.sow_repository.get_topic_drivers_by_topic_ids(tenant_schema, topic_tids)

        # Fetch trend maturity data so each TopicSchema can embed its parent trend
        trend_scores = self.sow_repository.get_maturity_scores_for_trend_ids(tenant_schema, [ssid])
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        trend_sources = self.sow_repository.get_maturity_score_sources(
            tenant_schema, trend_score_ids
        )
        trend_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, [trend.trend_id]
        )

        topic_ctx = _build_topic_context(topic_scores, topic_sources, topic_deltas)

        drivers_by_tid: Dict[int, List[int]] = defaultdict(list)
        for row in t2d_rows:
            drivers_by_tid[row.tid].append(row.did)

        trend_ctx = _build_trend_context(trend_scores, trend_sources, trend_deltas)

        topics_by_trend_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for t in topics:
            if t.ssid is not None:
                topics_by_trend_ssid[t.ssid].append(t)

        trend_schema = _trend_to_schema(trend, trend_ctx, topics_by_trend_ssid)
        trend_schema_by_ssid = {ssid: trend_schema} if trend.ssid else {}

        return [
            _topic_to_schema(topic, topic_ctx, trend_schema_by_ssid, drivers_by_tid)
            for topic in topics
        ]
