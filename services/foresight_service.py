"""Service layer for the foresight endpoints."""

from collections import defaultdict
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from database.schemas.insight import ForesightResponse, InsightSchema, InsightSourceSchema
from database.schemas.topic import TopicSchema
from database.schemas.trend import TrendSchema
from database.tenant_models.models import Insight, InsightSource, TenantSow, Topic
from repositories.protocols import SowRepositoryProtocol
from services.assemblers.maturity_context import _build_topic_context, _build_trend_context
from services.assemblers.topic_assembler import _topic_to_schema
from services.assemblers.trend_assembler import _trend_to_schema

_WEEKLY_INSIGHTS_MAX_SIZE = 8


class ForesightService:
    """Orchestrates foresight pagination, insight assembly, and entity predictions."""

    def __init__(self, sow_repository: SowRepositoryProtocol) -> None:
        self.sow_repository = sow_repository

    def _get_sow_or_404(self, tenant_schema: str, sow_id: int) -> TenantSow:
        sow = self.sow_repository.get_sow_by_id(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SOW not available")
        return sow

    def get_foresight(
        self,
        tenant_schema: str,
        sow_id: int,
        page: int = 1,
        limit: int = _WEEKLY_INSIGHTS_MAX_SIZE,
    ) -> ForesightResponse:
        sow = self._get_sow_or_404(tenant_schema, sow_id)

        if not sow.cs_sow_id:
            return ForesightResponse(total=0, limit=limit, hasNext=False, hasPrev=False)

        offset = (page - 1) * limit
        total, insights = self.sow_repository.get_insights_for_cs_sow_id(
            tenant_schema, sow.cs_sow_id, offset=offset, limit=limit
        )
        return self._assemble_foresight(tenant_schema, sow, insights, total, page, limit)

    def get_foresight_search(
        self,
        tenant_schema: str,
        sow_id: int,
        topic_ids: Optional[List[str]] = None,
        trend_ids: Optional[List[str]] = None,
        page: int = 1,
        limit: int = _WEEKLY_INSIGHTS_MAX_SIZE,
    ) -> ForesightResponse:
        sow = self._get_sow_or_404(tenant_schema, sow_id)

        if not sow.cs_sow_id:
            return ForesightResponse(total=0, limit=limit, hasNext=False, hasPrev=False)

        offset = (page - 1) * limit
        entity_ids = list({*(topic_ids or []), *(trend_ids or [])})

        if entity_ids:
            total, insights = self.sow_repository.get_insights_filtered(
                tenant_schema, sow.cs_sow_id, entity_ids=entity_ids, offset=offset, limit=limit
            )
        else:
            total, insights = self.sow_repository.get_insights_for_cs_sow_id(
                tenant_schema, sow.cs_sow_id, offset=offset, limit=limit
            )
        return self._assemble_foresight(tenant_schema, sow, insights, total, page, limit)

    def _fetch_topic_predictions(
        self, tenant_schema: str, sow_sid: int, entity_ids: List[str]
    ) -> Dict[str, List[TopicSchema]]:
        topic_schema_by_topic_id: Dict[str, List[TopicSchema]] = defaultdict(list)
        pred_topics = self.sow_repository.get_topics_by_topic_str_ids(
            tenant_schema, sow_sid, entity_ids
        )
        if not pred_topics:
            return topic_schema_by_topic_id

        topic_tids = [t.tid for t in pred_topics if t.tid is not None]
        topic_id_strings = [t.topic_id for t in pred_topics]
        topic_scores = self.sow_repository.get_maturity_scores_for_topic_ids(
            tenant_schema, topic_tids
        )
        topic_score_ids = [ms.id for ms in topic_scores if ms.id is not None]
        t_sources = self.sow_repository.get_maturity_score_sources(tenant_schema, topic_score_ids)
        topic_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_topic_ids(
            tenant_schema, sow_sid, topic_id_strings
        )
        t2d_rows = self.sow_repository.get_topic_drivers_by_topic_ids(tenant_schema, topic_tids)

        topic_trend_ssids = list({t.ssid for t in pred_topics if t.ssid is not None})
        topic_trends = self.sow_repository.get_trends_by_ssids(tenant_schema, topic_trend_ssids)
        topic_trend_id_strings = [tr.trend_id for tr in topic_trends]
        topic_trend_scores = self.sow_repository.get_maturity_scores_for_trend_ids(
            tenant_schema, topic_trend_ssids
        )
        topic_trend_score_ids = [ms.id for ms in topic_trend_scores if ms.id is not None]
        topic_trend_sources = self.sow_repository.get_maturity_score_sources(
            tenant_schema, topic_trend_score_ids
        )
        topic_trend_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, topic_trend_id_strings
        )

        topic_ctx = _build_topic_context(topic_scores, t_sources, topic_deltas)

        drivers_by_tid: Dict[int, List[int]] = defaultdict(list)
        for row in t2d_rows:
            drivers_by_tid[row.tid].append(row.did)

        trend_ctx = _build_trend_context(
            topic_trend_scores, topic_trend_sources, topic_trend_deltas
        )

        topics_by_trend_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for t in pred_topics:
            if t.ssid is not None:
                topics_by_trend_ssid[t.ssid].append(t)

        trend_schema_by_ssid: Dict[int, TrendSchema] = {
            tr.ssid: _trend_to_schema(tr, trend_ctx, topics_by_trend_ssid)
            for tr in topic_trends
            if tr.ssid is not None
        }

        for t in pred_topics:
            topic_schema_by_topic_id[t.topic_id].append(
                _topic_to_schema(t, topic_ctx, trend_schema_by_ssid, drivers_by_tid)
            )
        return topic_schema_by_topic_id

    def _fetch_trend_predictions(
        self, tenant_schema: str, sow_sid: int, entity_ids: List[str]
    ) -> Dict[str, List[TrendSchema]]:
        trend_schema_by_trend_id: Dict[str, List[TrendSchema]] = defaultdict(list)
        pred_trends = self.sow_repository.get_trends_by_trend_str_ids(
            tenant_schema, sow_sid, entity_ids
        )
        if not pred_trends:
            return trend_schema_by_trend_id

        trend_ssids = [t.ssid for t in pred_trends if t.ssid is not None]
        trend_id_strings = [t.trend_id for t in pred_trends]
        trend_scores = self.sow_repository.get_maturity_scores_for_trend_ids(
            tenant_schema, trend_ssids
        )
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        tr_sources = self.sow_repository.get_maturity_score_sources(tenant_schema, trend_score_ids)
        trend_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, trend_id_strings
        )
        rel_topics = self.sow_repository.get_topics_for_trends(tenant_schema, trend_ssids)
        rt_tids = [rt.tid for rt in rel_topics if rt.tid is not None]
        t2d_for_trends = self.sow_repository.get_topic_drivers_by_topic_ids(tenant_schema, rt_tids)

        trend_ctx = _build_trend_context(trend_scores, tr_sources, trend_deltas)

        rel_topics_by_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for rt in rel_topics:
            if rt.ssid is not None:
                rel_topics_by_ssid[rt.ssid].append(rt)

        dids_by_topic_tid: Dict[int, List[int]] = defaultdict(list)
        for row in t2d_for_trends:
            dids_by_topic_tid[row.tid].append(row.did)

        topic_tids_by_trend_ssid: Dict[int, List[int]] = defaultdict(list)
        for rt in rel_topics:
            if rt.tid is not None and rt.ssid is not None:
                topic_tids_by_trend_ssid[rt.ssid].append(rt.tid)

        for tr in pred_trends:
            ssid = tr.ssid or 0
            driver_count = len(
                {
                    did
                    for tid in topic_tids_by_trend_ssid.get(ssid, [])
                    for did in dids_by_topic_tid.get(tid, [])
                }
            )
            trend_schema_by_trend_id[tr.trend_id].append(
                _trend_to_schema(tr, trend_ctx, rel_topics_by_ssid, driver_count=driver_count)
            )
        return trend_schema_by_trend_id

    def _assemble_foresight(
        self,
        tenant_schema: str,
        sow: TenantSow,
        insights: List[Insight],
        total: int,
        page: int,
        limit: int,
    ) -> ForesightResponse:
        sow_sid = sow.sid or 0
        offset = (page - 1) * limit
        has_prev = page > 1
        has_next = (offset + len(insights)) < total

        if not insights:
            return ForesightResponse(
                total=total, limit=limit, hasNext=has_next, hasPrev=has_prev, weeklyInsights=[]
            )

        insight_ids = [i.id for i in insights if i.id is not None]
        raw_sources = self.sow_repository.get_insight_sources_for_insight_ids(
            tenant_schema, insight_ids
        )
        sources_by_insight: Dict[int, List[InsightSource]] = defaultdict(list)
        for isrc, insight_id in raw_sources:
            sources_by_insight[insight_id].append(isrc)

        topic_entity_ids = list({i.entity_id for i in insights if i.entity_type == "topic"})
        trend_entity_ids = list({i.entity_id for i in insights if i.entity_type == "trend"})

        topic_schema_by_topic_id = (
            self._fetch_topic_predictions(tenant_schema, sow_sid, topic_entity_ids)
            if topic_entity_ids
            else {}
        )
        trend_schema_by_trend_id = (
            self._fetch_trend_predictions(tenant_schema, sow_sid, trend_entity_ids)
            if trend_entity_ids
            else {}
        )

        weekly_insights: List[InsightSchema] = []
        for insight in insights:
            iid = insight.id or 0
            predictions: List[Any] = (
                topic_schema_by_topic_id.get(insight.entity_id, [])
                if insight.entity_type == "topic"
                else trend_schema_by_trend_id.get(insight.entity_id, [])
            )
            weekly_insights.append(
                InsightSchema(
                    insight_title=insight.insight_title,
                    insight_description=insight.insight_description,
                    created_at=insight.created_at,
                    sources=[
                        InsightSourceSchema(
                            source_url=s.source_url,
                            source_title=s.source_title,
                            source_favicon=s.source_favicon or "",
                        )
                        for s in sources_by_insight.get(iid, [])
                    ],
                    predictions=predictions,
                )
            )

        return ForesightResponse(
            total=total,
            limit=limit,
            hasNext=has_next,
            hasPrev=has_prev,
            weeklyInsights=weekly_insights,
        )


__all__ = ["ForesightService"]
