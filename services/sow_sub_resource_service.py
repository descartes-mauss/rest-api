"""Service layer for SOW sub-resource endpoints (shifts, trends, topics, drivers, opportunities)."""

from collections import defaultdict
from typing import Dict, List, Optional

from fastapi import HTTPException

from database.schemas.driver import DriverSchema
from database.schemas.opportunity import OpportunitySchema
from database.schemas.shift import ShiftSchema
from database.schemas.topic import TopicSchema
from database.schemas.trend import TrendSchema
from database.tenant_models.models import TenantSow, Topic
from repositories.protocols import SowRepositoryProtocol
from services.assemblers.maturity_context import _build_topic_context, _build_trend_context
from services.assemblers.sow_assembler import _build_geo_map, _sow_to_schema
from services.assemblers.topic_assembler import _topic_to_schema
from services.assemblers.trend_assembler import (
    _passes_maturity_filter,
    _trend_sort_key,
    _trend_to_schema,
)


class SowSubResourceService:
    """Aggregates sub-resources (shifts, trends, topics, drivers, opportunities) for a SOW."""

    def __init__(self, sow_repository: SowRepositoryProtocol) -> None:
        self.sow_repository = sow_repository

    def _get_sow_or_404(self, tenant_schema: str, sow_id: int) -> TenantSow:
        sow = self.sow_repository.get_sow_by_id(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SOW not available")
        return sow

    def get_shifts(self, tenant_schema: str, sow_id: int) -> List[ShiftSchema]:
        sow = self._get_sow_or_404(tenant_schema, sow_id)
        trends = self.sow_repository.get_trends_for_sow(tenant_schema, sow.sid or sow_id)
        if not trends:
            return []

        trend_ssids = [t.ssid for t in trends if t.ssid is not None]
        trend_id_strings = [t.trend_id for t in trends]

        all_scores = self.sow_repository.get_maturity_scores_for_trend_ids(
            tenant_schema, trend_ssids
        )
        score_ids = [ms.id for ms in all_scores if ms.id is not None]
        sources = self.sow_repository.get_maturity_score_sources(tenant_schema, score_ids)
        deltas = self.sow_repository.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow.sid or sow_id, trend_id_strings
        )
        related_topics = self.sow_repository.get_topics_for_trends(tenant_schema, trend_ssids)

        trend_ctx = _build_trend_context(all_scores, sources, deltas)

        rel_topics_by_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for topic in related_topics:
            if topic.ssid is not None:
                rel_topics_by_ssid[topic.ssid].append(topic)

        sorted_trends = sorted(
            trends, key=lambda t: _trend_sort_key(t, trend_ctx.global_scores_by_id)
        )

        shifts: Dict[str, ShiftSchema] = {}
        for t in sorted_trends:
            trend_schema = _trend_to_schema(t, trend_ctx, rel_topics_by_ssid)
            if t.shift_id not in shifts:
                shifts[t.shift_id] = ShiftSchema(
                    id=t.shift_id,
                    name=t.shift_name,
                    description=t.shift_description,
                )
            shifts[t.shift_id].trends.append(trend_schema)

        return list(shifts.values())

    def get_drivers(
        self,
        tenant_schema: str,
        sow_id: int,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> List[DriverSchema]:
        sow = self._get_sow_or_404(tenant_schema, sow_id)
        drivers = self.sow_repository.get_drivers_for_sow(
            tenant_schema, sow.sid or sow_id, name_order=order if sort == "name" else None
        )

        driver_dids = [d.did for d in drivers if d.did is not None]
        topic_counts = self.sow_repository.get_topic_counts_for_drivers(tenant_schema, driver_dids)

        geo_rows = self.sow_repository.get_sow_geographies([sow.cs_sow_id] if sow.cs_sow_id else [])
        geo_map = _build_geo_map(geo_rows)
        sow_schema = _sow_to_schema(sow, geo_map)

        return [
            DriverSchema(
                did=d.did,
                sow=sow_schema,
                load_date=d.load_date,
                driver_id=d.driver_id,
                driver_name=d.driver_name,
                driver_description=d.driver_description,
                masterfile_version=d.masterfile_version,
                for_deletion=d.for_deletion,
                topic_count=topic_counts.get(d.did or 0, 0),
            )
            for d in drivers
        ]

    def get_opportunities(self, tenant_schema: str, sow_id: int) -> List[OpportunitySchema]:
        sow = self._get_sow_or_404(tenant_schema, sow_id)
        sow_sid = sow.sid or sow_id
        opportunities = self.sow_repository.get_opportunities_for_sow(tenant_schema, sow_sid)
        if not opportunities:
            return []

        opp_oids = [o.oid for o in opportunities if o.oid is not None]

        t2o_rows = self.sow_repository.get_topic2opportunity_rows(tenant_schema, opp_oids)
        tids_by_opp: Dict[int, List[int]] = defaultdict(list)
        for row in t2o_rows:
            tids_by_opp[row.oid].append(row.tid)

        all_topic_tids = list({tid for tids in tids_by_opp.values() for tid in tids})
        topics = self.sow_repository.get_topics_by_ids(tenant_schema, all_topic_tids)
        topics_by_tid: Dict[int, Topic] = {t.tid: t for t in topics if t.tid is not None}
        topic_id_strings = [t.topic_id for t in topics]

        topic_scores = self.sow_repository.get_maturity_scores_for_topic_ids(
            tenant_schema, all_topic_tids
        )
        topic_score_ids = [ms.id for ms in topic_scores if ms.id is not None]
        topic_sources = self.sow_repository.get_maturity_score_sources(
            tenant_schema, topic_score_ids
        )
        topic_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_topic_ids(
            tenant_schema, sow_sid, topic_id_strings
        )
        t2d_rows = self.sow_repository.get_topic_drivers_by_topic_ids(tenant_schema, all_topic_tids)

        trend_ssids = list({t.ssid for t in topics if t.ssid is not None})
        trends = self.sow_repository.get_trends_by_ssids(tenant_schema, trend_ssids)
        trend_id_strings = [tr.trend_id for tr in trends]

        trend_scores = self.sow_repository.get_maturity_scores_for_trend_ids(
            tenant_schema, trend_ssids
        )
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        trend_sources = self.sow_repository.get_maturity_score_sources(
            tenant_schema, trend_score_ids
        )
        trend_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, trend_id_strings
        )
        related_topics_for_trends = self.sow_repository.get_topics_for_trends(
            tenant_schema, trend_ssids
        )

        topic_ctx = _build_topic_context(topic_scores, topic_sources, topic_deltas)

        drivers_by_tid: Dict[int, List[int]] = defaultdict(list)
        for row in t2d_rows:  # type: ignore[assignment]
            drivers_by_tid[row.tid].append(row.did)  # type: ignore[attr-defined]

        trend_ctx = _build_trend_context(trend_scores, trend_sources, trend_deltas)

        rel_topics_by_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for rt in related_topics_for_trends:
            if rt.ssid is not None:
                rel_topics_by_ssid[rt.ssid].append(rt)

        trend_schema_by_ssid: Dict[int, TrendSchema] = {
            tr.ssid: _trend_to_schema(tr, trend_ctx, rel_topics_by_ssid)
            for tr in trends
            if tr.ssid is not None
        }

        result: List[OpportunitySchema] = []
        for opp in opportunities:
            opp_tids = tids_by_opp.get(opp.oid or 0, [])
            opp_topics = [topics_by_tid[tid] for tid in opp_tids if tid in topics_by_tid]
            topic_schemas = [
                _topic_to_schema(t, topic_ctx, trend_schema_by_ssid, drivers_by_tid)
                for t in opp_topics
            ]
            result.append(
                OpportunitySchema(
                    oid=opp.oid,
                    sow=opp.sid,
                    opportunity_name=opp.opportunity_name,
                    opportunity=opp.opportunity,
                    masterfile_version=opp.masterfile_version,
                    for_deletion=opp.for_deletion,
                    topics=topic_schemas,
                    topic_ids=[t.topic_id for t in opp_topics],
                    topic=[t.tid for t in opp_topics if t.tid is not None],
                )
            )

        return result

    def get_topics(
        self,
        tenant_schema: str,
        sow_id: int,
        maturity_level: str = "All",
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> List[TopicSchema]:
        sow = self._get_sow_or_404(tenant_schema, sow_id)
        sow_sid = sow.sid or sow_id
        topics = self.sow_repository.get_topics_for_sow(
            tenant_schema, sow_sid, name_order=order if sort == "name" else None
        )
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

        trend_ssids = list({t.ssid for t in topics if t.ssid is not None})
        trends = self.sow_repository.get_trends_by_ssids(tenant_schema, trend_ssids)
        trend_id_strings = [tr.trend_id for tr in trends]
        trend_scores = self.sow_repository.get_maturity_scores_for_trend_ids(
            tenant_schema, trend_ssids
        )
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        trend_sources = self.sow_repository.get_maturity_score_sources(
            tenant_schema, trend_score_ids
        )
        trend_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, trend_id_strings
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

        trend_schema_by_ssid: Dict[int, TrendSchema] = {
            tr.ssid: _trend_to_schema(tr, trend_ctx, topics_by_trend_ssid)
            for tr in trends
            if tr.ssid is not None
        }

        result: List[TopicSchema] = []
        for topic in topics:
            tid = topic.tid or 0
            g_ms = topic_ctx.global_scores_by_id.get(tid)

            if not _passes_maturity_filter(topic, g_ms, maturity_level):
                continue

            result.append(_topic_to_schema(topic, topic_ctx, trend_schema_by_ssid, drivers_by_tid))

        if sort == "maturity":
            none_sentinel = float("inf") if order != "desc" else float("-inf")
            result.sort(
                key=lambda t: (
                    float(t.global_maturity_score.score)
                    if t.global_maturity_score and t.global_maturity_score.score is not None
                    else none_sentinel
                ),
                reverse=(order == "desc"),
            )

        return result

    def get_trends(
        self,
        tenant_schema: str,
        sow_id: int,
        maturity_level: str = "All",
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> List[TrendSchema]:
        sow = self._get_sow_or_404(tenant_schema, sow_id)
        sow_sid = sow.sid or sow_id
        trends = self.sow_repository.get_trends_for_sow(
            tenant_schema, sow_sid, name_order=order if sort == "name" else None
        )
        if not trends:
            return []

        trend_ssids = [t.ssid for t in trends if t.ssid is not None]
        trend_id_strings = [t.trend_id for t in trends]

        trend_scores = self.sow_repository.get_maturity_scores_for_trend_ids(
            tenant_schema, trend_ssids
        )
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        trend_sources = self.sow_repository.get_maturity_score_sources(
            tenant_schema, trend_score_ids
        )
        trend_deltas = self.sow_repository.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, trend_id_strings
        )
        related_topics = self.sow_repository.get_topics_for_trends(tenant_schema, trend_ssids)

        all_related_tids = [t.tid for t in related_topics if t.tid is not None]
        t2d_rows = self.sow_repository.get_topic_drivers_by_topic_ids(
            tenant_schema, all_related_tids
        )

        trend_ctx = _build_trend_context(trend_scores, trend_sources, trend_deltas)

        rel_topics_by_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for rt in related_topics:
            if rt.ssid is not None:
                rel_topics_by_ssid[rt.ssid].append(rt)

        dids_by_topic_tid: Dict[int, List[int]] = defaultdict(list)
        for row in t2d_rows:
            dids_by_topic_tid[row.tid].append(row.did)

        topic_tids_by_trend_ssid: Dict[int, List[int]] = defaultdict(list)
        for rt in related_topics:
            if rt.tid is not None and rt.ssid is not None:
                topic_tids_by_trend_ssid[rt.ssid].append(rt.tid)

        result: List[TrendSchema] = []
        for trend in trends:
            ssid = trend.ssid or 0
            g_ms = trend_ctx.global_scores_by_id.get(ssid)

            if not _passes_maturity_filter(trend, g_ms, maturity_level):
                continue

            driver_count = len(
                {
                    did
                    for tid in topic_tids_by_trend_ssid.get(ssid, [])
                    for did in dids_by_topic_tid.get(tid, [])
                }
            )
            result.append(
                _trend_to_schema(trend, trend_ctx, rel_topics_by_ssid, driver_count=driver_count)
            )

        if sort == "maturity":
            none_sentinel = float("inf") if order != "desc" else float("-inf")
            result.sort(
                key=lambda t: (
                    float(t.global_maturity_score.score)
                    if t.global_maturity_score and t.global_maturity_score.score is not None
                    else none_sentinel
                ),
                reverse=(order == "desc"),
            )

        return result


__all__ = ["SowSubResourceService"]
