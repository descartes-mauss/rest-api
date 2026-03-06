"""Service layer for the sows endpoints."""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException

from database.public_models.models import Geography, PublicSow
from database.schemas.driver import DriverSchema
from database.schemas.insight import ForesightResponse, InsightSchema, InsightSourceSchema
from database.schemas.opportunity import OpportunitySchema
from database.schemas.shift import ShiftSchema
from database.schemas.sow import DEFAULT_GEOGRAPHY_ID, DEFAULT_GEOGRAPHY_NAME, SowSchema
from database.schemas.topic import TopicSchema
from database.schemas.trend import TrendSchema
from database.tenant_models.models import (
    Insight,
    InsightSource,
    TenantSow,
    Topic,
)
from repositories.sow_repository import SowRepository
from services._maturity_helpers import (
    _assemble_topic_schema,
    _assemble_trend_schema,
    _build_sources_map,
    _split_topic_deltas,
    _split_topic_scores,
    _split_trend_deltas,
    _split_trend_scores,
    _trend_sort_key,
)

_WEEKLY_INSIGHTS_MAX_SIZE = 8


# ------------------------------------------------------------------
# SOW / geography helpers
# ------------------------------------------------------------------


def _assemble_sow_schema(
    sow: TenantSow,
    geo_map: Dict[str, Tuple[Optional[str], Optional[str]]],
) -> SowSchema:
    geo = geo_map.get(sow.cs_sow_id) if sow.cs_sow_id else None
    return SowSchema(
        id=sow.sid or 0,
        name=sow.sow_name,
        load_date=sow.load_date,
        geography_id=geo[0] if geo and geo[0] else DEFAULT_GEOGRAPHY_ID,
        geography_name=geo[1] if geo and geo[1] else DEFAULT_GEOGRAPHY_NAME,
    )


def _build_geo_map(
    rows: List[Tuple[PublicSow, Optional[Geography]]],
) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    geo_map: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for pub_sow, geo in rows:
        if pub_sow.sow_id and pub_sow.sow_id not in geo_map:
            geo_map[pub_sow.sow_id] = (
                pub_sow.geography_id,
                geo.name if geo else None,
            )
    return geo_map


class SowService:
    """Orchestrates data fetching and assembles SOW responses."""

    def __init__(self, repo: SowRepository) -> None:
        self.repo = repo

    def _get_sow_or_404(self, tenant_schema: str, sow_id: int) -> TenantSow:
        """Return the TenantSow for sow_id, or raise HTTP 404."""
        sow = self.repo.get_sow_by_id(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SOW not available")
        return sow

    # ------------------------------------------------------------------
    # Core SOW endpoints
    # ------------------------------------------------------------------

    def get_sows(self, tenant_schema: str) -> List[SowSchema]:
        sows = self.repo.get_latest_live_sows(tenant_schema)
        if not sows:
            return []

        cs_sow_ids = [s.cs_sow_id for s in sows if s.cs_sow_id]
        rows = self.repo.get_sow_geographies(cs_sow_ids) if cs_sow_ids else []
        geo_map = _build_geo_map(rows)

        return [_assemble_sow_schema(sow, geo_map) for sow in sows]

    def get_sow(self, tenant_schema: str, sow_id: int) -> SowSchema:
        sow = self._get_sow_or_404(tenant_schema, sow_id)

        if not sow.cs_sow_id:
            raise HTTPException(status_code=404, detail="SOW not available")

        latest = self.repo.get_latest_live_sow_for_cs_sow_id(tenant_schema, sow.cs_sow_id)
        if latest is None or latest.sid != sow_id:
            raise HTTPException(status_code=404, detail="SOW not available")

        rows = self.repo.get_sow_geographies([sow.cs_sow_id])
        geo_map = _build_geo_map(rows)

        return _assemble_sow_schema(sow, geo_map)

    # ------------------------------------------------------------------
    # SOW sub-endpoints
    # ------------------------------------------------------------------

    def get_shifts(self, tenant_schema: str, sow_id: int) -> List[ShiftSchema]:
        sow = self._get_sow_or_404(tenant_schema, sow_id)
        trends = self.repo.get_trends_for_sow(tenant_schema, sow.sid or sow_id)
        if not trends:
            return []

        trend_ssids = [t.ssid for t in trends if t.ssid is not None]
        trend_id_strings = [t.trend_id for t in trends]

        all_scores = self.repo.get_maturity_scores_for_trend_ids(tenant_schema, trend_ssids)
        score_ids = [ms.id for ms in all_scores if ms.id is not None]
        sources = self.repo.get_maturity_score_sources(tenant_schema, score_ids)
        deltas = self.repo.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow.sid or sow_id, trend_id_strings
        )
        related_topics = self.repo.get_topics_for_trends(tenant_schema, trend_ssids)

        sources_by_score = _build_sources_map(sources)
        global_by_ssid, non_global_by_ssid = _split_trend_scores(all_scores)
        global_delta_by_id, non_global_deltas_by_id = _split_trend_deltas(deltas)

        rel_topics_by_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for topic in related_topics:
            if topic.ssid is not None:
                rel_topics_by_ssid[topic.ssid].append(topic)

        sorted_trends = sorted(trends, key=lambda t: _trend_sort_key(t, global_by_ssid))

        shifts: Dict[str, ShiftSchema] = {}
        for t in sorted_trends:
            trend_schema = _assemble_trend_schema(
                t,
                sources_by_score,
                global_by_ssid,
                non_global_by_ssid,
                global_delta_by_id,
                non_global_deltas_by_id,
                rel_topics_by_ssid,
            )
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
        drivers = self.repo.get_drivers_for_sow(
            tenant_schema, sow.sid or sow_id, name_order=order if sort == "name" else None
        )

        driver_dids = [d.did for d in drivers if d.did is not None]
        topic_counts = self.repo.get_topic_counts_for_drivers(tenant_schema, driver_dids)

        geo_rows = self.repo.get_sow_geographies([sow.cs_sow_id] if sow.cs_sow_id else [])
        geo_map = _build_geo_map(geo_rows)
        sow_schema = _assemble_sow_schema(sow, geo_map)

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

    def get_versions(self, tenant_schema: str, sow_id: int) -> List[SowSchema]:
        sow = self._get_sow_or_404(tenant_schema, sow_id)

        if not sow.cs_sow_id:
            return []

        versions = self.repo.get_sow_versions(tenant_schema, sow.cs_sow_id)
        cs_sow_ids = [v.cs_sow_id for v in versions if v.cs_sow_id]
        rows = self.repo.get_sow_geographies(list(set(cs_sow_ids))) if cs_sow_ids else []
        geo_map = _build_geo_map(rows)

        return [_assemble_sow_schema(v, geo_map) for v in versions]

    def get_opportunities(self, tenant_schema: str, sow_id: int) -> List[OpportunitySchema]:
        sow = self._get_sow_or_404(tenant_schema, sow_id)
        sow_sid = sow.sid or sow_id
        opportunities = self.repo.get_opportunities_for_sow(tenant_schema, sow_sid)
        if not opportunities:
            return []

        opp_oids = [o.oid for o in opportunities if o.oid is not None]

        t2o_rows = self.repo.get_topic2opportunity_rows(tenant_schema, opp_oids)
        tids_by_opp: Dict[int, List[int]] = defaultdict(list)
        for row in t2o_rows:
            tids_by_opp[row.oid].append(row.tid)

        all_topic_tids = list({tid for tids in tids_by_opp.values() for tid in tids})
        topics = self.repo.get_topics_by_ids(tenant_schema, all_topic_tids)
        topics_by_tid: Dict[int, Topic] = {t.tid: t for t in topics if t.tid is not None}
        topic_id_strings = [t.topic_id for t in topics]

        topic_scores = self.repo.get_maturity_scores_for_topic_ids(tenant_schema, all_topic_tids)
        topic_score_ids = [ms.id for ms in topic_scores if ms.id is not None]
        topic_sources = self.repo.get_maturity_score_sources(tenant_schema, topic_score_ids)
        topic_deltas = self.repo.get_maturity_score_deltas_for_sow_topic_ids(
            tenant_schema, sow_sid, topic_id_strings
        )
        t2d_rows = self.repo.get_topic_drivers_by_topic_ids(tenant_schema, all_topic_tids)

        trend_ssids = list({t.ssid for t in topics if t.ssid is not None})
        trends = self.repo.get_trends_by_ssids(tenant_schema, trend_ssids)
        trend_id_strings = [tr.trend_id for tr in trends]

        trend_scores = self.repo.get_maturity_scores_for_trend_ids(tenant_schema, trend_ssids)
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        trend_sources = self.repo.get_maturity_score_sources(tenant_schema, trend_score_ids)
        trend_deltas = self.repo.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, trend_id_strings
        )
        related_topics_for_trends = self.repo.get_topics_for_trends(tenant_schema, trend_ssids)

        topic_sources_by_score = _build_sources_map(topic_sources)
        topic_global_by_tid, topic_non_global_by_tid = _split_topic_scores(topic_scores)
        topic_global_delta_by_id, topic_non_global_deltas_by_id = _split_topic_deltas(topic_deltas)

        drivers_by_tid: Dict[int, List[int]] = defaultdict(list)
        for row in t2d_rows:  # type: ignore[assignment]
            drivers_by_tid[row.tid].append(row.did)  # type: ignore[attr-defined]

        trend_sources_by_score = _build_sources_map(trend_sources)
        trend_global_by_ssid, trend_non_global_by_ssid = _split_trend_scores(trend_scores)
        trend_global_delta_by_id, trend_non_global_deltas_by_id = _split_trend_deltas(trend_deltas)

        rel_topics_by_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for rt in related_topics_for_trends:
            if rt.ssid is not None:
                rel_topics_by_ssid[rt.ssid].append(rt)

        trend_schema_by_ssid: Dict[int, TrendSchema] = {
            tr.ssid: _assemble_trend_schema(
                tr,
                trend_sources_by_score,
                trend_global_by_ssid,
                trend_non_global_by_ssid,
                trend_global_delta_by_id,
                trend_non_global_deltas_by_id,
                rel_topics_by_ssid,
            )
            for tr in trends
            if tr.ssid is not None
        }

        result: List[OpportunitySchema] = []
        for opp in opportunities:
            opp_tids = tids_by_opp.get(opp.oid or 0, [])
            opp_topics = [topics_by_tid[tid] for tid in opp_tids if tid in topics_by_tid]
            topic_schemas = [
                _assemble_topic_schema(
                    t,
                    topic_sources_by_score,
                    topic_global_by_tid,
                    topic_non_global_by_tid,
                    topic_global_delta_by_id,
                    topic_non_global_deltas_by_id,
                    trend_schema_by_ssid,
                    drivers_by_tid,
                )
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
        topics = self.repo.get_topics_for_sow(
            tenant_schema, sow_sid, name_order=order if sort == "name" else None
        )
        if not topics:
            return []

        topic_tids = [t.tid for t in topics if t.tid is not None]
        topic_id_strings = [t.topic_id for t in topics]

        topic_scores = self.repo.get_maturity_scores_for_topic_ids(tenant_schema, topic_tids)
        topic_score_ids = [ms.id for ms in topic_scores if ms.id is not None]
        topic_sources = self.repo.get_maturity_score_sources(tenant_schema, topic_score_ids)
        topic_deltas = self.repo.get_maturity_score_deltas_for_sow_topic_ids(
            tenant_schema, sow_sid, topic_id_strings
        )
        t2d_rows = self.repo.get_topic_drivers_by_topic_ids(tenant_schema, topic_tids)

        trend_ssids = list({t.ssid for t in topics if t.ssid is not None})
        trends = self.repo.get_trends_by_ssids(tenant_schema, trend_ssids)
        trend_id_strings = [tr.trend_id for tr in trends]
        trend_scores = self.repo.get_maturity_scores_for_trend_ids(tenant_schema, trend_ssids)
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        trend_sources = self.repo.get_maturity_score_sources(tenant_schema, trend_score_ids)
        trend_deltas = self.repo.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, trend_id_strings
        )

        topic_sources_by_score = _build_sources_map(topic_sources)
        topic_global_by_tid, topic_non_global_by_tid = _split_topic_scores(topic_scores)
        topic_global_delta_by_id, topic_non_global_deltas_by_id = _split_topic_deltas(topic_deltas)

        drivers_by_tid: Dict[int, List[int]] = defaultdict(list)
        for row in t2d_rows:
            drivers_by_tid[row.tid].append(row.did)

        trend_sources_by_score = _build_sources_map(trend_sources)
        trend_global_by_ssid, trend_non_global_by_ssid = _split_trend_scores(trend_scores)
        trend_global_delta_by_id, trend_non_global_deltas_by_id = _split_trend_deltas(trend_deltas)

        # Reuse already-loaded topics for each trend's related_topics (no extra DB call)
        topics_by_trend_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for t in topics:
            if t.ssid is not None:
                topics_by_trend_ssid[t.ssid].append(t)

        trend_schema_by_ssid: Dict[int, TrendSchema] = {
            tr.ssid: _assemble_trend_schema(
                tr,
                trend_sources_by_score,
                trend_global_by_ssid,
                trend_non_global_by_ssid,
                trend_global_delta_by_id,
                trend_non_global_deltas_by_id,
                topics_by_trend_ssid,
            )
            for tr in trends
            if tr.ssid is not None
        }

        result: List[TopicSchema] = []
        for topic in topics:
            tid = topic.tid or 0
            g_ms = topic_global_by_tid.get(tid)

            if maturity_level == "New":
                if not topic.new_discovery:
                    continue
            elif maturity_level != "All":
                if (g_ms.threshold if g_ms else None) != maturity_level:
                    continue

            result.append(
                _assemble_topic_schema(
                    topic,
                    topic_sources_by_score,
                    topic_global_by_tid,
                    topic_non_global_by_tid,
                    topic_global_delta_by_id,
                    topic_non_global_deltas_by_id,
                    trend_schema_by_ssid,
                    drivers_by_tid,
                )
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
        trends = self.repo.get_trends_for_sow(
            tenant_schema, sow_sid, name_order=order if sort == "name" else None
        )
        if not trends:
            return []

        trend_ssids = [t.ssid for t in trends if t.ssid is not None]
        trend_id_strings = [t.trend_id for t in trends]

        trend_scores = self.repo.get_maturity_scores_for_trend_ids(tenant_schema, trend_ssids)
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        trend_sources = self.repo.get_maturity_score_sources(tenant_schema, trend_score_ids)
        trend_deltas = self.repo.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, trend_id_strings
        )
        related_topics = self.repo.get_topics_for_trends(tenant_schema, trend_ssids)

        all_related_tids = [t.tid for t in related_topics if t.tid is not None]
        t2d_rows = self.repo.get_topic_drivers_by_topic_ids(tenant_schema, all_related_tids)

        sources_by_score = _build_sources_map(trend_sources)
        global_by_ssid, non_global_by_ssid = _split_trend_scores(trend_scores)
        global_delta_by_id, non_global_deltas_by_id = _split_trend_deltas(trend_deltas)

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
            g_ms = global_by_ssid.get(ssid)

            if maturity_level == "New":
                if not trend.new_discovery:
                    continue
            elif maturity_level != "All":
                if (g_ms.threshold if g_ms else None) != maturity_level:
                    continue

            driver_count = len(
                {
                    did
                    for tid in topic_tids_by_trend_ssid.get(ssid, [])
                    for did in dids_by_topic_tid.get(tid, [])
                }
            )
            result.append(
                _assemble_trend_schema(
                    trend,
                    sources_by_score,
                    global_by_ssid,
                    non_global_by_ssid,
                    global_delta_by_id,
                    non_global_deltas_by_id,
                    rel_topics_by_ssid,
                    driver_count=driver_count,
                )
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

    # ------------------------------------------------------------------
    # Foresight endpoints
    # ------------------------------------------------------------------

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
        total, insights = self.repo.get_insights_for_cs_sow_id(
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
            total, insights = self.repo.get_insights_filtered(
                tenant_schema, sow.cs_sow_id, entity_ids=entity_ids, offset=offset, limit=limit
            )
        else:
            total, insights = self.repo.get_insights_for_cs_sow_id(
                tenant_schema, sow.cs_sow_id, offset=offset, limit=limit
            )
        return self._assemble_foresight(tenant_schema, sow, insights, total, page, limit)

    def _build_topic_predictions(
        self, tenant_schema: str, sow_sid: int, entity_ids: List[str]
    ) -> Dict[str, List[TopicSchema]]:
        topic_schema_by_topic_id: Dict[str, List[TopicSchema]] = defaultdict(list)
        pred_topics = self.repo.get_topics_by_topic_str_ids(tenant_schema, sow_sid, entity_ids)
        if not pred_topics:
            return topic_schema_by_topic_id

        topic_tids = [t.tid for t in pred_topics if t.tid is not None]
        topic_id_strings = [t.topic_id for t in pred_topics]
        topic_scores = self.repo.get_maturity_scores_for_topic_ids(tenant_schema, topic_tids)
        topic_score_ids = [ms.id for ms in topic_scores if ms.id is not None]
        t_sources = self.repo.get_maturity_score_sources(tenant_schema, topic_score_ids)
        topic_deltas = self.repo.get_maturity_score_deltas_for_sow_topic_ids(
            tenant_schema, sow_sid, topic_id_strings
        )
        t2d_rows = self.repo.get_topic_drivers_by_topic_ids(tenant_schema, topic_tids)

        topic_trend_ssids = list({t.ssid for t in pred_topics if t.ssid is not None})
        topic_trends = self.repo.get_trends_by_ssids(tenant_schema, topic_trend_ssids)
        topic_trend_id_strings = [tr.trend_id for tr in topic_trends]
        topic_trend_scores = self.repo.get_maturity_scores_for_trend_ids(
            tenant_schema, topic_trend_ssids
        )
        topic_trend_score_ids = [ms.id for ms in topic_trend_scores if ms.id is not None]
        topic_trend_sources = self.repo.get_maturity_score_sources(
            tenant_schema, topic_trend_score_ids
        )
        topic_trend_deltas = self.repo.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, topic_trend_id_strings
        )

        t_sources_by_score = _build_sources_map(t_sources)
        t_global_by_tid, t_non_global_by_tid = _split_topic_scores(topic_scores)
        t_global_delta_by_id, t_non_global_deltas_by_id = _split_topic_deltas(topic_deltas)

        drivers_by_tid: Dict[int, List[int]] = defaultdict(list)
        for row in t2d_rows:
            drivers_by_tid[row.tid].append(row.did)

        tt_sources_by_score = _build_sources_map(topic_trend_sources)
        tt_global_by_ssid, tt_non_global_by_ssid = _split_trend_scores(topic_trend_scores)
        tt_global_delta_by_id, tt_non_global_deltas_by_id = _split_trend_deltas(topic_trend_deltas)

        topics_by_trend_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for t in pred_topics:
            if t.ssid is not None:
                topics_by_trend_ssid[t.ssid].append(t)

        trend_schema_by_ssid: Dict[int, TrendSchema] = {
            tr.ssid: _assemble_trend_schema(
                tr,
                tt_sources_by_score,
                tt_global_by_ssid,
                tt_non_global_by_ssid,
                tt_global_delta_by_id,
                tt_non_global_deltas_by_id,
                topics_by_trend_ssid,
            )
            for tr in topic_trends
            if tr.ssid is not None
        }

        for t in pred_topics:
            topic_schema_by_topic_id[t.topic_id].append(
                _assemble_topic_schema(
                    t,
                    t_sources_by_score,
                    t_global_by_tid,
                    t_non_global_by_tid,
                    t_global_delta_by_id,
                    t_non_global_deltas_by_id,
                    trend_schema_by_ssid,
                    drivers_by_tid,
                )
            )
        return topic_schema_by_topic_id

    def _build_trend_predictions(
        self, tenant_schema: str, sow_sid: int, entity_ids: List[str]
    ) -> Dict[str, List[TrendSchema]]:
        trend_schema_by_trend_id: Dict[str, List[TrendSchema]] = defaultdict(list)
        pred_trends = self.repo.get_trends_by_trend_str_ids(tenant_schema, sow_sid, entity_ids)
        if not pred_trends:
            return trend_schema_by_trend_id

        trend_ssids = [t.ssid for t in pred_trends if t.ssid is not None]
        trend_id_strings = [t.trend_id for t in pred_trends]
        trend_scores = self.repo.get_maturity_scores_for_trend_ids(tenant_schema, trend_ssids)
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        tr_sources = self.repo.get_maturity_score_sources(tenant_schema, trend_score_ids)
        trend_deltas = self.repo.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, trend_id_strings
        )
        rel_topics = self.repo.get_topics_for_trends(tenant_schema, trend_ssids)
        rt_tids = [rt.tid for rt in rel_topics if rt.tid is not None]
        t2d_for_trends = self.repo.get_topic_drivers_by_topic_ids(tenant_schema, rt_tids)

        tr_sources_by_score = _build_sources_map(tr_sources)
        tr_global_by_ssid, tr_non_global_by_ssid = _split_trend_scores(trend_scores)
        tr_global_delta_by_id, tr_non_global_deltas_by_id = _split_trend_deltas(trend_deltas)

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
                _assemble_trend_schema(
                    tr,
                    tr_sources_by_score,
                    tr_global_by_ssid,
                    tr_non_global_by_ssid,
                    tr_global_delta_by_id,
                    tr_non_global_deltas_by_id,
                    rel_topics_by_ssid,
                    driver_count=driver_count,
                )
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
        raw_sources = self.repo.get_insight_sources_for_insight_ids(tenant_schema, insight_ids)
        sources_by_insight: Dict[int, List[InsightSource]] = defaultdict(list)
        for isrc, insight_id in raw_sources:
            sources_by_insight[insight_id].append(isrc)

        topic_entity_ids = list({i.entity_id for i in insights if i.entity_type == "topic"})
        trend_entity_ids = list({i.entity_id for i in insights if i.entity_type == "trend"})

        topic_schema_by_topic_id = (
            self._build_topic_predictions(tenant_schema, sow_sid, topic_entity_ids)
            if topic_entity_ids
            else {}
        )
        trend_schema_by_trend_id = (
            self._build_trend_predictions(tenant_schema, sow_sid, trend_entity_ids)
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
