"""Service layer for the sows endpoints."""

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException

from database.public_models.models import Geography, PublicSow
from database.schemas.driver import DriverSchema
from database.schemas.maturity import (
    MaturityScoreDeltaSchema,
    MaturityScoreSchema,
    MaturityScoreSourceSchema,
)
from database.schemas.opportunity import OpportunitySchema, TopicInOpportunitySchema
from database.schemas.shift import ShiftSchema
from database.schemas.sow import DEFAULT_GEOGRAPHY_ID, DEFAULT_GEOGRAPHY_NAME, SowSchema
from database.schemas.topic import UnlinkedTopicSchema
from database.schemas.trend import TrendSchema
from database.tenant_models.models import (
    MaturityScore,
    MaturityScoreDelta,
    MaturityScoreSource,
    TenantSow,
    Topic,
    Trend,
)
from repositories.sow_repository import SowRepository

_MISSING_TENANT = "Authorization token missing tenant schema information."


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


def _maturity_score_schema(
    ms: MaturityScore,
    sources: List[MaturityScoreSource],
) -> MaturityScoreSchema:
    return MaturityScoreSchema(
        id=ms.id,
        category=str(ms.category),
        score=float(ms.score) if ms.score is not None else None,
        threshold=ms.threshold,
        rationale=ms.rationale,
        sources=[MaturityScoreSourceSchema.model_validate(s) for s in sources],
    )


def _trend_sort_key(trend: Trend, global_score_by_ssid: Dict[int, MaturityScore]) -> Tuple:  # type: ignore[type-arg]
    """Sort key mirroring Django's order_by('-global_maturity_score', 'shift_id', 'trend_name')."""
    ms = global_score_by_ssid.get(trend.ssid or -1)
    score = float(ms.score) if ms and ms.score is not None else None
    # None scores sort last: (True, ...) > (False, ...)
    return (score is None, -(score or 0.0), trend.shift_id or "", trend.trend_name or "")


class SowService:
    """Orchestrates data fetching and assembles SOW responses."""

    def __init__(self, repo: SowRepository) -> None:
        self.repo = repo

    # ------------------------------------------------------------------
    # Core SOW endpoints
    # ------------------------------------------------------------------

    def get_sows(self, tenant_schema: Optional[str]) -> List[SowSchema]:
        if not tenant_schema:
            raise HTTPException(status_code=400, detail=_MISSING_TENANT)

        sows = self.repo.get_latest_live_sows(tenant_schema)
        if not sows:
            return []

        cs_sow_ids = [s.cs_sow_id for s in sows if s.cs_sow_id]
        rows = self.repo.get_sow_geographies(cs_sow_ids) if cs_sow_ids else []
        geo_map = _build_geo_map(rows)

        return [_assemble_sow_schema(sow, geo_map) for sow in sows]

    def get_sow(self, tenant_schema: Optional[str], sow_id: int) -> SowSchema:
        if not tenant_schema:
            raise HTTPException(status_code=400, detail=_MISSING_TENANT)

        sow = self.repo.get_sow_by_id(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SOW not available")

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

    def get_shifts(self, tenant_schema: Optional[str], sow_id: int) -> List[ShiftSchema]:
        if not tenant_schema:
            raise HTTPException(status_code=400, detail=_MISSING_TENANT)

        sow = self.repo.get_sow_by_id(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SOW not available")

        trends = self.repo.get_trends_for_sow(tenant_schema, sow.sid or sow_id)
        if not trends:
            return []

        trend_ssids = [t.ssid for t in trends if t.ssid is not None]
        trend_id_strings = [t.trend_id for t in trends]

        # Batch-fetch all related data
        all_scores = self.repo.get_maturity_scores_for_trend_ids(tenant_schema, trend_ssids)
        score_ids = [ms.id for ms in all_scores if ms.id is not None]
        sources = self.repo.get_maturity_score_sources(tenant_schema, score_ids)
        deltas = self.repo.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow.sid or sow_id, trend_id_strings
        )
        related_topics = self.repo.get_topics_for_trends(tenant_schema, trend_ssids)

        # Build lookup maps
        sources_by_score_id: Dict[int, List[MaturityScoreSource]] = defaultdict(list)
        for src in sources:
            sources_by_score_id[src.maturity_score_id].append(src)

        non_global_by_trend: Dict[int, List[MaturityScore]] = defaultdict(list)
        global_by_trend: Dict[int, MaturityScore] = {}
        for ms in all_scores:
            if ms.trend_id is None:
                continue
            if str(ms.category) == "global":
                global_by_trend.setdefault(ms.trend_id, ms)
            else:
                non_global_by_trend[ms.trend_id].append(ms)

        non_global_deltas_by_trend: Dict[str, List[MaturityScoreDelta]] = defaultdict(list)
        global_delta_by_trend: Dict[str, MaturityScoreDelta] = {}
        for delta in deltas:
            if delta.trend_id is None:
                continue
            if str(delta.category) == "global":
                global_delta_by_trend.setdefault(delta.trend_id, delta)
            else:
                non_global_deltas_by_trend[delta.trend_id].append(delta)

        topics_by_trend_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for topic in related_topics:
            if topic.ssid is not None:
                topics_by_trend_ssid[topic.ssid].append(topic)

        # Sort trends: descending global_maturity_score, then shift_id, then trend_name
        sorted_trends = sorted(trends, key=lambda t: _trend_sort_key(t, global_by_trend))

        # Assemble trend schemas and group into shifts preserving order
        shifts: Dict[str, ShiftSchema] = {}
        for t in sorted_trends:
            ssid = t.ssid or 0
            trend_id = t.trend_id

            ms_schemas = [
                _maturity_score_schema(ms, sources_by_score_id.get(ms.id or 0, []))
                for ms in sorted(non_global_by_trend.get(ssid, []), key=lambda x: str(x.category))
            ]
            global_ms = global_by_trend.get(ssid)
            global_ms_schema = (
                _maturity_score_schema(global_ms, sources_by_score_id.get(global_ms.id or 0, []))
                if global_ms
                else None
            )
            delta_schemas = [
                MaturityScoreDeltaSchema.model_validate(d)
                for d in sorted(
                    non_global_deltas_by_trend.get(trend_id, []),
                    key=lambda x: str(x.category),
                )
            ]
            global_delta = global_delta_by_trend.get(trend_id)
            global_delta_schema = (
                MaturityScoreDeltaSchema.model_validate(global_delta) if global_delta else None
            )

            trend_schema = TrendSchema(
                ssid=t.ssid,
                sow=t.sid,
                load_date=t.load_date,
                trend_id=t.trend_id,
                trend_name=t.trend_name,
                trend_description=t.trend_description,
                shift_id=t.shift_id,
                shift_name=t.shift_name,
                shift_description=t.shift_description,
                trend_image_s3_uri=t.trend_image_s3_uri,
                masterfile_version=t.masterfile_version,
                for_deletion=t.for_deletion,
                new_discovery=t.new_discovery,
                maturity_scores=ms_schemas,
                maturity_scores_deltas=delta_schemas,
                global_maturity_score=global_ms_schema,
                global_maturity_score_delta=global_delta_schema,
                related_topics=[
                    UnlinkedTopicSchema.model_validate(tp)
                    for tp in topics_by_trend_ssid.get(ssid, [])
                ],
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
        tenant_schema: Optional[str],
        sow_id: int,
        sort: Optional[str] = None,
        order: Optional[str] = None,
    ) -> List[DriverSchema]:
        if not tenant_schema:
            raise HTTPException(status_code=400, detail=_MISSING_TENANT)

        sow = self.repo.get_sow_by_id(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SOW not available")

        drivers = self.repo.get_drivers_for_sow(tenant_schema, sow.sid or sow_id)

        driver_dids = [d.did for d in drivers if d.did is not None]
        topic_counts = self.repo.get_topic_counts_for_drivers(tenant_schema, driver_dids)

        geo_rows = self.repo.get_sow_geographies([sow.cs_sow_id] if sow.cs_sow_id else [])
        geo_map = _build_geo_map(geo_rows)
        sow_schema = _assemble_sow_schema(sow, geo_map)

        if sort == "name":
            drivers = sorted(drivers, key=lambda d: d.driver_name, reverse=(order == "desc"))

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

    def get_versions(self, tenant_schema: Optional[str], sow_id: int) -> List[SowSchema]:
        if not tenant_schema:
            raise HTTPException(status_code=400, detail=_MISSING_TENANT)

        sow = self.repo.get_sow_by_id(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SOW not available")

        if not sow.cs_sow_id:
            return []

        versions = self.repo.get_sow_versions(tenant_schema, sow.cs_sow_id)
        cs_sow_ids = [v.cs_sow_id for v in versions if v.cs_sow_id]
        rows = self.repo.get_sow_geographies(list(set(cs_sow_ids))) if cs_sow_ids else []
        geo_map = _build_geo_map(rows)

        return [_assemble_sow_schema(v, geo_map) for v in versions]

    def get_opportunities(
        self, tenant_schema: Optional[str], sow_id: int
    ) -> List[OpportunitySchema]:
        if not tenant_schema:
            raise HTTPException(status_code=400, detail=_MISSING_TENANT)

        sow = self.repo.get_sow_by_id(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SOW not available")

        sow_sid = sow.sid or sow_id
        opportunities = self.repo.get_opportunities_for_sow(tenant_schema, sow_sid)
        if not opportunities:
            return []

        opp_oids = [o.oid for o in opportunities if o.oid is not None]

        # Topic M2M join rows → {oid: [tid, ...]}
        t2o_rows = self.repo.get_topic2opportunity_rows(tenant_schema, opp_oids)
        tids_by_opp: Dict[int, List[int]] = defaultdict(list)
        for row in t2o_rows:
            tids_by_opp[row.oid].append(row.tid)

        all_topic_tids = list({tid for tids in tids_by_opp.values() for tid in tids})
        topics = self.repo.get_topics_by_ids(tenant_schema, all_topic_tids)
        topics_by_tid: Dict[int, Topic] = {t.tid: t for t in topics if t.tid is not None}
        topic_id_strings = [t.topic_id for t in topics]

        # Batch-fetch all topic-level related data
        topic_scores = self.repo.get_maturity_scores_for_topic_ids(tenant_schema, all_topic_tids)
        topic_score_ids = [ms.id for ms in topic_scores if ms.id is not None]
        topic_sources = self.repo.get_maturity_score_sources(tenant_schema, topic_score_ids)
        topic_deltas = self.repo.get_maturity_score_deltas_for_sow_topic_ids(
            tenant_schema, sow_sid, topic_id_strings
        )
        t2d_rows = self.repo.get_topic_drivers_by_topic_ids(tenant_schema, all_topic_tids)

        # Trends referenced by topics
        trend_ssids = list({t.ssid for t in topics if t.ssid is not None})
        trends = self.repo.get_trends_by_ssids(tenant_schema, trend_ssids)
        trend_id_strings = [tr.trend_id for tr in trends]

        # Batch-fetch all trend-level related data (for the trend nested in each topic)
        trend_scores = self.repo.get_maturity_scores_for_trend_ids(tenant_schema, trend_ssids)
        trend_score_ids = [ms.id for ms in trend_scores if ms.id is not None]
        trend_sources = self.repo.get_maturity_score_sources(tenant_schema, trend_score_ids)
        trend_deltas = self.repo.get_maturity_score_deltas_for_sow_trends(
            tenant_schema, sow_sid, trend_id_strings
        )
        related_topics_for_trends = self.repo.get_topics_for_trends(tenant_schema, trend_ssids)

        # ---- Build lookup maps ----

        topic_sources_by_score: Dict[int, List[MaturityScoreSource]] = defaultdict(list)
        for src in topic_sources:
            topic_sources_by_score[src.maturity_score_id].append(src)

        topic_non_global_by_tid: Dict[int, List[MaturityScore]] = defaultdict(list)
        topic_global_by_tid: Dict[int, MaturityScore] = {}
        for ms in topic_scores:
            if ms.topic_id is None:
                continue
            if str(ms.category) == "global":
                topic_global_by_tid.setdefault(ms.topic_id, ms)
            else:
                topic_non_global_by_tid[ms.topic_id].append(ms)

        topic_non_global_deltas_by_id: Dict[str, List[MaturityScoreDelta]] = defaultdict(list)
        topic_global_delta_by_id: Dict[str, MaturityScoreDelta] = {}
        for delta in topic_deltas:
            if delta.topic_id is None:
                continue
            if str(delta.category) == "global":
                topic_global_delta_by_id.setdefault(delta.topic_id, delta)
            else:
                topic_non_global_deltas_by_id[delta.topic_id].append(delta)

        drivers_by_topic_tid: Dict[int, List[int]] = defaultdict(list)
        for row in t2d_rows:  # type: ignore[assignment]
            drivers_by_topic_tid[row.tid].append(row.did)  # type: ignore[attr-defined]

        trend_sources_by_score: Dict[int, List[MaturityScoreSource]] = defaultdict(list)
        for src in trend_sources:
            trend_sources_by_score[src.maturity_score_id].append(src)

        trend_non_global_by_ssid: Dict[int, List[MaturityScore]] = defaultdict(list)
        trend_global_by_ssid: Dict[int, MaturityScore] = {}
        for ms in trend_scores:
            if ms.trend_id is None:
                continue
            if str(ms.category) == "global":
                trend_global_by_ssid.setdefault(ms.trend_id, ms)
            else:
                trend_non_global_by_ssid[ms.trend_id].append(ms)

        trend_non_global_deltas: Dict[str, List[MaturityScoreDelta]] = defaultdict(list)
        trend_global_delta: Dict[str, MaturityScoreDelta] = {}
        for delta in trend_deltas:
            if delta.trend_id is None:
                continue
            if str(delta.category) == "global":
                trend_global_delta.setdefault(delta.trend_id, delta)
            else:
                trend_non_global_deltas[delta.trend_id].append(delta)

        rel_topics_by_trend_ssid: Dict[int, List[Topic]] = defaultdict(list)
        for rt in related_topics_for_trends:
            if rt.ssid is not None:
                rel_topics_by_trend_ssid[rt.ssid].append(rt)

        # ---- Assemble trend schemas ----

        def _build_trend(trend: Trend) -> TrendSchema:
            ssid = trend.ssid or 0
            ms_list = [
                _maturity_score_schema(ms, trend_sources_by_score.get(ms.id or 0, []))
                for ms in sorted(
                    trend_non_global_by_ssid.get(ssid, []), key=lambda x: str(x.category)
                )
            ]
            g_ms = trend_global_by_ssid.get(ssid)
            g_delta = trend_global_delta.get(trend.trend_id)
            return TrendSchema(
                ssid=trend.ssid,
                sow=trend.sid,
                load_date=trend.load_date,
                trend_id=trend.trend_id,
                trend_name=trend.trend_name,
                trend_description=trend.trend_description,
                shift_id=trend.shift_id,
                shift_name=trend.shift_name,
                shift_description=trend.shift_description,
                trend_image_s3_uri=trend.trend_image_s3_uri,
                masterfile_version=trend.masterfile_version,
                for_deletion=trend.for_deletion,
                new_discovery=trend.new_discovery,
                maturity_scores=ms_list,
                maturity_scores_deltas=[
                    MaturityScoreDeltaSchema.model_validate(d)
                    for d in sorted(
                        trend_non_global_deltas.get(trend.trend_id, []),
                        key=lambda x: str(x.category),
                    )
                ],
                global_maturity_score=(
                    _maturity_score_schema(g_ms, trend_sources_by_score.get(g_ms.id or 0, []))
                    if g_ms
                    else None
                ),
                global_maturity_score_delta=(
                    MaturityScoreDeltaSchema.model_validate(g_delta) if g_delta else None
                ),
                related_topics=[
                    UnlinkedTopicSchema.model_validate(rt)
                    for rt in rel_topics_by_trend_ssid.get(ssid, [])
                ],
            )

        trend_schema_by_ssid: Dict[int, TrendSchema] = {
            tr.ssid: _build_trend(tr) for tr in trends if tr.ssid is not None
        }

        # ---- Assemble topic schemas ----

        def _build_topic(topic: Topic) -> TopicInOpportunitySchema:
            tid = topic.tid or 0
            g_ms = topic_global_by_tid.get(tid)
            g_delta = topic_global_delta_by_id.get(topic.topic_id)
            return TopicInOpportunitySchema(
                tid=topic.tid,
                sow=topic.sid,
                load_date=topic.load_date,
                topic_id=topic.topic_id,
                topic_name=topic.topic_name,
                topic_status=topic.topic_status,
                topic_description=topic.topic_description,
                topic_image_s3_uri=topic.topic_image_s3_uri,
                masterfile_version=topic.masterfile_version,
                for_deletion=topic.for_deletion,
                new_discovery=topic.new_discovery,
                trend=trend_schema_by_ssid.get(topic.ssid or 0),
                driver=drivers_by_topic_tid.get(tid, []),
                maturity_scores=[
                    _maturity_score_schema(ms, topic_sources_by_score.get(ms.id or 0, []))
                    for ms in sorted(
                        topic_non_global_by_tid.get(tid, []), key=lambda x: str(x.category)
                    )
                ],
                maturity_scores_deltas=[
                    MaturityScoreDeltaSchema.model_validate(d)
                    for d in sorted(
                        topic_non_global_deltas_by_id.get(topic.topic_id, []),
                        key=lambda x: str(x.category),
                    )
                ],
                global_maturity_score=(
                    _maturity_score_schema(g_ms, topic_sources_by_score.get(g_ms.id or 0, []))
                    if g_ms
                    else None
                ),
                global_maturity_score_delta=(
                    MaturityScoreDeltaSchema.model_validate(g_delta) if g_delta else None
                ),
            )

        # ---- Assemble opportunity schemas ----
        result: List[OpportunitySchema] = []
        for opp in opportunities:
            opp_tids = tids_by_opp.get(opp.oid or 0, [])
            opp_topics = [topics_by_tid[tid] for tid in opp_tids if tid in topics_by_tid]
            topic_schemas = [_build_topic(t) for t in opp_topics]
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
                )
            )

        return result
