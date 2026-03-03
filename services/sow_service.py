"""Service layer for the sows endpoints."""

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException

from database.public_models.models import Geography, PublicSow
from database.schemas.driver import DriverSchema
from database.schemas.shift import (
    MaturityScoreDeltaSchema,
    MaturityScoreSchema,
    MaturityScoreSourceSchema,
    ShiftSchema,
    TrendInShiftSchema,
    UnlinkedTopicSchema,
)
from database.schemas.sow import DEFAULT_GEOGRAPHY_ID, DEFAULT_GEOGRAPHY_NAME, SowSchema
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

            trend_schema = TrendInShiftSchema(
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
