"""Service layer for the sows endpoints."""

from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException

from database.public_models.models import Geography, PublicSow
from database.schemas.sow import DEFAULT_GEOGRAPHY_ID, DEFAULT_GEOGRAPHY_NAME, SowSchema
from database.tenant_models.models import TenantSow
from repositories.sow_repository import SowRepository


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

    def get_sows(self, tenant_schema: Optional[str]) -> List[SowSchema]:
        if not tenant_schema:
            raise HTTPException(
                status_code=400,
                detail="Authorization token missing tenant schema information.",
            )

        sows = self.repo.get_latest_live_sows(tenant_schema)
        if not sows:
            return []

        cs_sow_ids = [s.cs_sow_id for s in sows if s.cs_sow_id]
        rows = self.repo.get_sow_geographies(cs_sow_ids) if cs_sow_ids else []
        geo_map = _build_geo_map(rows)

        return [_assemble_sow_schema(sow, geo_map) for sow in sows]

    def get_sow(self, tenant_schema: Optional[str], sow_id: int) -> SowSchema:
        if not tenant_schema:
            raise HTTPException(
                status_code=400,
                detail="Authorization token missing tenant schema information.",
            )

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
