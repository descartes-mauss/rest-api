"""Service layer for the sows endpoints."""

from typing import List

from fastapi import HTTPException

from database.schemas.sow import SowSchema
from database.tenant_models.models import TenantSow
from repositories.protocols import SowRepositoryProtocol
from services.assemblers.sow_assembler import _build_geo_map, _sow_to_schema


class SowService:
    """Orchestrates data fetching and assembles SOW responses."""

    def __init__(self, sow_repository: SowRepositoryProtocol) -> None:
        self.sow_repository = sow_repository

    def _get_sow_or_404(self, tenant_schema: str, sow_id: int) -> TenantSow:
        """Return the TenantSow for sow_id, or raise HTTP 404."""
        sow = self.sow_repository.get_sow_by_id(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SOW not available")
        return sow

    def get_sows(self, tenant_schema: str) -> List[SowSchema]:
        sows = self.sow_repository.get_latest_live_sows(tenant_schema)
        if not sows:
            return []

        cs_sow_ids = [s.cs_sow_id for s in sows if s.cs_sow_id]
        rows = self.sow_repository.get_sow_geographies(cs_sow_ids) if cs_sow_ids else []
        geo_map = _build_geo_map(rows)

        return [_sow_to_schema(sow, geo_map) for sow in sows]

    def get_sow(self, tenant_schema: str, sow_id: int) -> SowSchema:
        sow = self._get_sow_or_404(tenant_schema, sow_id)

        if not sow.cs_sow_id:
            raise HTTPException(status_code=404, detail="SOW not available")

        latest = self.sow_repository.get_latest_live_sow_for_cs_sow_id(tenant_schema, sow.cs_sow_id)
        if latest is None or latest.sid != sow_id:
            raise HTTPException(status_code=404, detail="SOW not available")

        rows = self.sow_repository.get_sow_geographies([sow.cs_sow_id])
        geo_map = _build_geo_map(rows)

        return _sow_to_schema(sow, geo_map)

    def get_versions(self, tenant_schema: str, sow_id: int) -> List[SowSchema]:
        sow = self._get_sow_or_404(tenant_schema, sow_id)

        if not sow.cs_sow_id:
            return []

        versions = self.sow_repository.get_sow_versions(tenant_schema, sow.cs_sow_id)
        cs_sow_ids = [v.cs_sow_id for v in versions if v.cs_sow_id]
        rows = self.sow_repository.get_sow_geographies(list(set(cs_sow_ids))) if cs_sow_ids else []
        geo_map = _build_geo_map(rows)

        return [_sow_to_schema(v, geo_map) for v in versions]


__all__ = ["SowService"]
