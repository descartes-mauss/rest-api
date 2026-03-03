"""Repository for the sows endpoints.

Queries span two schemas:
  - Tenant schema : TenantSow (latest live SOWs per cs_sow_id)
  - Public schema : PublicSow + Geography (geography enrichment)
"""

from typing import List, Optional, Tuple

from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.public_models.models import Geography, PublicSow
from database.tenant_models.models import TenantSow


class SowRepository:
    """Repository for data required by the sows endpoints."""

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    # ------------------------------------------------------------------
    # Tenant schema queries
    # ------------------------------------------------------------------

    def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]:
        """Return the most recent live SOW per cs_sow_id, ordered by cs_sow_id."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(TenantSow)
                .where(TenantSow.sow_status == "live")
                .distinct(TenantSow.cs_sow_id)  # type: ignore[arg-type]
                .order_by(TenantSow.cs_sow_id, TenantSow.load_date.desc())  # type: ignore[attr-defined]
            )
            return list(session.exec(stmt).all())

    def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
        """Return a TenantSow by primary key (sid), or None."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(TenantSow).where(TenantSow.sid == sow_id)
            return session.exec(stmt).first()  # type: ignore[no-any-return]

    def get_latest_live_sow_for_cs_sow_id(
        self, tenant_schema: str, cs_sow_id: str
    ) -> Optional[TenantSow]:
        """Return the most recent live TenantSow for the given cs_sow_id."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(TenantSow)
                .where(TenantSow.sow_status == "live", TenantSow.cs_sow_id == cs_sow_id)
                .order_by(TenantSow.load_date.desc())  # type: ignore[attr-defined]
                .limit(1)
            )
            return session.exec(stmt).first()  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Public schema queries
    # ------------------------------------------------------------------

    def get_sow_geographies(
        self, cs_sow_ids: List[str]
    ) -> List[Tuple[PublicSow, Optional[Geography]]]:
        """Return (PublicSow, Geography) pairs for the given cs_sow_ids with status 'live'."""
        if not cs_sow_ids:
            return []
        with self.db.session() as session:
            stmt = (
                select(PublicSow, Geography)
                .outerjoin(Geography, PublicSow.geography_id == Geography.geography_id)  # type: ignore[arg-type]
                .where(
                    PublicSow.sow_id.in_(cs_sow_ids),  # type: ignore[union-attr]
                    PublicSow.sow_status == "live",
                )
            )
            return list(session.exec(stmt).all())


__all__ = ["SowRepository"]
