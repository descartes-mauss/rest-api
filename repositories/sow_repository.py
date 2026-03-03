"""Repository for the sows endpoints.

Queries span two schemas:
  - Tenant schema : TenantSow, Trend, MaturityScore, MaturityScoreSource,
                    MaturityScoreDelta, Topic, Driver, Topic2Driver
  - Public schema : PublicSow + Geography (geography enrichment)
"""

from typing import Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.public_models.models import Geography, PublicSow
from database.tenant_models.models import (
    Driver,
    MaturityScore,
    MaturityScoreDelta,
    MaturityScoreSource,
    TenantSow,
    Topic,
    Topic2Driver,
    Trend,
)


class SowRepository:
    """Repository for data required by the sows endpoints."""

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    # ------------------------------------------------------------------
    # Tenant schema — SOW queries
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

    def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]:
        """Return all live non-deleted SOW versions sharing the same cs_sow_id, newest first."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(TenantSow)
                .where(
                    TenantSow.cs_sow_id == cs_sow_id,
                    TenantSow.sow_status == "live",
                    TenantSow.for_deletion == False,  # noqa: E712
                )
                .order_by(TenantSow.sid.desc())  # type: ignore[union-attr]
            )
            return list(session.exec(stmt).all())

    # ------------------------------------------------------------------
    # Tenant schema — Trend / Shift queries
    # ------------------------------------------------------------------

    def get_trends_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Trend]:
        """Return all non-deleted Trend rows for the given sow sid."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Trend).where(
                Trend.sid == sow_sid,
                Trend.for_deletion == False,  # noqa: E712
            )
            return list(session.exec(stmt).all())

    def get_maturity_scores_for_trend_ids(
        self, tenant_schema: str, trend_ssids: List[int]
    ) -> List[MaturityScore]:
        """Return all MaturityScore rows (global and non-global) for the given trend ssids."""
        if not trend_ssids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScore).where(
                MaturityScore.trend_id.in_(trend_ssids)  # type: ignore[union-attr]
            )
            return list(session.exec(stmt).all())

    def get_maturity_score_sources(
        self, tenant_schema: str, score_ids: List[int]
    ) -> List[MaturityScoreSource]:
        """Return MaturityScoreSource rows for the given score ids."""
        if not score_ids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScoreSource).where(
                MaturityScoreSource.maturity_score_id.in_(score_ids)  # type: ignore[attr-defined]
            )
            return list(session.exec(stmt).all())

    def get_maturity_score_deltas_for_sow_trends(
        self, tenant_schema: str, sow_sid: int, trend_id_strings: List[str]
    ) -> List[MaturityScoreDelta]:
        """Return MaturityScoreDelta rows for the given sow and trend_id strings (topic_id=null)."""
        if not trend_id_strings:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScoreDelta).where(
                MaturityScoreDelta.sow_id == sow_sid,
                MaturityScoreDelta.trend_id.in_(trend_id_strings),  # type: ignore[union-attr]
                MaturityScoreDelta.topic_id == None,  # noqa: E711
                MaturityScoreDelta.for_deletion == False,  # noqa: E712
            )
            return list(session.exec(stmt).all())

    def get_topics_for_trends(self, tenant_schema: str, trend_ssids: List[int]) -> List[Topic]:
        """Return all non-deleted Topic rows for the given trend ssids."""
        if not trend_ssids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Topic).where(
                Topic.ssid.in_(trend_ssids),  # type: ignore[union-attr]
                Topic.for_deletion == False,  # noqa: E712
            )
            return list(session.exec(stmt).all())

    # ------------------------------------------------------------------
    # Tenant schema — Driver queries
    # ------------------------------------------------------------------

    def get_drivers_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Driver]:
        """Return all Driver rows for the given sow sid."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Driver).where(Driver.sow_sid == sow_sid)
            return list(session.exec(stmt).all())

    def get_topic_counts_for_drivers(
        self, tenant_schema: str, driver_dids: List[int]
    ) -> Dict[int, int]:
        """Return a mapping of driver_did → topic count via the Topic2Driver join table."""
        if not driver_dids:
            return {}
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(
                    Topic2Driver.driver_did,
                    func.count(Topic2Driver.tdid).label("count"),  # type: ignore[arg-type]
                )
                .where(Topic2Driver.driver_did.in_(driver_dids))  # type: ignore[attr-defined]
                .group_by(Topic2Driver.driver_did)  # type: ignore[arg-type]
            )
            rows = session.exec(stmt).all()
            return {row[0]: row[1] for row in rows}

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
