"""Repository for the sows endpoints.

Queries span two schemas:
  - Tenant schema : TenantSow, Trend, MaturityScore, MaturityScoreSource,
                    MaturityScoreDelta, Topic, Driver, Topic2Driver,
                    Opportunity, Topic2Opportunity
  - Public schema : PublicSow + Geography (geography enrichment)
"""

from typing import Dict, List, Optional, Tuple

from sqlalchemy import cast, func, or_
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.public_models.models import Geography, PublicSow
from database.tenant_models.models import (
    Driver,
    Insight,
    InsightSource,
    MaturityScore,
    MaturityScoreDelta,
    MaturityScoreSource,
    Opportunity,
    TenantSow,
    Topic,
    Topic2Driver,
    Topic2Opportunity,
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

    def get_trends_for_sow(
        self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
    ) -> List[Trend]:
        """Return all non-deleted Trend rows for the given sow sid."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Trend).where(
                Trend.sid == sow_sid,
                Trend.for_deletion == False,  # noqa: E712
            )
            if name_order == "asc":
                stmt = stmt.order_by(func.lower(Trend.trend_name))
            elif name_order == "desc":
                stmt = stmt.order_by(func.lower(Trend.trend_name).desc())
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

    def get_topics_for_sow(
        self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
    ) -> List[Topic]:
        """Return all non-deleted Topic rows for the given sow sid."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Topic).where(
                Topic.sid == sow_sid,
                Topic.for_deletion == False,  # noqa: E712
            )
            if name_order == "asc":
                stmt = stmt.order_by(func.lower(Topic.topic_name))
            elif name_order == "desc":
                stmt = stmt.order_by(func.lower(Topic.topic_name).desc())
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

    def get_drivers_for_sow(
        self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
    ) -> List[Driver]:
        """Return all Driver rows for the given sow sid."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Driver).where(Driver.sow_sid == sow_sid)
            if name_order == "asc":
                stmt = stmt.order_by(func.lower(Driver.driver_name))
            elif name_order == "desc":
                stmt = stmt.order_by(func.lower(Driver.driver_name).desc())
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
                    Topic2Driver.did,
                    func.count(Topic2Driver.tdid).label("count"),  # type: ignore[arg-type]
                )
                .where(Topic2Driver.did.in_(driver_dids))  # type: ignore[attr-defined]
                .group_by(Topic2Driver.did)  # type: ignore[arg-type]
            )
            rows = session.exec(stmt).all()
            return {row[0]: row[1] for row in rows}

    # ------------------------------------------------------------------
    # Tenant schema — Opportunity queries
    # ------------------------------------------------------------------

    def get_opportunities_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Opportunity]:
        """Return Opportunity rows for the given sow sid, ordered by opportunity."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(Opportunity)
                .where(Opportunity.sid == sow_sid)
                .order_by(Opportunity.opportunity)  # type: ignore[arg-type]
            )
            return list(session.exec(stmt).all())

    def get_topic2opportunity_rows(
        self, tenant_schema: str, opp_oids: List[int]
    ) -> List[Topic2Opportunity]:
        """Return Topic2Opportunity join rows for the given opportunity oids."""
        if not opp_oids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Topic2Opportunity).where(
                Topic2Opportunity.oid.in_(opp_oids)  # type: ignore[attr-defined]
            )
            return list(session.exec(stmt).all())

    def get_topics_by_ids(self, tenant_schema: str, topic_tids: List[int]) -> List[Topic]:
        """Return Topic rows for the given tids."""
        if not topic_tids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Topic).where(Topic.tid.in_(topic_tids))  # type: ignore[union-attr]
            return list(session.exec(stmt).all())

    def get_maturity_scores_for_topic_ids(
        self, tenant_schema: str, topic_tids: List[int]
    ) -> List[MaturityScore]:
        """Return all MaturityScore rows for the given topic tids."""
        if not topic_tids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScore).where(MaturityScore.topic_id.in_(topic_tids))  # type: ignore[union-attr]
            return list(session.exec(stmt).all())

    def get_maturity_score_deltas_for_sow_topic_ids(
        self, tenant_schema: str, sow_sid: int, topic_id_strings: List[str]
    ) -> List[MaturityScoreDelta]:
        """Return MaturityScoreDelta rows for the given sow and topic_id strings (trend_id=null)."""
        if not topic_id_strings:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScoreDelta).where(
                MaturityScoreDelta.sow_id == sow_sid,
                MaturityScoreDelta.topic_id.in_(topic_id_strings),  # type: ignore[union-attr]
                MaturityScoreDelta.trend_id == None,  # noqa: E711
                MaturityScoreDelta.for_deletion == False,  # noqa: E712
            )
            return list(session.exec(stmt).all())

    def get_topic_drivers_by_topic_ids(
        self, tenant_schema: str, topic_tids: List[int]
    ) -> List[Topic2Driver]:
        """Return Topic2Driver rows for the given topic tids."""
        if not topic_tids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Topic2Driver).where(
                Topic2Driver.tid.in_(topic_tids)  # type: ignore[attr-defined]
            )
            return list(session.exec(stmt).all())

    def get_trends_by_ssids(self, tenant_schema: str, trend_ssids: List[int]) -> List[Trend]:
        """Return Trend rows for the given ssids."""
        if not trend_ssids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Trend).where(Trend.ssid.in_(trend_ssids))  # type: ignore[union-attr]
            return list(session.exec(stmt).all())

    # ------------------------------------------------------------------
    # Tenant schema — Insight queries
    # ------------------------------------------------------------------

    def get_insights_for_cs_sow_id(
        self, tenant_schema: str, cs_sow_id: str, offset: int, limit: int
    ) -> Tuple[int, List[Insight]]:
        """Return (total, page) of Insights for the given cs_sow_id, newest first."""
        with self.db.tenant_session(tenant_schema) as session:
            total: int = session.exec(
                select(func.count()).select_from(Insight).where(Insight.cs_sow_id == cs_sow_id)
            ).one()
            stmt = (
                select(Insight)
                .where(Insight.cs_sow_id == cs_sow_id)
                .order_by(Insight.created_at.desc())  # type: ignore[attr-defined]
                .offset(offset)
                .limit(limit)
            )
            return total, list(session.exec(stmt).all())

    def get_insights_filtered(
        self,
        tenant_schema: str,
        cs_sow_id: str,
        entity_ids: List[str],
        offset: int,
        limit: int,
    ) -> Tuple[int, List[Insight]]:
        """Return (total, page) of Insights filtered by entity_ids, newest first."""
        with self.db.tenant_session(tenant_schema) as session:
            total = session.exec(
                select(func.count())
                .select_from(Insight)
                .where(Insight.cs_sow_id == cs_sow_id, Insight.entity_id.in_(entity_ids))  # type: ignore[attr-defined]
            ).one()
            stmt = (
                select(Insight)
                .where(Insight.cs_sow_id == cs_sow_id, Insight.entity_id.in_(entity_ids))  # type: ignore[attr-defined]
                .order_by(Insight.created_at.desc())  # type: ignore[attr-defined]
                .offset(offset)
                .limit(limit)
            )
            return total, list(session.exec(stmt).all())

    def get_insight_sources_for_insight_ids(
        self, tenant_schema: str, insight_ids: List[int]
    ) -> List[InsightSource]:
        """Return InsightSource rows whose insight_ids JSON array contains any of the given IDs."""
        if not insight_ids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            conditions = [
                cast(InsightSource.insight_ids, JSONB).contains([iid]) for iid in insight_ids
            ]
            stmt = select(InsightSource).where(or_(*conditions))
            return list(session.exec(stmt).all())

    def get_topics_by_topic_str_ids(
        self, tenant_schema: str, sow_sid: int, topic_ids: List[str]
    ) -> List[Topic]:
        """Return non-deleted Topics for the given sow by string topic_id."""
        if not topic_ids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Topic).where(
                Topic.sid == sow_sid,
                Topic.topic_id.in_(topic_ids),  # type: ignore[attr-defined]
                Topic.for_deletion == False,  # noqa: E712
            )
            return list(session.exec(stmt).all())

    def get_trends_by_trend_str_ids(
        self, tenant_schema: str, sow_sid: int, trend_ids: List[str]
    ) -> List[Trend]:
        """Return non-deleted Trends for the given sow by string trend_id."""
        if not trend_ids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Trend).where(
                Trend.sid == sow_sid,
                Trend.trend_id.in_(trend_ids),  # type: ignore[attr-defined]
                Trend.for_deletion == False,  # noqa: E712
            )
            return list(session.exec(stmt).all())

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
