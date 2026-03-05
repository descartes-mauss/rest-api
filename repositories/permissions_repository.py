"""Repository for the permissions endpoint.

Fetches tenant SOW, client tier features, experiments, and opportunity platform
availability from both the public and tenant schemas.
"""

from typing import List, Optional

from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.public_models.models import Client, Experiment, PublicSow, ServiceFeature, TierFeature
from database.tenant_models.models import Opportunity, TenantSow


class PermissionsRepository:
    """Repository for data required by the permissions endpoint."""

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]:
        """Return the TenantSow with the given sid, or None."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(TenantSow).where(TenantSow.sid == sow_id)
            return session.exec(stmt).first()  # type: ignore[no-any-return]

    def get_client_tier_id(self, org_id: str) -> Optional[int]:
        """Return the tier_id for the cs_interface Client matching org_id."""
        with self.db.session() as session:
            stmt = select(Client.tier_id).where(Client.customer_id == org_id.lower())
            return session.exec(stmt).first()  # type: ignore[no-any-return]

    def get_feature_codes(self, tier_id: int) -> List[str]:
        """Return all feature codes available for the given service tier."""
        with self.db.session() as session:
            stmt = (
                select(ServiceFeature.code)
                .join(TierFeature, TierFeature.feature_id == ServiceFeature.id)  # type: ignore[arg-type]
                .where(TierFeature.tier_id == tier_id)
            )
            return list(session.exec(stmt).all())

    def get_experiments(self, cs_sow_id: Optional[str]) -> List[Experiment]:
        """Return all Experiment rows associated with the given cs_sow_id.

        Joins PublicSow → Experiment on PublicSow.id = Experiment.sow_id,
        filtering by PublicSow.sow_id = cs_sow_id.
        """
        if not cs_sow_id:
            return []
        with self.db.session() as session:
            stmt = (
                select(Experiment)
                .join(PublicSow, PublicSow.id == Experiment.sow_id)  # type: ignore[arg-type]
                .where(PublicSow.sow_id == cs_sow_id)
            )
            return list(session.exec(stmt).all())

    def has_opportunity_platforms(self, tenant_schema: str, sow_sid: int) -> bool:
        """Return True if any non-deleted Opportunity row exists for the given sow."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(Opportunity)
                .where(
                    Opportunity.sid == sow_sid,
                    Opportunity.for_deletion == False,  # noqa: E712
                )
                .limit(1)
            )
            return session.exec(stmt).first() is not None


__all__ = ["PermissionsRepository"]
