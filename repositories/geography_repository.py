"""Repository for the geographies endpoint.

Fetches active geographies assigned to a client from the public schema.
"""

from typing import List, Optional, Tuple

from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.public_models.models import Client, ClientGeography, Geography


class GeographyRepository:
    """Repository for data required by the geographies endpoint."""

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    def get_client_id(self, org_id: str) -> Optional[int]:
        """Return the primary key of the cs_interface Client matching org_id."""
        with self.db.session() as session:
            stmt = select(Client.id).where(Client.customer_id == org_id.lower())
            return session.exec(stmt).first()  # type: ignore[no-any-return]

    def get_active_geographies(self, client_id: int) -> List[Tuple[ClientGeography, Geography]]:
        """Return all active (ClientGeography, Geography) pairs for the given client."""
        with self.db.session() as session:
            stmt = (
                select(ClientGeography, Geography)
                .join(Geography, ClientGeography.geography_id == Geography.geography_id)  # type: ignore[arg-type]
                .where(
                    ClientGeography.client_id == client_id,
                    Geography.is_active == True,  # noqa: E712
                )
            )
            return list(session.exec(stmt).all())


__all__ = ["GeographyRepository"]
