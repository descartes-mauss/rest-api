from typing import List, Optional, cast
from uuid import UUID

from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.tenant_models.models import TenantUser


class TenantUserRepository:
    """Repository for TenantUser data access."""

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    def get_all(self, tenant_schema: str) -> List[TenantUser]:
        """Return all tenant users."""
        with self.db.tenant_session(tenant_schema) as session:
            statement = select(TenantUser)
            return list(session.exec(statement).all())

    def get_by_id(self, tenant_schema: str, user_id: UUID) -> Optional[TenantUser]:
        """Return a single tenant user by ID, or None."""
        with self.db.tenant_session(tenant_schema) as session:
            statement = select(TenantUser).where(TenantUser.id == user_id)
            result = session.exec(statement).first()
            return cast(Optional[TenantUser], result)

    def create(self, tenant_schema: str, user: TenantUser) -> TenantUser:
        """Insert a new tenant user and return it."""
        with self.db.tenant_session(tenant_schema) as session:
            session.add(user)
            session.flush()
            session.refresh(user)
            return user

    def update(self, tenant_schema: str, user: TenantUser) -> TenantUser:
        """Merge an updated tenant user and return it."""
        with self.db.tenant_session(tenant_schema) as session:
            merged = cast(TenantUser, session.merge(user))
            session.flush()
            session.refresh(merged)
            return merged

    def delete(self, tenant_schema: str, user_id: UUID) -> bool:
        """Delete a tenant user by ID. Returns True if deleted, False if not found."""
        with self.db.tenant_session(tenant_schema) as session:
            statement = select(TenantUser).where(TenantUser.id == user_id)
            user = session.exec(statement).first()
            if user is None:
                return False
            session.delete(user)
            return True
