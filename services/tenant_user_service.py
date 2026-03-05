"""Service layer for the tenant users endpoint."""

from datetime import datetime, timezone
from typing import List
from uuid import UUID, uuid4

from fastapi import HTTPException

from database.schemas.tenant_user import TenantUserCreateSchema, TenantUserUpdateSchema
from database.tenant_models.models import TenantUser
from repositories.tenant_user_repository import TenantUserRepository


class TenantUserService:
    """Service layer for TenantUser business logic."""

    def __init__(self, repo: TenantUserRepository) -> None:
        self.repo = repo

    def get_all_users(self, tenant_schema: str) -> List[TenantUser]:
        """Return all tenant users."""
        return self.repo.get_all(tenant_schema)

    def get_user(self, tenant_schema: str, user_id: UUID) -> TenantUser:
        """Return a single tenant user by ID."""
        user = self.repo.get_by_id(tenant_schema, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="Tenant user not found")
        return user

    def create_user(self, tenant_schema: str, payload: TenantUserCreateSchema) -> TenantUser:
        """Create a new tenant user."""
        now = datetime.now(timezone.utc)
        user = TenantUser(
            id=uuid4(),
            email=payload.email,
            display_name=payload.display_name,
            status=payload.status,
            is_active=payload.is_active,
            created_at=now,
            updated_at=now,
            locale=payload.locale,
            timezone=payload.timezone,
            job_title=payload.job_title,
            extra_metadata=payload.extra_metadata,
        )
        return self.repo.create(tenant_schema, user)

    def update_user(
        self, tenant_schema: str, user_id: UUID, payload: TenantUserUpdateSchema
    ) -> TenantUser:
        """Fully replace an existing tenant user (PUT semantics)."""
        user = self.repo.get_by_id(tenant_schema, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="Tenant user not found")

        user.email = payload.email
        user.display_name = payload.display_name
        user.status = payload.status
        user.is_active = payload.is_active
        user.locale = payload.locale
        user.timezone = payload.timezone
        user.job_title = payload.job_title
        user.extra_metadata = payload.extra_metadata
        user.updated_at = datetime.now(timezone.utc)

        return self.repo.update(tenant_schema, user)

    def delete_user(self, tenant_schema: str, user_id: UUID) -> None:
        """Delete a tenant user by ID."""
        deleted = self.repo.delete(tenant_schema, user_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Tenant user not found")
