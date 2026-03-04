from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID, uuid4

from fastapi import HTTPException

from database.schemas.tenant_user import TenantUserCreateSchema, TenantUserUpdateSchema
from database.tenant_models.models import TenantUser
from repositories.tenant_user_repository import TenantUserRepository


class TenantUserService:
    """Service layer for TenantUser business logic."""

    def __init__(self, repo: TenantUserRepository) -> None:
        self.repo = repo

    def _validate_tenant_schema(self, tenant_schema: Optional[str]) -> str:
        if not tenant_schema:
            raise HTTPException(
                status_code=400,
                detail="Authorization token missing tenant schema information.",
            )
        return tenant_schema

    def get_all_users(self, tenant_schema: Optional[str]) -> List[TenantUser]:
        """Return all tenant users."""
        schema = self._validate_tenant_schema(tenant_schema)
        return self.repo.get_all(schema)

    def get_user(self, tenant_schema: Optional[str], user_id: UUID) -> TenantUser:
        """Return a single tenant user by ID."""
        schema = self._validate_tenant_schema(tenant_schema)
        user = self.repo.get_by_id(schema, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="Tenant user not found")
        return user

    def create_user(
        self, tenant_schema: Optional[str], payload: TenantUserCreateSchema
    ) -> TenantUser:
        """Create a new tenant user."""
        schema = self._validate_tenant_schema(tenant_schema)
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
        return self.repo.create(schema, user)

    def update_user(
        self, tenant_schema: Optional[str], user_id: UUID, payload: TenantUserUpdateSchema
    ) -> TenantUser:
        """Fully replace an existing tenant user (PUT semantics)."""
        schema = self._validate_tenant_schema(tenant_schema)
        user = self.repo.get_by_id(schema, user_id)
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

        return self.repo.update(schema, user)

    def delete_user(self, tenant_schema: Optional[str], user_id: UUID) -> None:
        """Delete a tenant user by ID."""
        schema = self._validate_tenant_schema(tenant_schema)
        deleted = self.repo.delete(schema, user_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Tenant user not found")
