from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from database.tenant_models.enums import TenantUserStatus


class TenantUserSchema(BaseModel):
    """Response DTO for a tenant user."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: Optional[str] = None
    display_name: str
    status: TenantUserStatus
    is_active: bool
    created_at: datetime
    updated_at: datetime
    last_login_at: Optional[datetime] = None
    locale: str
    timezone: str
    job_title: str
    extra_metadata: Optional[dict[str, Any]] = None


class TenantUserCreateSchema(BaseModel):
    """Request DTO for creating a tenant user."""

    email: Optional[str] = None
    display_name: str
    status: TenantUserStatus = TenantUserStatus.ACTIVE
    is_active: bool = True
    locale: str = "en"
    timezone: str = "UTC"
    job_title: str = ""
    extra_metadata: Optional[dict[str, Any]] = None


class TenantUserUpdateSchema(BaseModel):
    """Request DTO for fully replacing a tenant user (PUT)."""

    email: Optional[str] = None
    display_name: str
    status: TenantUserStatus
    is_active: bool
    locale: str
    timezone: str
    job_title: str
    extra_metadata: Optional[dict[str, Any]] = None
