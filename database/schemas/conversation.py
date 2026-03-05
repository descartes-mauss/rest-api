from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from database.schemas.tenant_user import TenantUserSchema
from database.tenant_models.enums import ConversationStatus


class ConversationSchema(BaseModel):
    """Response DTO for a conversation."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    title: str
    status: ConversationStatus
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None
    purge_at: Optional[datetime] = None
    user: TenantUserSchema


class ConversationCreateSchema(BaseModel):
    """Request DTO for creating a conversation."""

    title: str
    status: ConversationStatus = ConversationStatus.ACTIVE
    user_id: UUID


class ConversationUpdateSchema(BaseModel):
    """Request DTO for fully replacing a conversation (PUT)."""

    title: str
    status: ConversationStatus
