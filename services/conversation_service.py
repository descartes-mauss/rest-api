from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID, uuid4

from fastapi import HTTPException

from database.schemas.conversation import (
    ConversationCreateSchema,
    ConversationSchema,
    ConversationUpdateSchema,
)
from database.schemas.tenant_user import TenantUserSchema
from database.tenant_models.models import Conversation
from repositories.conversation_repository import ConversationRepository


class ConversationService:
    """Service layer for Conversation business logic."""

    def __init__(self, repo: ConversationRepository) -> None:
        self.repo = repo

    def _validate_tenant_schema(self, tenant_schema: Optional[str]) -> str:
        if not tenant_schema:
            raise HTTPException(
                status_code=400,
                detail="Authorization token missing tenant schema information.",
            )
        return tenant_schema

    def get_all_conversations(self, tenant_schema: Optional[str]) -> List[ConversationSchema]:
        """Return all conversations with nested user."""
        schema = self._validate_tenant_schema(tenant_schema)
        rows = self.repo.get_all(schema)
        return [
            ConversationSchema(
                id=conv.id,
                title=conv.title,
                status=conv.status,
                created_at=conv.created_at,
                updated_at=conv.updated_at,
                deleted_at=conv.deleted_at,
                purge_at=conv.purge_at,
                user=TenantUserSchema.model_validate(user),
            )
            for conv, user in rows
        ]

    def get_conversation(
        self, tenant_schema: Optional[str], conversation_id: UUID
    ) -> ConversationSchema:
        """Return a single conversation by ID with nested user."""
        schema = self._validate_tenant_schema(tenant_schema)
        result = self.repo.get_by_id(schema, conversation_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Conversation not found")
        conv, user = result
        return ConversationSchema(
            id=conv.id,
            title=conv.title,
            status=conv.status,
            created_at=conv.created_at,
            updated_at=conv.updated_at,
            deleted_at=conv.deleted_at,
            purge_at=conv.purge_at,
            user=TenantUserSchema.model_validate(user),
        )

    def create_conversation(
        self, tenant_schema: Optional[str], payload: ConversationCreateSchema
    ) -> ConversationSchema:
        """Create a new conversation and return it with nested user."""
        schema = self._validate_tenant_schema(tenant_schema)
        now = datetime.now(timezone.utc)
        conversation = Conversation(
            id=uuid4(),
            title=payload.title,
            status=payload.status,
            created_at=now,
            updated_at=now,
            user_id=payload.user_id,
        )
        created = self.repo.create(schema, conversation)
        # Re-fetch with joined user
        result = self.repo.get_by_id(schema, created.id)
        if result is None:
            raise HTTPException(status_code=500, detail="Failed to fetch created conversation")
        conv, user = result
        return ConversationSchema(
            id=conv.id,
            title=conv.title,
            status=conv.status,
            created_at=conv.created_at,
            updated_at=conv.updated_at,
            deleted_at=conv.deleted_at,
            purge_at=conv.purge_at,
            user=TenantUserSchema.model_validate(user),
        )

    def update_conversation(
        self,
        tenant_schema: Optional[str],
        conversation_id: UUID,
        payload: ConversationUpdateSchema,
    ) -> ConversationSchema:
        """Fully replace a conversation (PUT semantics) and return it with nested user."""
        schema = self._validate_tenant_schema(tenant_schema)
        conv = self.repo.get_raw_by_id(schema, conversation_id)
        if conv is None:
            raise HTTPException(status_code=404, detail="Conversation not found")

        conv.title = payload.title
        conv.status = payload.status
        conv.updated_at = datetime.now(timezone.utc)

        updated = self.repo.update(schema, conv)
        result = self.repo.get_by_id(schema, updated.id)
        if result is None:
            raise HTTPException(status_code=500, detail="Failed to fetch updated conversation")
        merged_conv, user = result
        return ConversationSchema(
            id=merged_conv.id,
            title=merged_conv.title,
            status=merged_conv.status,
            created_at=merged_conv.created_at,
            updated_at=merged_conv.updated_at,
            deleted_at=merged_conv.deleted_at,
            purge_at=merged_conv.purge_at,
            user=TenantUserSchema.model_validate(user),
        )

    def delete_conversation(self, tenant_schema: Optional[str], conversation_id: UUID) -> None:
        """Delete a conversation by ID."""
        schema = self._validate_tenant_schema(tenant_schema)
        deleted = self.repo.delete(schema, conversation_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Conversation not found")
