from typing import List, Optional, Tuple, cast
from uuid import UUID

from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.tenant_models.models import Conversation, TenantUser


class ConversationRepository:
    """Repository for Conversation data access."""

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    def get_all(self, tenant_schema: str) -> List[Tuple[Conversation, TenantUser]]:
        """Return all conversations joined with their user."""
        with self.db.tenant_session(tenant_schema) as session:
            statement = select(Conversation, TenantUser).where(
                Conversation.user_id == TenantUser.id
            )
            return list(session.exec(statement).all())

    def get_by_id(
        self, tenant_schema: str, conversation_id: UUID
    ) -> Optional[Tuple[Conversation, TenantUser]]:
        """Return a single conversation with its user, or None."""
        with self.db.tenant_session(tenant_schema) as session:
            statement = select(Conversation, TenantUser).where(
                Conversation.id == conversation_id, Conversation.user_id == TenantUser.id
            )
            result = session.exec(statement).first()
            return cast(Optional[Tuple[Conversation, TenantUser]], result)

    def get_raw_by_id(
        self, tenant_schema: str, conversation_id: UUID
    ) -> Optional[Conversation]:
        """Return a single conversation without joining user."""
        with self.db.tenant_session(tenant_schema) as session:
            statement = select(Conversation).where(Conversation.id == conversation_id)
            result = session.exec(statement).first()
            return cast(Optional[Conversation], result)

    def create(self, tenant_schema: str, conversation: Conversation) -> Conversation:
        """Insert a new conversation and return it."""
        with self.db.tenant_session(tenant_schema) as session:
            session.add(conversation)
            session.flush()
            session.refresh(conversation)
            return conversation

    def update(self, tenant_schema: str, conversation: Conversation) -> Conversation:
        """Merge an updated conversation and return it."""
        with self.db.tenant_session(tenant_schema) as session:
            merged = cast(Conversation, session.merge(conversation))
            session.flush()
            session.refresh(merged)
            return merged

    def delete(self, tenant_schema: str, conversation_id: UUID) -> bool:
        """Delete a conversation by ID. Returns True if deleted, False if not found."""
        with self.db.tenant_session(tenant_schema) as session:
            statement = select(Conversation).where(Conversation.id == conversation_id)
            conversation = session.exec(statement).first()
            if conversation is None:
                return False
            session.delete(conversation)
            return True
