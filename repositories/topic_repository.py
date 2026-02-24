"""Repository for the `Topic` model.

This module provides `TopicRepository`, which accepts a DB session provider
so callers (FastAPI dependencies or tests) can inject the DB object.
"""

from typing import Any, List, Optional, Protocol, cast

from sqlmodel import select

from database.tenant_models.models import Topic


class DBSessionProvider(Protocol):
    """Protocol describing the DB session provider used by repositories.

    The provider is expected to expose a `tenant_session(schema)` context
    manager that yields a SQLModel/SQLAlchemy session with `exec` and
    `get` methods.
    """

    def tenant_session(self, schema: str) -> Any: ...


class TopicRepository:
    """Repository for `Topic` that accepts an injectable DB provider.

    Example:
        repo = TopicRepository(db)  # where `db` is the manager.db object
    """

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    def get_all(self, tenant_schema: str) -> List[Topic]:
        """Return all non-deleted Topic rows for a tenant."""
        with self.db.tenant_session(tenant_schema) as session:
            statement = select(Topic).where(Topic.for_deletion == False)  # noqa: E712
            return list(session.exec(statement).all())

    def get_all_by_sow_id(self, tenant_schema: str, sow_id: int) -> List[Topic]:
        """Return all non-deleted Topic rows for a given sow_id."""
        with self.db.tenant_session(tenant_schema) as session:
            statement = select(Topic).where(
                Topic.for_deletion == False, Topic.sid == sow_id  # noqa: E712
            )
            return list(session.exec(statement).all())

    def get_by_id(self, tenant_schema: str, tid: int) -> Optional[Topic]:
        """Return a single Topic by its `tid` (or None)."""
        statement = select(Topic).where(Topic.tid == tid, Topic.for_deletion == False)  # noqa: E712
        with self.db.tenant_session(tenant_schema) as session:
            result = session.exec(statement).first()
            return cast(Optional[Topic], result)

    def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
        """Return a single Topic by its `topic_id` (or None)."""
        statement = (
            select(Topic)
            .where(Topic.topic_id == topic_id, Topic.for_deletion == False)  # noqa: E712
            .order_by(Topic.sid.desc())  # type: ignore[attr-defined]
        )
        with self.db.tenant_session(tenant_schema) as session:
            result = session.exec(statement).first()
            return cast(Optional[Topic], result)


__all__ = ["TopicRepository", "DBSessionProvider"]
