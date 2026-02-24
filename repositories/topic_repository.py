"""Repository functions for the Topic model."""

from typing import List, Optional

from sqlmodel import select

from database.manager import db
from database.tenant_models.models import Topic


def get_all(tenant_schema: str) -> List[Topic]:
    """Return all Topic rows, optionally scoped to a tenant schema."""
    with db.tenant_session(tenant_schema) as session:
        statement = select(Topic).where(Topic.for_deletion == False)  # noqa: E712
        return list(session.exec(statement).all())


def get_all_by_sow_id(tenant_schema: str, sow_id: int) -> List[Topic]:
    """Return all Topic rows for a given sow_id, optionally scoped to a tenant schema."""
    with db.tenant_session(tenant_schema) as session:
        statement = select(Topic).where(
            Topic.for_deletion == False, Topic.sid == sow_id  # noqa: E712
        )
        return list(session.exec(statement).all())


def get_by_id(tenant_schema: str, tid: int) -> Optional[Topic]:
    """Return a single Topic by its `tid` (or None)."""
    statement = select(Topic).where(Topic.tid == tid, Topic.for_deletion == False)  # noqa: E712
    print(statement)
    with db.tenant_session(tenant_schema) as session:
        return session.exec(statement).first()


def get_by_topic_id(tenant_schema: str, topic_id: str) -> Optional[Topic]:
    """Return a single Topic by its `topic_id` (or None)."""
    statement = (
        select(Topic)
        .where(Topic.topic_id == topic_id, Topic.for_deletion == False)  # noqa: E712
        .order_by(Topic.sid.desc())  # type: ignore[attr-defined]
    )  # noqa: E712
    print(statement)
    with db.tenant_session(tenant_schema) as session:
        return session.exec(statement).first()
