import os
from typing import Any, Type

from dotenv import load_dotenv
from sqlmodel import select

from database.session import DBSession
from database.shared import SQLModelType
from database.tenant_models.models import Topic, Trend

load_dotenv()

DB_HOST = os.environ.get("POSTGRES_HOST")
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
DB_NAME = os.environ.get("POSTGRES_DB")
DB_PORT = os.environ.get("POSTGRES_PORT", 5432)

db = DBSession(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
)


def get_all(model: Type[SQLModelType], tenant_schema: str | None = None) -> list[SQLModelType]:
    """Fetch all rows for the given SqlModel `model`."""
    if tenant_schema:
        with db.tenant_session(tenant_schema) as session:
            statement = select(model)
            return list(session.exec(statement).all())

    with db.session() as session:
        return list(session.exec(select(model)).all())


def get_by_id(
    model: Type[SQLModelType], id: Any, tenant_schema: str | None = None
) -> SQLModelType | None:
    """Fetch a single row by ID for the given SqlModel `model`."""
    if tenant_schema:
        with db.tenant_session(tenant_schema) as session:
            return session.get(model, id)

    with db.session() as session:
        return session.get(model, id)


def get_topics(topic_id: str, tenant_schema: str) -> list[Topic]:
    """Fetch all Topic rows for the given topic."""

    if tenant_schema:
        with db.tenant_session(tenant_schema) as session:
            statement = select(Topic).where(Topic.topic_id == topic_id)
            return list(session.exec(statement).all())
    return []


def get_topics_trends(topic_id: str, tenant_schema: str) -> list[tuple[Topic, Trend]]:
    """Fetch all Topic rows for the given topic."""

    if tenant_schema:
        with db.tenant_session(tenant_schema) as session:
            statement = select(Topic, Trend).where(
                Topic.topic_id == topic_id, Trend.ssid == Topic.ssid
            )
            return list(session.exec(statement).all())
    return []
