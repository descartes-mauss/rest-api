"""Repository for the `Topic` model.

This module provides `TopicRepository`, which accepts a DB session provider
so callers (FastAPI dependencies or tests) can inject the DB object.
"""

from typing import List, Optional, Tuple, cast

from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.tenant_models.models import (
    Driver,
    MaturityScore,
    MaturityScoreDelta,
    MaturityScoreSource,
    Source,
    Topic,
    Topic2Driver,
    Topic2Source,
    Trend,
)


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
            stmt = select(Topic).where(Topic.for_deletion == False)  # noqa: E712
            return list(session.exec(stmt).all())

    def get_all_by_sow_id(self, tenant_schema: str, sow_id: int) -> List[Topic]:
        """Return all non-deleted Topic rows for a given sow_id."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Topic).where(
                Topic.for_deletion == False, Topic.sid == sow_id  # noqa: E712
            )
            return list(session.exec(stmt).all())

    def get_by_id(self, tenant_schema: str, tid: int) -> Optional[Topic]:
        """Return a single Topic by its `tid` (or None)."""
        stmt = select(Topic).where(Topic.tid == tid, Topic.for_deletion == False)  # noqa: E712
        with self.db.tenant_session(tenant_schema) as session:
            return cast(Optional[Topic], session.exec(stmt).first())

    def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]:
        """Return a single Topic by its `topic_id` (or None)."""
        stmt = (
            select(Topic)
            .where(Topic.topic_id == topic_id, Topic.for_deletion == False)  # noqa: E712
            .order_by(Topic.sid.desc())  # type: ignore[attr-defined]
        )
        with self.db.tenant_session(tenant_schema) as session:
            return cast(Optional[Topic], session.exec(stmt).first())

    def get_sources_for_topic(
        self, tenant_schema: str, tid: int
    ) -> List[Tuple[Topic2Source, Source]]:
        """Return (Topic2Source, Source) pairs for the given topic tid."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(Topic2Source, Source)
                .join(Source, Topic2Source.soid == Source.soid)  # type: ignore[arg-type]
                .where(Topic2Source.tid == tid)
            )
            return [(row[0], row[1]) for row in session.exec(stmt).all()]

    def update_status(self, tenant_schema: str, tid: int, status_id: int) -> bool:
        """Update topic_status for the given tid. Returns True if the row existed."""
        with self.db.tenant_session(tenant_schema) as session:
            topic = session.get(Topic, tid)
            if topic is None:
                return False
            topic.topic_status = status_id
            session.add(topic)
            session.commit()
            return True

    def get_topic2drivers_with_driver(
        self, tenant_schema: str, tid: int
    ) -> List[Tuple[Topic2Driver, Driver]]:
        """Return (Topic2Driver, Driver) pairs for the given topic tid."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(Topic2Driver, Driver)
                .join(Driver, Topic2Driver.did == Driver.did)  # type: ignore[arg-type]
                .where(Topic2Driver.tid == tid)
            )
            return [(row[0], row[1]) for row in session.exec(stmt).all()]

    def get_maturity_scores_for_topic(self, tenant_schema: str, tid: int) -> List[MaturityScore]:
        """Return all MaturityScore rows for the given topic tid."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScore).where(MaturityScore.topic_id == tid)
            return list(session.exec(stmt).all())

    def get_maturity_score_sources_for_ids(
        self, tenant_schema: str, score_ids: List[int]
    ) -> List[MaturityScoreSource]:
        """Return MaturityScoreSource rows for the given score ids."""
        if not score_ids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScoreSource).where(
                MaturityScoreSource.maturity_score_id.in_(score_ids)  # type: ignore[attr-defined]
            )
            return list(session.exec(stmt).all())

    def get_maturity_score_deltas_for_topic(
        self, tenant_schema: str, sow_sid: int, topic_id: str
    ) -> List[MaturityScoreDelta]:
        """Return MaturityScoreDelta rows for the given topic (trend_id=null)."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScoreDelta).where(
                MaturityScoreDelta.sow_id == sow_sid,
                MaturityScoreDelta.topic_id == topic_id,
                MaturityScoreDelta.trend_id == None,  # noqa: E711
                MaturityScoreDelta.for_deletion == False,  # noqa: E712
            )
            return list(session.exec(stmt).all())

    def get_trend_by_ssid(self, tenant_schema: str, ssid: int) -> Optional[Trend]:
        """Return the Trend for the given ssid (or None)."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Trend).where(
                Trend.ssid == ssid,
                Trend.for_deletion == False,  # noqa: E712
            )
            return cast(Optional[Trend], session.exec(stmt).first())

    def get_maturity_scores_for_trend(self, tenant_schema: str, ssid: int) -> List[MaturityScore]:
        """Return all MaturityScore rows for the given trend ssid."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScore).where(MaturityScore.trend_id == ssid)
            return list(session.exec(stmt).all())

    def get_maturity_score_deltas_for_trend(
        self, tenant_schema: str, sow_sid: int, trend_id: str
    ) -> List[MaturityScoreDelta]:
        """Return MaturityScoreDelta rows for the given trend (topic_id=null)."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(MaturityScoreDelta).where(
                MaturityScoreDelta.sow_id == sow_sid,
                MaturityScoreDelta.trend_id == trend_id,
                MaturityScoreDelta.topic_id == None,  # noqa: E711
                MaturityScoreDelta.for_deletion == False,  # noqa: E712
            )
            return list(session.exec(stmt).all())


__all__ = ["TopicRepository"]
