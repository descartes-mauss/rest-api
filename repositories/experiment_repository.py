"""Repository for the Experiment model (public schema)."""

from typing import Optional

from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.public_models.models import Experiment


class ExperimentRepository:
    """Repository for public-schema Experiment rows."""

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    def get_by_id(self, experiment_id: int) -> Optional[Experiment]:
        """Return the Experiment with the given primary key, or None."""
        with self.db.session() as session:
            stmt = select(Experiment).where(Experiment.experiment_id == experiment_id)
            return session.exec(stmt).first()  # type: ignore[no-any-return]


__all__ = ["ExperimentRepository"]
