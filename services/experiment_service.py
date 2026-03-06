"""Service layer for the experiments endpoint."""

from typing import Optional

from database.public_models.models import Experiment
from repositories.protocols import ExperimentRepositoryProtocol


class ExperimentService:
    """Service layer for Experiment-related business logic."""

    def __init__(self, experiment_repository: ExperimentRepositoryProtocol) -> None:
        self.experiment_repository = experiment_repository

    def get_experiment(self, experiment_id: int) -> Optional[Experiment]:
        """Return the Experiment by its primary key, or None."""
        return self.experiment_repository.get_by_id(experiment_id)


__all__ = ["ExperimentService"]
