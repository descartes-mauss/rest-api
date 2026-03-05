"""Pydantic schemas for the experiments endpoint."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ExperimentSchema(BaseModel):
    experiment_id: Optional[int] = None
    sow_id: int
    experiment_name: str
    experiment_url: str
    experiment_type: int
    created_at: datetime
    updated_at: datetime


__all__ = ["ExperimentSchema"]
