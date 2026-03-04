from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class MaturityScoreSourceSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source_url: str
    source_title: str


class MaturityScoreSchema(BaseModel):
    id: Optional[int] = None
    category: str
    score: Optional[float] = None
    threshold: Optional[str] = None
    rationale: str
    sources: List[MaturityScoreSourceSchema] = []


class MaturityScoreDeltaSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    category: str
    absolute_delta: float
    percentage_delta: float
    label: Optional[str] = None
    masterfile_version: int
    for_deletion: bool
    created_at: datetime
