from datetime import datetime
from typing import Any, List

from pydantic import BaseModel


class InsightSourceSchema(BaseModel):
    source_url: str
    source_title: str
    source_favicon: str = ""


class InsightSchema(BaseModel):
    insight_title: str
    insight_description: str
    created_at: datetime
    sources: List[InsightSourceSchema] = []
    predictions: List[Any] = []


class ForesightSearchRequest(BaseModel):
    topic_ids: List[str] = []
    trend_ids: List[str] = []


class ForesightResponse(BaseModel):
    total: int
    limit: int
    hasNext: bool
    hasPrev: bool
    weeklyInsights: List[InsightSchema] = []
