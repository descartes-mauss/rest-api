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


class UnlinkedTopicSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    tid: Optional[int] = None
    topic_id: str
    topic_name: str
    topic_description: Optional[str] = None
    topic_status: int
    topic_image_s3_uri: Optional[str] = None
    for_deletion: bool = False
    new_discovery: bool = False


class TrendInShiftSchema(BaseModel):
    ssid: Optional[int] = None
    sow: int
    load_date: datetime
    trend_id: str
    trend_name: str
    trend_description: Optional[str] = None
    shift_id: str
    shift_name: str
    shift_description: Optional[str] = None
    trend_image_s3_uri: Optional[str] = None
    masterfile_version: int
    for_deletion: bool = False
    new_discovery: bool = False
    type: str = "Trend"
    maturity_scores: List[MaturityScoreSchema] = []
    maturity_scores_deltas: List[MaturityScoreDeltaSchema] = []
    global_maturity_score: Optional[MaturityScoreSchema] = None
    global_maturity_score_delta: Optional[MaturityScoreDeltaSchema] = None
    driver_count: Optional[int] = None
    related_topics: List[UnlinkedTopicSchema] = []


class ShiftSchema(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    trends: List[TrendInShiftSchema] = []
