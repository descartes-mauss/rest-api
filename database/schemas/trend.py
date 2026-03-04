from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel

from database.schemas.maturity import MaturityScoreDeltaSchema, MaturityScoreSchema
from database.schemas.topic import UnlinkedTopicSchema


class TrendSchema(BaseModel):
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
