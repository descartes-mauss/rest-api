from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel

from database.schemas.maturity import MaturityScoreDeltaSchema, MaturityScoreSchema
from database.schemas.trend import TrendSchema


class TopicInOpportunitySchema(BaseModel):
    """Topic as serialized inside an Opportunity.

    Mirrors Django's TopicSerializer (excludes metric/sizing fields).
    The nested `trend` uses TrendSchema (which already carries
    related_topics as UnlinkedTopicSchema to avoid circular references).
    """

    tid: Optional[int] = None
    sow: int
    load_date: datetime
    topic_id: str
    topic_name: str
    topic_status: int
    topic_description: Optional[str] = None
    topic_image_s3_uri: Optional[str] = None
    masterfile_version: int
    for_deletion: bool = False
    new_discovery: bool = False
    type: str = "Topic"
    trend: Optional[TrendSchema] = None
    driver: List[int] = []
    driver_count: Optional[int] = None
    maturity_scores: List[MaturityScoreSchema] = []
    maturity_scores_deltas: List[MaturityScoreDeltaSchema] = []
    global_maturity_score: Optional[MaturityScoreSchema] = None
    global_maturity_score_delta: Optional[MaturityScoreDeltaSchema] = None


class OpportunitySchema(BaseModel):
    oid: Optional[int] = None
    sow: int
    opportunity_name: str
    opportunity: Optional[int]
    masterfile_version: int
    for_deletion: bool = False
    topics: List[TopicInOpportunitySchema] = []
    topic_ids: List[str] = []
