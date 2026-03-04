from typing import List, Optional

from pydantic import BaseModel

from database.schemas.topic import TopicSchema


class OpportunitySchema(BaseModel):
    oid: Optional[int] = None
    sow: int
    opportunity_name: str
    opportunity: Optional[int]
    masterfile_version: int
    for_deletion: bool = False
    topics: List[TopicSchema] = []
    topic_ids: List[str] = []
