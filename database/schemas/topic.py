"""Pydantic models for topic-related API responses."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class TopicResponse(BaseModel):
    """Response DTO model for a topic item."""

    tid: Optional[int]
    sid: int
    load_date: datetime
    topic_id: str
    topic_name: str
    topic_status: int
    topic_description: Optional[str] = None
    topic_image_s3_uri: Optional[str] = None
    ssid: Optional[int] = None
    industry_sizing: Optional[float] = None
    society_sizing: Optional[float] = None
    consumer_sizing: Optional[float] = None
    average_sizing: Optional[float] = None
    average_sizing_label: str = ""
    timeline: Optional[float] = None
    timeline_display: Optional[float] = None
    timeline_label: str = ""
    topic_growth: Optional[float] = None
    topic_growth_normalized: Optional[float] = None
    topic_consensus: Optional[float] = None
    topic_consensus_normalized: Optional[float] = None
    topic_consensus_label: str = ""
    topic_consensus_icon_uri: Optional[str] = None
    industry_impact: Optional[float] = None
    industry_impact_display: Optional[float] = None
    industry_impact_label: str = ""
    industry_impact_icon_uri: Optional[str] = None
    action_required: str = ""
    masterfile_version: int
    for_deletion: bool = False
    new_discovery: bool = False


class TopicsListResponse(BaseModel):
    """Response model for listing topics."""

    topics: List[TopicResponse]


class TopicItemResponse(BaseModel):
    """Response model for a single topic item."""

    topic: TopicResponse
