"""Pydantic models for topic-related API responses."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from pydantic import BaseModel, ConfigDict

from database.schemas.maturity import MaturityScoreDeltaSchema, MaturityScoreSchema

if TYPE_CHECKING:
    from database.schemas.trend import TrendSchema


class UnlinkedTopicSchema(BaseModel):
    """Minimal topic representation used inside TrendSchema to avoid circular references."""

    model_config = ConfigDict(from_attributes=True)

    tid: Optional[int] = None
    topic_id: str
    topic_name: str
    topic_description: Optional[str] = None
    topic_status: int
    topic_image_s3_uri: Optional[str] = None
    for_deletion: bool = False
    new_discovery: bool = False


class TopicSchema(BaseModel):
    """Topic with full enrichment: maturity scores, drivers, and embedded trend."""

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


class TopicResponse(BaseModel):
    """Response DTO for the /topics endpoint (includes sizing metrics)."""

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


class TopicSourceSchema(BaseModel):
    """A single source linked to a topic."""

    id: int
    url: str
    title: str
    internal_classification: Optional[str] = None


class TopicSourcesResponse(BaseModel):
    """Response model for topic sources endpoint."""

    last_updated: Optional[str] = None
    topic_sources: List[TopicSourceSchema] = []


class UpdateTopicStatusRequest(BaseModel):
    """Request body for the topic status update endpoint."""

    status_id: int


class Topic2DriverDriverSchema(BaseModel):
    """Embedded driver info returned inside Topic2DriverSchema."""

    driver_name: str
    driver_description: Optional[str] = None


class Topic2DriverSchema(BaseModel):
    """Response schema for a single topic-driver relationship."""

    driver: Topic2DriverDriverSchema
    driver_influence: Optional[float] = None
