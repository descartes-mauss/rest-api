from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import JSON, Column
from sqlmodel import Field, SQLModel

from database.tenant_models.enums import ExperimentType


class TenantSow(SQLModel, table=True):
    __tablename__ = "client_interface_sowmodel"

    sid: Optional[int] = Field(default=None, primary_key=True)
    load_date: datetime
    sow_name: str
    sow_status: str
    sow_description: Optional[str] = None
    cs_sow_id: Optional[str] = None
    masterfile_version: int
    for_deletion: bool = False


class Driver(SQLModel, table=True):
    __tablename__ = "client_interface_drivermodel"

    did: Optional[int] = Field(default=None, primary_key=True)
    sow_sid: int = Field(foreign_key="client_interface_sowmodel.sid")
    load_date: datetime
    driver_id: str
    driver_name: str
    driver_description: Optional[str] = None
    masterfile_version: int
    for_deletion: bool = False


class Trend(SQLModel, table=True):
    __tablename__ = "client_interface_trendmodel"

    ssid: Optional[int] = Field(default=None, primary_key=True)
    sid: int = Field(foreign_key="client_interface_sowmodel.sid")
    load_date: datetime
    trend_id: str
    trend_name: str
    trend_description: Optional[str] = None
    shift_id: str
    shift_name: str
    shift_description: Optional[str] = None
    trend_image_s3_uri: Optional[str] = None
    trend_industry_sizing: Optional[float] = None
    trend_society_sizing: Optional[float] = None
    trend_consumer_sizing: Optional[float] = None
    trend_average_sizing: Optional[float] = None
    trend_average_sizing_label: str = ""
    trend_timeline: Optional[float] = None
    trend_timeline_label: str = ""
    trend_growth: Optional[float] = None
    trend_growth_normalized: Optional[float] = None
    trend_consensus: Optional[float] = None
    trend_consensus_normalized: Optional[float] = None
    trend_consensus_label: str = ""
    trend_consensus_icon_uri: Optional[str] = None
    trend_industry_impact: Optional[float] = None
    trend_industry_impact_label: str = ""
    trend_industry_impact_icon_uri: Optional[str] = None
    masterfile_version: int
    for_deletion: bool = False
    new_discovery: bool = False


class TrendDelta(SQLModel, table=True):
    __tablename__ = "client_interface_trenddeltamodel"

    ssdeltaid: Optional[int] = Field(default=None, primary_key=True)
    sow_sid: int = Field(foreign_key="client_interface_sowmodel.sid")
    load_date: datetime = Field(default_factory=datetime.utcnow)
    trend_id: str
    trend_average_sizing_delta: float
    trend_growth_delta: float
    trend_consensus_delta: float


class Topic(SQLModel, table=True):
    __tablename__ = "client_interface_topicmodel"

    tid: Optional[int] = Field(default=None, primary_key=True)
    sid: int = Field(foreign_key="client_interface_sowmodel.sid")
    load_date: datetime
    topic_id: str
    topic_name: str
    topic_status: int
    topic_description: Optional[str] = None
    topic_image_s3_uri: Optional[str] = None
    ssid: Optional[int] = Field(default=None, foreign_key="client_interface_trendmodel.ssid")
    # Many-to-many driver relation represented as JSON list of driver ids
    # driver_ids: Optional[List[int]] = Field(default=None, sa_column=Column(JSON))
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


class TopicDelta(SQLModel, table=True):
    __tablename__ = "client_interface_topicdeltamodel"

    tdeltaid: Optional[int] = Field(default=None, primary_key=True)
    sow_sid: int = Field(foreign_key="client_interface_sowmodel.sid")
    load_date: datetime = Field(default_factory=datetime.utcnow)
    topic_id: str
    topic_average_sizing_delta: float
    topic_growth_delta: float
    topic_consensus_delta: float


class Topic2Driver(SQLModel, table=True):
    __tablename__ = "client_interface_topic2drivermodel"

    tdid: Optional[int] = Field(default=None, primary_key=True)
    topic_tid: int = Field(foreign_key="client_interface_topicmodel.tid")
    driver_did: int = Field(foreign_key="client_interface_drivermodel.did")
    strength: Optional[float] = None
    polarity: Optional[float] = None
    cooccurrence: Optional[int] = None


class Opportunity(SQLModel, table=True):
    __tablename__ = "client_interface_opportunitymodel"

    oid: Optional[int] = Field(default=None, primary_key=True)
    sow_sid: int = Field(foreign_key="client_interface_sowmodel.sid")
    opportunity_name: str
    opportunity: int
    # Topic soft-links as JSON list of topic ids
    topic_ids: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    masterfile_version: int
    for_deletion: bool = False


class Topic2Opportunity(SQLModel, table=True):
    __tablename__ = "client_interface_topic2opportunitymodel"

    toid: Optional[int] = Field(default=None, primary_key=True)
    topic_tid: int = Field(foreign_key="client_interface_topicmodel.tid")
    opportunity_oid: int = Field(foreign_key="client_interface_opportunitymodel.oid")


class Source(SQLModel, table=True):
    __tablename__ = "client_interface_sourcemodel"

    soid: Optional[int] = Field(default=None, primary_key=True)
    sow_sid: int = Field(foreign_key="client_interface_sowmodel.sid")
    source_url: str
    source_title: str
    internal_classification: Optional[str] = None
    topic_ids: Optional[List[int]] = Field(default=None, sa_column=Column(JSON))
    load_date: datetime
    masterfile_version: int
    for_deletion: bool = False


class Topic2Source(SQLModel, table=True):
    __tablename__ = "client_interface_topic2sourcemodel"

    tsid: Optional[int] = Field(default=None, primary_key=True)
    topic_tid: int = Field(foreign_key="client_interface_topicmodel.tid")
    source_soid: int = Field(foreign_key="client_interface_sourcemodel.soid")


class Experiment(SQLModel, table=True):
    __tablename__ = "client_interface_experiment"

    eid: Optional[int] = Field(default=None, primary_key=True)
    experiment_id: int
    sow_sid: int = Field(foreign_key="client_interface_sowmodel.sid")
    experiment_name: str
    experiment_url: str
    experiment_type: ExperimentType = ExperimentType.EXPERIMENT
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Insight(SQLModel, table=True):
    __tablename__ = "client_interface_insightmodel"

    id: Optional[int] = Field(default=None, primary_key=True)
    entity_id: str
    entity_type: str
    insight_title: str
    insight_description: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    cs_sow_id: Optional[str] = None
    # index on created_at can be applied via migrations on DB side if needed


class InsightSource(SQLModel, table=True):
    __tablename__ = "client_interface_insightsourcemodel"

    id: Optional[int] = Field(default=None, primary_key=True)
    # Many-to-many to insights represented as JSON list of insight ids
    insight_ids: Optional[List[int]] = Field(default=None, sa_column=Column(JSON))
    source_url: str
    source_title: str
    source_favicon: Optional[str] = None


class RawGrowthOpportunity(SQLModel, table=True):
    __tablename__ = "client_interface_rawgrowthopportunity"

    gsid: Optional[int] = Field(default=None, primary_key=True)
    old_growth_space_name: str
    reformulated_growth_space_name: str
    old_rationale: str
    reformulated_rationale: str
    customer_need: str
    evidence_score: int
    growth_opportunity_id: str
    topics: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class GrowthOpportunity(SQLModel, table=True):
    __tablename__ = "client_interface_growthopportunitymodel"

    goid: Optional[int] = Field(default=None, primary_key=True)
    load_date: datetime
    name: str
    market: str
    category: str
    customer_segment: str
    customer_segment_description: str
    customer_segment_occasion: str
    granular_consumer_need: str
    potential_customers: int
    potential_customers_step_by_step_reasoning: Optional[str] = None
    web_content_customer_estimator: Optional[List[str]] = Field(
        default=None, sa_column=Column(JSON)
    )
    avr_annual_spend: Decimal
    avr_annual_spend_step_by_step_reasoning: Optional[str] = None
    web_content_spend_estimator: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    customer_segment_market_size: Decimal
    growth_opportunity_id: str
    rating: Optional[int] = None
    description: str
    customer_need: str
    topic_ids: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    geography_id: str
    geography_name: str
    currency: str = "USD"
    growth_space_market_size: Decimal
    market_level_market_size: Decimal
    for_deletion: bool = False


class GrowthOpportunity2Topic(SQLModel, table=True):
    __tablename__ = "client_interface_growthopportunity2topic"

    id: Optional[int] = Field(default=None, primary_key=True)
    growth_opportunity_id: str
    topic_id: str


class GrowthOpportunitySource(SQLModel, table=True):
    __tablename__ = "client_interface_growthopportunitysource"

    gosid: Optional[int] = Field(default=None, primary_key=True)
    source_url: str
    source_title: str
    growth_opportunity_ids: Optional[List[int]] = Field(default=None, sa_column=Column(JSON))
    load_date: datetime
    for_deletion: bool = False


class GrowthOpportunity2Source(SQLModel, table=True):
    __tablename__ = "client_interface_growthopportunity2source"

    gsid: Optional[int] = Field(default=None, primary_key=True)
    growth_opportunity_goid: Optional[int] = Field(
        default=None, foreign_key="client_interface_growthopportunitymodel.goid"
    )
    growth_opportunity_source_gosid: Optional[int] = Field(
        default=None, foreign_key="client_interface_growthopportunitysource.gosid"
    )
