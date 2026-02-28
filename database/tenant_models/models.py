from datetime import datetime
from decimal import Decimal
from typing import Any, List, Optional

from sqlalchemy import JSON, Column
from sqlmodel import Field, SQLModel

from database.tenant_models.enums import ExperimentType, MaturityCategory


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


# ---------------------------------------------------------------------------
# Maturity score models
# ---------------------------------------------------------------------------


class MaturityScore(SQLModel, table=True):
    __tablename__ = "client_interface_maturityscore"

    id: Optional[int] = Field(default=None, primary_key=True)
    topic_id: Optional[int] = Field(default=None, foreign_key="client_interface_topicmodel.tid")
    trend_id: Optional[int] = Field(default=None, foreign_key="client_interface_trendmodel.ssid")
    category: MaturityCategory
    score: Optional[Decimal] = None
    threshold: Optional[str] = None
    rationale: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class MaturityScoreSource(SQLModel, table=True):
    __tablename__ = "client_interface_maturityscoresource"

    id: Optional[int] = Field(default=None, primary_key=True)
    maturity_score_id: int = Field(foreign_key="client_interface_maturityscore.id")
    source_url: str
    source_title: str = ""


class MaturityScoreDelta(SQLModel, table=True):
    __tablename__ = "client_interface_maturityscoredelta"

    id: Optional[int] = Field(default=None, primary_key=True)
    sow_id: int = Field(foreign_key="client_interface_sowmodel.sid")
    topic_id: Optional[str] = None
    trend_id: Optional[str] = None
    category: MaturityCategory
    absolute_delta: Decimal
    percentage_delta: Decimal
    label: str
    masterfile_version: int
    for_deletion: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Company models
# ---------------------------------------------------------------------------


class BusinessCategory(SQLModel, table=True):
    __tablename__ = "client_interface_businesscategory"

    id: Optional[int] = Field(default=None, primary_key=True)
    business_category_name: str
    business_category_description: Optional[str] = None


class CustomerSegment(SQLModel, table=True):
    __tablename__ = "client_interface_customersegment"

    id: Optional[int] = Field(default=None, primary_key=True)
    customer_segment_name: str
    customer_segment_description: Optional[str] = None


class Brand(SQLModel, table=True):
    __tablename__ = "client_interface_brand"

    id: Optional[int] = Field(default=None, primary_key=True)
    brand_name: str
    brand_description: Optional[str] = None
    brand_purpose: Optional[str] = None
    brand_mission: Optional[str] = None
    brand_attributes: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    brand_business_category_id: Optional[int] = Field(
        default=None, foreign_key="client_interface_businesscategory.id"
    )
    brand_country: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))


class ProductLine(SQLModel, table=True):
    __tablename__ = "client_interface_productline"

    id: Optional[int] = Field(default=None, primary_key=True)
    brand_id: int = Field(foreign_key="client_interface_brand.id")
    product_line_name: str
    product_line_user_benefit: Optional[str] = None
    product_line_value_proposition: Optional[str] = None


class BrandStrategicFitScore(SQLModel, table=True):
    __tablename__ = "client_interface_brandstrategicfitscore"

    id: Optional[int] = Field(default=None, primary_key=True)
    strategic_fit_score: Optional[float] = None
    growth_opportunity_id: Optional[str] = None
    growth_opportunity_geography: Optional[str] = None
    brand_id: int = Field(foreign_key="client_interface_brand.id")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    for_deletion: bool = False


# ---------------------------------------------------------------------------
# New growth opportunity models (hierarchical schema)
# ---------------------------------------------------------------------------


class BaseGrowthOpportunity(SQLModel, table=True):
    __tablename__ = "client_interface_basegrowthopportunitymodel"

    goid: Optional[int] = Field(default=None, primary_key=True)
    load_date: datetime
    growth_opportunity_id: str
    rating: Optional[int] = None
    currency: str = "USD"
    for_deletion: bool = False


class GrowthOpportunityTopics(SQLModel, table=True):
    __tablename__ = "client_interface_growthopportunitytopicsmodel"

    id: Optional[int] = Field(default=None, primary_key=True)
    goid: int = Field(foreign_key="client_interface_basegrowthopportunitymodel.goid")
    topic_id: str
    topic_name: str


class GeographyGrowthOpportunity(SQLModel, table=True):
    __tablename__ = "client_interface_geographygrowthopportunitymodel"

    ggoid: Optional[int] = Field(default=None, primary_key=True)
    goid: int = Field(foreign_key="client_interface_basegrowthopportunitymodel.goid")
    name: str
    geography_id: str
    geography_name: str
    description: str
    customer_need: str
    growth_space_market_size: Decimal
    strategic_fit_score: Optional[float] = None
    ranking_index: Optional[float] = None


class Market(SQLModel, table=True):
    __tablename__ = "client_interface_marketmodel"

    mid: Optional[int] = Field(default=None, primary_key=True)
    ggoid: int = Field(foreign_key="client_interface_geographygrowthopportunitymodel.ggoid")
    market: str
    market_level_market_size: Decimal


class Estimator(SQLModel, table=True):
    __tablename__ = "client_interface_estimatormodel"

    eid: Optional[int] = Field(default=None, primary_key=True)
    mid: int = Field(foreign_key="client_interface_marketmodel.mid")
    category: str
    customer_segment: str
    customer_segment_description: str
    customer_segment_occasion: str
    granular_consumer_need: str
    potential_customers: int
    potential_customers_step_by_step_reasoning: Optional[str] = None
    web_content_customer_estimator: Optional[List[Any]] = Field(
        default=None, sa_column=Column(JSON)
    )
    avr_annual_spend: Decimal
    avr_annual_spend_step_by_step_reasoning: Optional[str] = None
    web_content_spend_estimator: Optional[List[Any]] = Field(default=None, sa_column=Column(JSON))
    customer_segment_market_size: Decimal
