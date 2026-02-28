from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID, uuid4

from sqlmodel import JSON, Column, Field, SQLModel

from database.public_models.enums import DatasetProcessStatus, ExperimentType, Industry


class PublicModel(SQLModel):
    """Base SQLModel that enforces the 'public' database schema.

    Intended to be subclassed by application models so they inherit the table
    configuration placing tables in the "public" schema.

    Usage:
        class MyModel(PublicModel, table=True):
            id: Optional[int] = Field(default=None, primary_key=True)
            ...

    Attributes:
        __table_args__ (dict): Table options set to {"schema": "public"}.
    """

    __table_args__ = {"schema": "public"}


class ServiceTier(SQLModel, table=True):
    __tablename__ = "cs_interface_service_tier"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    name: str
    description: Optional[str] = None
    created_on: datetime = Field(default_factory=datetime.utcnow)
    updated_on: datetime = Field(default_factory=datetime.utcnow)


class ServiceFeature(SQLModel, table=True):
    __tablename__ = "cs_interface_service_feature"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    name: str
    description: Optional[str] = None
    created_on: datetime = Field(default_factory=datetime.utcnow)
    updated_on: datetime = Field(default_factory=datetime.utcnow)


class TierFeature(SQLModel, table=True):
    __tablename__ = "cs_interface_tier_feature"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    tier_id: int = Field(foreign_key="service_tier.id")
    feature_id: int = Field(foreign_key="service_feature.id")


class Geography(SQLModel, table=True):
    __tablename__ = "cs_interface_geography"
    __table_args__ = {"schema": "public"}

    geography_id: str = Field(primary_key=True)
    name: str
    is_active: bool = False
    is_region: bool = False
    created_on: datetime = Field(default_factory=datetime.utcnow)
    updated_on: datetime = Field(default_factory=datetime.utcnow)


class Client(SQLModel, table=True):
    __tablename__ = "cs_interface_client"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    company: str = Field(default="D&M")
    customer_id: str
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    hidden: bool = False
    industry: Optional[Industry] = Field(default=None)
    # industry: Optional[str] = Field(default=None)
    tier_id: Optional[int] = Field(default=None, foreign_key="service_tier.id")
    website: Optional[str] = None
    company_profile_image_s3_uri: Optional[str] = None


class ClientGeography(SQLModel, table=True):
    __tablename__ = "cs_interface_client_geography"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    client_id: int = Field(foreign_key="client.id")
    geography_id: str = Field(foreign_key="geography.geography_id")
    business_category: Optional[str] = None


class PublicSow(SQLModel, table=True):
    __tablename__ = "cs_interface_sow"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    client_id: int = Field(foreign_key="client.id")
    name: str
    description: Optional[str] = None
    sow_id: Optional[str] = None
    sow_status: str = Field(default="disabled")
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    hidden: bool = False
    feed_group_id: Optional[int] = None
    geography_id: Optional[str] = Field(default=None, foreign_key="geography.geography_id")
    tier_id: Optional[int] = Field(default=None, foreign_key="service_tier.id")


class Masterfile(SQLModel, table=True):
    __tablename__ = "cs_interface_masterfile"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    sow_id: int = Field(foreign_key="sow.id")
    version: int
    label: str
    status: str = Field(default="disabled")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Experiment(SQLModel, table=True):
    __tablename__ = "cs_interface_experiment"
    __table_args__ = {"schema": "public"}

    experiment_id: Optional[int] = Field(default=None, primary_key=True)
    sow_id: int = Field(foreign_key="sow.id")
    experiment_name: str
    experiment_url: str
    experiment_type: ExperimentType = ExperimentType.EXPERIMENT
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Dataset(SQLModel, table=True):
    __tablename__ = "cs_interface_dataset"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    program_id: int = Field(foreign_key="sow.id")
    name: str
    workflow_config_id: Optional[str] = None
    workflow_timestamp: Optional[str] = None
    cs_contact_emails: Optional[str] = None
    dataset_description: Optional[str] = None
    query_type: Optional[str] = None
    industry: Optional[str] = None
    sourcing_status: DatasetProcessStatus = DatasetProcessStatus.NOT_STARTED
    scraping_status: DatasetProcessStatus = DatasetProcessStatus.NOT_STARTED
    cleaning_translation_status: DatasetProcessStatus = DatasetProcessStatus.NOT_STARTED
    cleaning_translation_output: Optional[str] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    hidden: bool = False


class GrowthOpportunityJobExecution(SQLModel, table=True):
    __tablename__ = "cs_interface_growth_opportunity_job_execution"
    __table_args__ = {"schema": "public"}

    job_id: UUID = Field(default_factory=uuid4, primary_key=True)
    client_id: int = Field(foreign_key="client.id")
    processing_status: DatasetProcessStatus = DatasetProcessStatus.NOT_STARTED
    steps_status: Optional[List[dict[str, Any]]] = Field(default=None, sa_column=Column(JSON))
    geographies: Optional[str] = None
    categories: Optional[str] = None
    topics: Optional[str] = None
    industry: str = Field(default="Other")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class CompanyProfileFetcherJobExecution(SQLModel, table=True):
    __tablename__ = "cs_interface_company_profile_fetcher_job_execution"
    __table_args__ = {"schema": "public"}

    job_id: UUID = Field(default_factory=uuid4, primary_key=True)
    client_id: int = Field(foreign_key="client.id")
    processing_status: DatasetProcessStatus = DatasetProcessStatus.NOT_STARTED
    steps_status: Optional[List[dict[str, Any]]] = Field(default=None, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class AnalysisParameters(SQLModel, table=True):
    __tablename__ = "cs_interface_analysis_parameters"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    threshold_in_percentage: int = Field(default=100)
    aggregation_type: str = Field(default="sum")


class GrowthMetric(SQLModel, table=True):
    __tablename__ = "cs_interface_growth_metric"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    program_id: int = Field(foreign_key="sow.id")
    iteration: int = Field(default=0)
    iteration_name: Optional[str] = None
    param_yaml_path: str
    input_xlsx_path: str
    output_filepath: str
    completed: bool = False
    status: str = Field(default="queued")


class CIClient(SQLModel, table=True):
    """client_interface.Client — the django-tenants TenantMixin model stored in the public schema.

    Used to resolve a user's org_id to their PostgreSQL schema_name.
    Distinct from cs_interface.Client (the `Client` model above).
    """

    __tablename__ = "client_interface_client"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    schema_name: str
    name: str
    website: Optional[str] = None
    created_on: Optional[datetime] = None
    org_id: str
    client_status: str = "processing"
    industry: Optional[Industry] = None
    display_opportunity_map: bool = True


class ClientCompanyProfile(SQLModel, table=True):
    """Enriched company profile for a tenant, stored in the public schema."""

    __tablename__ = "cs_interface_clientcompanyprofile"
    __table_args__ = {"schema": "public"}

    id: Optional[int] = Field(default=None, primary_key=True)
    client_id: int = Field(foreign_key="client_interface_client.id")
    company_name: Optional[str] = None
    brands_count: Optional[int] = None
    company_purpose: Optional[str] = None
    company_purpose_implication: Optional[str] = None
    company_vision: Optional[str] = None
    company_vision_implication: Optional[str] = None
    company_mission: Optional[str] = None
    company_mission_implication: Optional[str] = None
    company_personality: Optional[List[Any]] = Field(default=None, sa_column=Column(JSON))
    company_strategic_priorities: Optional[str] = None
    company_competitors: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
