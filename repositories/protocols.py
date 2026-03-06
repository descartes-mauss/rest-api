"""Repository Protocol interfaces.

Each Protocol mirrors the public interface of its concrete repository class.
Services type-hint against these Protocols so that test fakes only need to
implement the methods they actually exercise, and mypy can verify conformance
without coupling services to concrete repository implementations (DIP).
"""

from typing import Dict, List, Optional, Protocol, Tuple
from uuid import UUID

from database.public_models.models import (
    CIClient,
    ClientCompanyProfile,
    ClientGeography,
    Experiment,
    Geography,
    PublicSow,
)
from database.tenant_models.models import (
    Brand,
    BusinessCategory,
    CustomerSegment,
    Driver,
    Insight,
    InsightSource,
    MaturityScore,
    MaturityScoreDelta,
    MaturityScoreSource,
    Opportunity,
    ProductLine,
    Source,
    TenantSow,
    TenantUser,
    Topic,
    Topic2Driver,
    Topic2Opportunity,
    Topic2Source,
    Trend,
)


class SowRepositoryProtocol(Protocol):
    """Interface satisfied by SowRepository (and test fakes)."""

    def get_latest_live_sows(self, tenant_schema: str) -> List[TenantSow]: ...

    def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]: ...

    def get_latest_live_sow_for_cs_sow_id(
        self, tenant_schema: str, cs_sow_id: str
    ) -> Optional[TenantSow]: ...

    def get_sow_versions(self, tenant_schema: str, cs_sow_id: str) -> List[TenantSow]: ...

    def get_trends_for_sow(
        self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
    ) -> List[Trend]: ...

    def get_maturity_scores_for_trend_ids(
        self, tenant_schema: str, trend_ssids: List[int]
    ) -> List[MaturityScore]: ...

    def get_maturity_score_sources(
        self, tenant_schema: str, score_ids: List[int]
    ) -> List[MaturityScoreSource]: ...

    def get_maturity_score_deltas_for_sow_trends(
        self, tenant_schema: str, sow_sid: int, trend_id_strings: List[str]
    ) -> List[MaturityScoreDelta]: ...

    def get_topics_for_sow(
        self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
    ) -> List[Topic]: ...

    def get_topics_for_trends(self, tenant_schema: str, trend_ssids: List[int]) -> List[Topic]: ...

    def get_drivers_for_sow(
        self, tenant_schema: str, sow_sid: int, name_order: Optional[str] = None
    ) -> List[Driver]: ...

    def get_topic_counts_for_drivers(
        self, tenant_schema: str, driver_dids: List[int]
    ) -> Dict[int, int]: ...

    def get_opportunities_for_sow(self, tenant_schema: str, sow_sid: int) -> List[Opportunity]: ...

    def get_topic2opportunity_rows(
        self, tenant_schema: str, opp_oids: List[int]
    ) -> List[Topic2Opportunity]: ...

    def get_topics_by_ids(self, tenant_schema: str, topic_tids: List[int]) -> List[Topic]: ...

    def get_maturity_scores_for_topic_ids(
        self, tenant_schema: str, topic_tids: List[int]
    ) -> List[MaturityScore]: ...

    def get_maturity_score_deltas_for_sow_topic_ids(
        self, tenant_schema: str, sow_sid: int, topic_id_strings: List[str]
    ) -> List[MaturityScoreDelta]: ...

    def get_topic_drivers_by_topic_ids(
        self, tenant_schema: str, topic_tids: List[int]
    ) -> List[Topic2Driver]: ...

    def get_trend_by_trend_id(self, tenant_schema: str, trend_id: str) -> Optional[Trend]: ...

    def get_trends_by_ssids(self, tenant_schema: str, trend_ssids: List[int]) -> List[Trend]: ...

    def get_insights_for_cs_sow_id(
        self, tenant_schema: str, cs_sow_id: str, offset: int, limit: int
    ) -> Tuple[int, List[Insight]]: ...

    def get_insights_filtered(
        self,
        tenant_schema: str,
        cs_sow_id: str,
        entity_ids: List[str],
        offset: int,
        limit: int,
    ) -> Tuple[int, List[Insight]]: ...

    def get_insight_sources_for_insight_ids(
        self, tenant_schema: str, insight_ids: List[int]
    ) -> List[Tuple[InsightSource, int]]: ...

    def get_topics_by_topic_str_ids(
        self, tenant_schema: str, sow_sid: int, topic_ids: List[str]
    ) -> List[Topic]: ...

    def get_trends_by_trend_str_ids(
        self, tenant_schema: str, sow_sid: int, trend_ids: List[str]
    ) -> List[Trend]: ...

    def get_sow_geographies(
        self, cs_sow_ids: List[str]
    ) -> List[Tuple[PublicSow, Optional[Geography]]]: ...


class TopicRepositoryProtocol(Protocol):
    """Interface satisfied by TopicRepository (and test fakes)."""

    def get_all(self, tenant_schema: str) -> List[Topic]: ...

    def get_by_topic_id(self, tenant_schema: str, topic_id: str) -> Optional[Topic]: ...

    def get_sources_for_topic(
        self, tenant_schema: str, tid: int
    ) -> List[Tuple[Topic2Source, Source]]: ...

    def update_status(self, tenant_schema: str, tid: int, status_id: int) -> bool: ...

    def get_topic2drivers_with_driver(
        self, tenant_schema: str, tid: int
    ) -> List[Tuple[Topic2Driver, Driver]]: ...

    def get_maturity_scores_for_topic(
        self, tenant_schema: str, tid: int
    ) -> List[MaturityScore]: ...

    def get_maturity_score_deltas_for_topic(
        self, tenant_schema: str, sow_sid: int, topic_id: str
    ) -> List[MaturityScoreDelta]: ...


class BrandRepositoryProtocol(Protocol):
    """Interface satisfied by BrandRepository (and test fakes)."""

    def get_brands(self, tenant_schema: str) -> List[Brand]: ...

    def get_brand_by_id(self, tenant_schema: str, brand_id: int) -> Optional[Brand]: ...

    def get_product_lines_by_brand_ids(
        self, tenant_schema: str, brand_ids: List[int]
    ) -> List[ProductLine]: ...

    def get_business_categories_by_ids(
        self, tenant_schema: str, category_ids: List[int]
    ) -> List[BusinessCategory]: ...


class CompanyRepositoryProtocol(Protocol):
    """Interface satisfied by CompanyRepository (and test fakes)."""

    def get_ci_client(self, org_id: str) -> Optional[CIClient]: ...

    def get_cs_client_image(self, org_id: str) -> Optional[str]: ...

    def get_company_profile(self, ci_client_id: int) -> Optional[ClientCompanyProfile]: ...

    def get_customer_segments(self, tenant_schema: str) -> List[CustomerSegment]: ...

    def get_all_business_categories(self, tenant_schema: str) -> List[BusinessCategory]: ...


class GeographyRepositoryProtocol(Protocol):
    """Interface satisfied by GeographyRepository (and test fakes)."""

    def get_client_id(self, org_id: str) -> Optional[int]: ...

    def get_active_geographies(self, client_id: int) -> List[Tuple[ClientGeography, Geography]]: ...


class TenantUserRepositoryProtocol(Protocol):
    """Interface satisfied by TenantUserRepository (and test fakes)."""

    def get_all(self, tenant_schema: str) -> List[TenantUser]: ...

    def get_by_id(self, tenant_schema: str, user_id: UUID) -> Optional[TenantUser]: ...

    def create(self, tenant_schema: str, user: TenantUser) -> TenantUser: ...

    def update(self, tenant_schema: str, user: TenantUser) -> TenantUser: ...

    def delete(self, tenant_schema: str, user_id: UUID) -> bool: ...


class ExperimentRepositoryProtocol(Protocol):
    """Interface satisfied by ExperimentRepository (and test fakes)."""

    def get_by_id(self, experiment_id: int) -> Optional[Experiment]: ...


class PermissionsRepositoryProtocol(Protocol):
    """Interface satisfied by PermissionsRepository (and test fakes)."""

    def get_sow_by_id(self, tenant_schema: str, sow_id: int) -> Optional[TenantSow]: ...

    def get_client_tier_id(self, org_id: str) -> Optional[int]: ...

    def get_feature_codes(self, tier_id: int) -> List[str]: ...

    def get_experiments(self, cs_sow_id: Optional[str]) -> List[Experiment]: ...

    def has_opportunity_platforms(self, tenant_schema: str, sow_sid: int) -> bool: ...
