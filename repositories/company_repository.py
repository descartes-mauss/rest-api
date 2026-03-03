"""Repository for the company endpoint.

Queries span two schemas:
  - Public schema  : CIClient, Client, ClientCompanyProfile
  - Tenant schema  : Brand, ProductLine, BusinessCategory, CustomerSegment
"""

from typing import List, Optional

from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.public_models.models import CIClient, Client, ClientCompanyProfile
from database.tenant_models.models import (
    Brand,
    BusinessCategory,
    CustomerSegment,
    ProductLine,
)


class CompanyRepository:
    """Repository for data required by the company endpoint."""

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    # ------------------------------------------------------------------
    # Public schema queries
    # ------------------------------------------------------------------

    def get_ci_client(self, org_id: str) -> Optional[CIClient]:
        """Return the client_interface.Client matching org_id."""
        with self.db.session() as session:
            stmt = select(CIClient).where(CIClient.org_id == org_id)
            return session.exec(stmt).first()  # type: ignore[no-any-return]

    def get_cs_client_image(self, org_id: str) -> Optional[str]:
        """Return company_profile_image_s3_uri from cs_interface.Client."""
        with self.db.session() as session:
            stmt = select(Client.company_profile_image_s3_uri).where(
                Client.customer_id == org_id.lower()
            )
            return session.exec(stmt).first()  # type: ignore[no-any-return]

    def get_company_profile(self, ci_client_id: int) -> Optional[ClientCompanyProfile]:
        """Return the most recent ClientCompanyProfile for the given client_interface client."""
        with self.db.session() as session:
            stmt = (
                select(ClientCompanyProfile)
                .where(ClientCompanyProfile.client_id == ci_client_id)
                .order_by(ClientCompanyProfile.created_at.desc())  # type: ignore[attr-defined]
                .limit(1)
            )
            return session.exec(stmt).first()  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Tenant schema queries
    # ------------------------------------------------------------------

    def get_brands(self, tenant_schema: str) -> List[Brand]:
        """Return all brands, distinct by brand_name, ordered alphabetically."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(Brand)
                .distinct(Brand.brand_name)  # type: ignore[arg-type]
                .order_by(Brand.brand_name)
            )
            return list(session.exec(stmt).all())

    def get_product_lines_by_brand_ids(
        self, tenant_schema: str, brand_ids: List[int]
    ) -> List[ProductLine]:
        """Return all ProductLine rows for the given brand IDs."""
        if not brand_ids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(ProductLine).where(
                ProductLine.brand_id.in_(brand_ids)  # type: ignore[attr-defined]
            )
            return list(session.exec(stmt).all())

    def get_business_categories_by_ids(
        self, tenant_schema: str, category_ids: List[int]
    ) -> List[BusinessCategory]:
        """Return BusinessCategory rows by ID (used to populate nested brand data)."""
        if not category_ids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(BusinessCategory).where(
                BusinessCategory.id.in_(category_ids)  # type: ignore[union-attr]
            )
            return list(session.exec(stmt).all())

    def get_customer_segments(self, tenant_schema: str) -> List[CustomerSegment]:
        """Return all customer segments, distinct by name, ordered alphabetically."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(CustomerSegment)
                .distinct(CustomerSegment.customer_segment_name)  # type: ignore[arg-type]
                .order_by(CustomerSegment.customer_segment_name)
            )
            return list(session.exec(stmt).all())

    def get_all_business_categories(self, tenant_schema: str) -> List[BusinessCategory]:
        """Return all business categories, distinct by name, ordered alphabetically."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(BusinessCategory)
                .distinct(BusinessCategory.business_category_name)  # type: ignore[arg-type]
                .order_by(BusinessCategory.business_category_name)
            )
            return list(session.exec(stmt).all())


__all__ = ["CompanyRepository"]
