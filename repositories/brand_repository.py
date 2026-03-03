"""Repository for the brands endpoints.

Fetches Brand, ProductLine, and BusinessCategory rows from the tenant schema.
"""

from typing import List, Optional

from sqlmodel import select

from database.db_session_provider import DBSessionProvider
from database.tenant_models.models import Brand, BusinessCategory, ProductLine


class BrandRepository:
    """Repository for data required by the brands endpoints."""

    def __init__(self, db_provider: DBSessionProvider) -> None:
        self.db = db_provider

    def get_brands(self, tenant_schema: str) -> List[Brand]:
        """Return all brands, distinct by brand_name, ordered alphabetically."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = (
                select(Brand)
                .distinct(Brand.brand_name)  # type: ignore[arg-type]
                .order_by(Brand.brand_name)
            )
            return list(session.exec(stmt).all())

    def get_brand_by_id(self, tenant_schema: str, brand_id: int) -> Optional[Brand]:
        """Return a single Brand by its PK, or None."""
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(Brand).where(Brand.id == brand_id)
            return session.exec(stmt).first()  # type: ignore[no-any-return]

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
        """Return all BusinessCategory rows matching the given IDs."""
        if not category_ids:
            return []
        with self.db.tenant_session(tenant_schema) as session:
            stmt = select(BusinessCategory).where(
                BusinessCategory.id.in_(category_ids)  # type: ignore[union-attr]
            )
            return list(session.exec(stmt).all())


__all__ = ["BrandRepository"]
