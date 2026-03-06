"""Service layer for the brands endpoints."""

from collections import defaultdict
from typing import Dict, List

from fastapi import HTTPException

from database.schemas.brand import BrandSchema
from database.tenant_models.models import BusinessCategory, ProductLine
from repositories.brand_repository import BrandRepository
from services.assemblers.brand_assembler import _brand_to_schema


class BrandService:
    """Orchestrates data fetching and assembles brand responses."""

    def __init__(self, brand_repository: BrandRepository) -> None:
        self.brand_repository = brand_repository

    def get_brands(self, tenant_schema: str) -> List[BrandSchema]:
        brands = self.brand_repository.get_brands(tenant_schema)
        if not brands:
            return []

        brand_ids = [b.id for b in brands if b.id is not None]
        category_ids = list(
            {b.brand_business_category_id for b in brands if b.brand_business_category_id}
        )

        product_lines = self.brand_repository.get_product_lines_by_brand_ids(
            tenant_schema, brand_ids
        )
        categories = self.brand_repository.get_business_categories_by_ids(
            tenant_schema, category_ids
        )

        pl_by_brand: Dict[int, List[ProductLine]] = defaultdict(list)
        for pl in product_lines:
            pl_by_brand[pl.brand_id].append(pl)

        cat_by_id: Dict[int, BusinessCategory] = {c.id: c for c in categories if c.id}

        return [_brand_to_schema(b, cat_by_id, pl_by_brand) for b in brands]

    def get_brand(self, tenant_schema: str, brand_id: int) -> BrandSchema:
        brand = self.brand_repository.get_brand_by_id(tenant_schema, brand_id)
        if brand is None:
            raise HTTPException(status_code=404, detail="Brand not available")

        brand_ids = [brand.id] if brand.id is not None else []
        category_ids = (
            [brand.brand_business_category_id] if brand.brand_business_category_id else []
        )

        product_lines = self.brand_repository.get_product_lines_by_brand_ids(
            tenant_schema, brand_ids
        )
        categories = self.brand_repository.get_business_categories_by_ids(
            tenant_schema, category_ids
        )

        pl_by_brand: Dict[int, List[ProductLine]] = {brand.id or 0: product_lines}
        cat_by_id: Dict[int, BusinessCategory] = {c.id: c for c in categories if c.id}

        return _brand_to_schema(brand, cat_by_id, pl_by_brand)
