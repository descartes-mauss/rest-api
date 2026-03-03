"""Service layer for the brands endpoints."""

from collections import defaultdict
from typing import Dict, List, Optional

from fastapi import HTTPException

from database.schemas.brand import BrandSchema, BusinessCategorySchema, ProductLineSchema
from database.tenant_models.models import Brand, BusinessCategory, ProductLine
from repositories.brand_repository import BrandRepository


def _assemble_brand_schema(
    brand: Brand,
    categories_by_id: Dict[int, BusinessCategory],
    product_lines_by_brand_id: Dict[int, List[ProductLine]],
) -> BrandSchema:
    cat = (
        categories_by_id.get(brand.brand_business_category_id)
        if brand.brand_business_category_id
        else None
    )
    return BrandSchema(
        id=brand.id,
        brand_name=brand.brand_name,
        brand_description=brand.brand_description,
        brand_purpose=brand.brand_purpose,
        brand_mission=brand.brand_mission,
        brand_attributes=brand.brand_attributes,
        brand_country=brand.brand_country,
        brand_business_category=BusinessCategorySchema.model_validate(cat) if cat else None,
        product_lines=[
            ProductLineSchema.model_validate(pl)
            for pl in product_lines_by_brand_id.get(brand.id or 0, [])
        ],
    )


class BrandService:
    """Orchestrates data fetching and assembles brand responses."""

    def __init__(self, repo: BrandRepository) -> None:
        self.repo = repo

    def get_brands(self, tenant_schema: Optional[str]) -> List[BrandSchema]:
        if not tenant_schema:
            raise HTTPException(
                status_code=400,
                detail="Authorization token missing tenant schema information.",
            )

        brands = self.repo.get_brands(tenant_schema)
        if not brands:
            return []

        brand_ids = [b.id for b in brands if b.id is not None]
        category_ids = list(
            {b.brand_business_category_id for b in brands if b.brand_business_category_id}
        )

        product_lines = self.repo.get_product_lines_by_brand_ids(tenant_schema, brand_ids)
        categories = self.repo.get_business_categories_by_ids(tenant_schema, category_ids)

        pl_by_brand: Dict[int, List[ProductLine]] = defaultdict(list)
        for pl in product_lines:
            pl_by_brand[pl.brand_id].append(pl)

        cat_by_id: Dict[int, BusinessCategory] = {c.id: c for c in categories if c.id}

        return [_assemble_brand_schema(b, cat_by_id, pl_by_brand) for b in brands]

    def get_brand(self, tenant_schema: Optional[str], brand_id: int) -> BrandSchema:
        if not tenant_schema:
            raise HTTPException(
                status_code=400,
                detail="Authorization token missing tenant schema information.",
            )

        brand = self.repo.get_brand_by_id(tenant_schema, brand_id)
        if brand is None:
            raise HTTPException(status_code=404, detail="Brand not available")

        brand_ids = [brand.id] if brand.id is not None else []
        category_ids = (
            [brand.brand_business_category_id] if brand.brand_business_category_id else []
        )

        product_lines = self.repo.get_product_lines_by_brand_ids(tenant_schema, brand_ids)
        categories = self.repo.get_business_categories_by_ids(tenant_schema, category_ids)

        pl_by_brand: Dict[int, List[ProductLine]] = {brand.id or 0: product_lines}
        cat_by_id: Dict[int, BusinessCategory] = {c.id: c for c in categories if c.id}

        return _assemble_brand_schema(brand, cat_by_id, pl_by_brand)
