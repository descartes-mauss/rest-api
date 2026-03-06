"""Schema assembler for the Brand domain."""

from typing import Dict, List

from database.schemas.brand import BrandSchema, BusinessCategorySchema, ProductLineSchema
from database.tenant_models.models import Brand, BusinessCategory, ProductLine


def _brand_to_schema(
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
