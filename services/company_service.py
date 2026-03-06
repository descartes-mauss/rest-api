"""Service layer for the company endpoint."""

from collections import defaultdict
from typing import Dict, List

from fastapi import HTTPException

from database.schemas.brand import BrandSchema, BusinessCategorySchema
from database.schemas.company import (
    CompanyProfileSchema,
    CompanyResponse,
    CustomerSegmentSchema,
)
from database.tenant_models.models import Brand, BusinessCategory, ProductLine
from repositories.protocols import BrandRepositoryProtocol, CompanyRepositoryProtocol
from services.assemblers.brand_assembler import _brand_to_schema


def _assemble_brands(
    brands: List[Brand],
    product_lines: List[ProductLine],
    categories: List[BusinessCategory],
) -> List[BrandSchema]:
    pl_by_brand: Dict[int, List[ProductLine]] = defaultdict(list)
    for pl in product_lines:
        pl_by_brand[pl.brand_id].append(pl)

    cat_by_id: Dict[int, BusinessCategory] = {c.id: c for c in categories if c.id}

    return [_brand_to_schema(b, cat_by_id, pl_by_brand) for b in brands]


class CompanyService:
    """Orchestrates cross-schema data fetching and assembles the company response."""

    def __init__(
        self,
        company_repository: CompanyRepositoryProtocol,
        brand_repository: BrandRepositoryProtocol,
    ) -> None:
        self.company_repository = company_repository
        self.brand_repository = brand_repository

    def get_company(self, tenant_schema: str) -> CompanyResponse:
        ci_client = self.company_repository.get_ci_client(tenant_schema.lower())
        if ci_client is None:
            raise HTTPException(status_code=404, detail="Client not found")

        company_profile = self.company_repository.get_company_profile(ci_client.id)  # type: ignore[arg-type]

        if not company_profile:
            return CompanyResponse(
                company_profile=None,
                brands=[],
                customer_segments=[],
                business_categories=[],
            )

        image_uri = self.company_repository.get_cs_client_image(tenant_schema)

        brands = self.brand_repository.get_brands(tenant_schema)
        brand_ids = [b.id for b in brands if b.id is not None]
        category_ids = list(
            {b.brand_business_category_id for b in brands if b.brand_business_category_id}
        )
        product_lines = self.brand_repository.get_product_lines_by_brand_ids(
            tenant_schema, brand_ids
        )
        brand_categories = self.brand_repository.get_business_categories_by_ids(
            tenant_schema, category_ids
        )

        customer_segments = self.company_repository.get_customer_segments(tenant_schema)
        all_business_categories = self.company_repository.get_all_business_categories(tenant_schema)

        return CompanyResponse(
            company_profile_image_uri=image_uri,
            company_profile=CompanyProfileSchema.model_validate(company_profile),
            brands=_assemble_brands(brands, product_lines, brand_categories),
            customer_segments=[CustomerSegmentSchema.model_validate(s) for s in customer_segments],
            business_categories=[
                BusinessCategorySchema.model_validate(c) for c in all_business_categories
            ],
        )
