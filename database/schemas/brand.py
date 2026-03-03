from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, model_validator


class BusinessCategorySchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    business_category_name: str
    business_category_description: Optional[str] = None
    # Aliases matching the Django BusinessCategorySerializer declared fields
    name: str = ""
    description: Optional[str] = None

    @model_validator(mode="after")
    def populate_aliases(self) -> "BusinessCategorySchema":
        self.name = self.business_category_name
        self.description = self.business_category_description
        return self


class ProductLineSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    brand_id: int
    product_line_name: str
    product_line_user_benefit: Optional[str] = None
    product_line_value_proposition: Optional[str] = None


class BrandSchema(BaseModel):
    id: Optional[int] = None
    brand_name: str
    brand_description: Optional[str] = None
    brand_purpose: Optional[str] = None
    brand_mission: Optional[str] = None
    brand_attributes: Optional[List[str]] = None
    brand_country: Optional[List[Any]] = None
    brand_business_category: Optional[BusinessCategorySchema] = None
    product_lines: List[ProductLineSchema] = []
