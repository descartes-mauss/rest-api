from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, model_validator

from database.schemas.brand import BrandSchema, BusinessCategorySchema


class CustomerSegmentSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    customer_segment_name: str
    customer_segment_description: Optional[str] = None
    # Aliases matching the Django CustomerSegmentSerializer declared fields
    name: str = ""
    description: Optional[str] = None

    @model_validator(mode="after")
    def populate_aliases(self) -> "CustomerSegmentSchema":
        self.name = self.customer_segment_name
        self.description = self.customer_segment_description
        return self


class CompanyProfileSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    client_id: int
    company_name: Optional[str] = None
    brands_count: Optional[int] = None
    company_purpose: Optional[str] = None
    company_purpose_implication: Optional[str] = None
    company_vision: Optional[str] = None
    company_vision_implication: Optional[str] = None
    company_mission: Optional[str] = None
    company_mission_implication: Optional[str] = None
    company_personality: Optional[List[Any]] = None
    company_strategic_priorities: Optional[str] = None
    company_competitors: Optional[List[str]] = None
    created_at: datetime
    updated_at: datetime


class CompanyResponse(BaseModel):
    company_profile_image_uri: Optional[str] = None
    company_profile: Optional[CompanyProfileSchema] = None
    brands: List[BrandSchema] = []
    customer_segments: List[CustomerSegmentSchema] = []
    business_categories: List[BusinessCategorySchema] = []
