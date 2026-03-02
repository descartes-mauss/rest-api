from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class GeographySchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    geography_id: str
    name: str
    is_active: bool
    created_on: datetime
    updated_on: datetime
    business_category: Optional[str] = None
