from typing import List, Optional

from pydantic import BaseModel

from database.schemas.trend import TrendSchema


class ShiftSchema(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    trends: List[TrendSchema] = []
