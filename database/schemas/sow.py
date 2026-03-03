from datetime import datetime

from pydantic import BaseModel

DEFAULT_GEOGRAPHY_ID = "ALL"
DEFAULT_GEOGRAPHY_NAME = "Worldwide"


class SowSchema(BaseModel):
    id: int
    name: str
    load_date: datetime
    geography_id: str = DEFAULT_GEOGRAPHY_ID
    geography_name: str = DEFAULT_GEOGRAPHY_NAME
