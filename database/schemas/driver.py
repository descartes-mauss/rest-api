from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from database.schemas.sow import SowSchema


class DriverSchema(BaseModel):
    did: Optional[int] = None
    sow: SowSchema
    load_date: datetime
    driver_id: str
    driver_name: str
    driver_description: Optional[str] = None
    masterfile_version: int
    for_deletion: bool = False
    topic_count: int = 0
