from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class TenantSow(SQLModel, table=True):
    __tablename__ = "client_interface_sowmodel"

    sid: Optional[int] = Field(default=None, primary_key=True)
    load_date: datetime
    sow_name: str
    sow_status: str
    sow_description: Optional[str] = None
    cs_sow_id: Optional[str] = None
    masterfile_version: int
    for_deletion: bool = False
