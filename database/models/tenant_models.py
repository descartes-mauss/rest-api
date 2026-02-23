from datetime import datetime
from decimal import Decimal
from enum import IntEnum
from typing import List, Optional

from sqlalchemy import JSON, Column
from sqlmodel import Field, SQLModel


class SOW(SQLModel, table=True):
    __tablename__ = "client_interface_sowmodel"

    sid: Optional[int] = Field(default=None, primary_key=True)
    load_date: datetime
    sow_name: str
    sow_status: str
    sow_description: Optional[str] = None
    cs_sow_id: Optional[str] = None
    masterfile_version: int
    for_deletion: bool = False
