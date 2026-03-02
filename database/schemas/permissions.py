from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ExperimentSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    experiment_id: int
    sow_id: int
    experiment_name: str
    experiment_url: str
    experiment_type: int
    created_at: datetime
    updated_at: datetime


class PermissionsResponse(BaseModel):
    experiments: list[ExperimentSchema]
    permissions: dict[str, bool]
    opportunity_platforms: bool
