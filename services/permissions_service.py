"""Service layer for the permissions endpoint."""

from fastapi import HTTPException

from database.schemas.permissions import ExperimentSchema, PermissionsResponse
from repositories.permissions_repository import PermissionsRepository


class PermissionsService:
    """Orchestrates data fetching and assembles the permissions response."""

    def __init__(self, repository: PermissionsRepository) -> None:
        self.permissions_repository = repository

    def get_permissions(
        self,
        tenant_schema: str,
        sow_id: int,
    ) -> PermissionsResponse:
        sow = self.permissions_repository.get_sow(tenant_schema, sow_id)
        if sow is None:
            raise HTTPException(status_code=404, detail="SowModel not available")

        tier_id = self.permissions_repository.get_client_tier_id(tenant_schema)
        feature_codes = (
            self.permissions_repository.get_feature_codes(tier_id) if tier_id is not None else []
        )

        experiments = self.permissions_repository.get_experiments(sow.cs_sow_id)

        has_opportunities = self.permissions_repository.has_opportunity_platforms(
            tenant_schema, sow.sid  # type: ignore[arg-type]
        )

        return PermissionsResponse(
            experiments=[ExperimentSchema.model_validate(e) for e in experiments],
            permissions={code: True for code in feature_codes},
            opportunity_platforms=has_opportunities,
        )
