"""Service layer for the geographies endpoint."""

from typing import List

from fastapi import HTTPException

from database.schemas.geography import GeographySchema
from repositories.geography_repository import GeographyRepository


class GeographyService:
    """Orchestrates data fetching and assembles the geographies response."""

    def __init__(self, repo: GeographyRepository) -> None:
        self.repo = repo

    def get_geographies(self, tenant_schema: str) -> List[GeographySchema]:
        client_id = self.repo.get_client_id(tenant_schema)
        if client_id is None:
            raise HTTPException(status_code=404, detail="Client not found")

        rows = self.repo.get_active_geographies(client_id)
        return [
            GeographySchema(
                geography_id=geography.geography_id,
                name=geography.name,
                is_active=geography.is_active,
                created_on=geography.created_on,
                updated_on=geography.updated_on,
                business_category=client_geography.business_category,
            )
            for client_geography, geography in rows
        ]
