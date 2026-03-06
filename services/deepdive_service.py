"""Service layer for the topic deepdive endpoint."""

from typing import Any, List

from database.schemas.deepdive import DeepdiveResponse
from external.s3_rest_client import S3RestClient
from repositories.sow_repository import SowRepository
from repositories.topic_repository import TopicRepository

_MANIFESTATIONS_LIMIT = 4
_MARKET_INSIGHTS_LIMIT = 2
_YEAR_KEYS = ("year_one", "year_two", "year_three", "year_four", "year_five")
_DESCRIPTION_KEYS = (
    "year_one_description",
    "year_two_description",
    "year_three_description",
    "year_four_description",
    "year_five_description",
)


class DeepdiveService:
    """Orchestrates the topic deepdive by fetching from S3 and post-processing."""

    def __init__(
        self,
        topic_repository: TopicRepository,
        sow_repository: SowRepository,
        s3_client: S3RestClient,
    ) -> None:
        self.topic_repository = topic_repository
        self.sow_repository = sow_repository
        self.s3_client = s3_client

    def get_topic_deepdive(self, tenant_schema: str, topic_id: str) -> DeepdiveResponse:
        """Return provocations, evolution, manifestations and datapoints for a topic."""
        topic = self.topic_repository.get_by_topic_id(tenant_schema, topic_id)
        if topic is None or topic.tid is None or topic.sid is None:
            return DeepdiveResponse()

        sow = self.sow_repository.get_sow_by_id(tenant_schema, topic.sid)
        cs_sow_id = sow.cs_sow_id if sow and sow.cs_sow_id else ""

        provocations = self._get_provocations(tenant_schema, cs_sow_id, topic.topic_id)
        evolution = self._get_evolution(tenant_schema, cs_sow_id, topic.topic_id)
        manifestations = self._get_manifestations(tenant_schema, cs_sow_id, topic.topic_id)
        datapoints = self._get_market_insights(tenant_schema, cs_sow_id, topic.topic_id)

        return DeepdiveResponse(
            provocations=provocations,
            evolution=evolution,
            manifestations=manifestations,
            datapoints=datapoints,
        )

    def _get_provocations(self, org_id: str, cs_sow_id: str, topic_id: str) -> List[str]:
        response = self.s3_client.get_topic_provocations(org_id, cs_sow_id, topic_id)
        first_result = response[0] if response else {}
        return [
            v for k, v in first_result.items() if k in ("provocation_one", "provocation_two") and v
        ]

    def _get_evolution(self, org_id: str, cs_sow_id: str, topic_id: str) -> List[Any]:
        response = self.s3_client.get_topic_evolution(org_id, cs_sow_id, topic_id)
        if not response:
            return []
        first_result = response[0]
        return [
            [first_result[year], first_result[desc]]
            for year, desc in zip(_YEAR_KEYS, _DESCRIPTION_KEYS)
            if year in first_result and desc in first_result
        ]

    def _get_manifestations(self, org_id: str, cs_sow_id: str, topic_id: str) -> List[Any]:
        response = self.s3_client.get_topic_manifestations(org_id, cs_sow_id, topic_id)
        return response[:_MANIFESTATIONS_LIMIT]

    def _get_market_insights(self, org_id: str, cs_sow_id: str, topic_id: str) -> List[Any]:
        response = self.s3_client.get_topic_market_insights(org_id, cs_sow_id, topic_id)
        return response[:_MARKET_INSIGHTS_LIMIT]


__all__ = ["DeepdiveService"]
