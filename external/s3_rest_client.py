"""Client for the S3-backed DeepDive REST API."""

import json
import logging
from typing import Any, List

import httpx

logger = logging.getLogger(__name__)

_HTTP_OK = 200


class S3RestClient:
    """HTTP client for the topic deepdive S3 REST API."""

    def __init__(self, base_url: str, api_key: str) -> None:
        self._base_url = base_url
        self._headers = {"Content-Type": "application/json", "x-api-key": api_key}

    def get_topic_provocations(self, org_id: str, cs_sow_id: str, topic_id: str) -> List[Any]:
        url = self._build_url("topic_provocations", org_id, cs_sow_id, topic_id)
        return self._get(url)

    def get_topic_evolution(self, org_id: str, cs_sow_id: str, topic_id: str) -> List[Any]:
        url = self._build_url("topic_evolutions", org_id, cs_sow_id, topic_id)
        return self._get(url)

    def get_topic_manifestations(self, org_id: str, cs_sow_id: str, topic_id: str) -> List[Any]:
        url = self._build_url("topic_manifestations", org_id, cs_sow_id, topic_id)
        return self._get(url)

    def get_topic_market_insights(self, org_id: str, cs_sow_id: str, topic_id: str) -> List[Any]:
        url = self._build_url("topic_market_insights", org_id, cs_sow_id, topic_id)
        return self._get(url)

    def _build_url(self, topic_name: str, org_id: str, cs_sow_id: str, topic_id: str) -> str:
        return (
            f"{self._base_url}?topic_name={topic_name}"
            f"&org_id={org_id}&sow_id={cs_sow_id}&topic_id={topic_id}"
        )

    def _get(self, url: str) -> List[Any]:
        logger.info("GET %s", url)
        try:
            response = httpx.get(url, headers=self._headers, timeout=10)
            response.raise_for_status()
            json_response = response.json()
            if int(json_response.get("statusCode", 0)) != _HTTP_OK:
                return []
            body = json_response.get("body")
            if body is None:
                return []
            parsed = json.loads(body) if isinstance(body, str) else body
            return parsed if isinstance(parsed, list) else []
        except Exception as exc:
            logger.error("S3RestClient error for %s: %s", url, exc)
            return []


__all__ = ["S3RestClient"]
