"""Pydantic schemas for the topic deepdive endpoint."""

from typing import Any, List

from pydantic import BaseModel


class DeepdiveResponse(BaseModel):
    provocations: List[str] = []
    evolution: List[Any] = []
    manifestations: List[Any] = []
    datapoints: List[Any] = []


__all__ = ["DeepdiveResponse"]
