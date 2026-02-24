"""Shared utilities and types for database/typing interactions."""

# Generic type for SQLModel subclasses
from typing import TypeVar

from sqlmodel import SQLModel

SQLModelType = TypeVar("SQLModelType", bound=SQLModel)
