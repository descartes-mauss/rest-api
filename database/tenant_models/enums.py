"""This module defines enumerations used across the tenant-specific models in the application."""

from enum import IntEnum, StrEnum


class ExperimentType(IntEnum):
    """Enumeration representing the type of an experiment."""

    EXPERIMENT = 0
    TOPIC_DEEP_DIVE = 1
    DIGITAL_TWIN = 2
    PATENT_TOPIC_ASSOCIATION = 3


class MaturityCategory(StrEnum):
    """Maturity score categories matching client_interface MaturityCategory TextChoices."""

    TECH_READINESS = "technology_readiness"
    REGULATORY = "regulatory_and_institutional"
    ADOPTION = "adoption_normalisation"
    ECONOMIC = "economic_viability"
    COMPETITORS = "competitor_landscape"
    ECOSYSTEM = "ecosystem_readiness"
    GLOBAL = "global"


class TenantUserStatus(StrEnum):
    """Enumeration representing the status of a tenant user."""

    ACTIVE = "Active"
    INVITED = "Invited"
    SUSPENDED = "Suspended"


class ConversationStatus(StrEnum):
    """Enumeration representing the status of a conversation."""

    ACTIVE = "Active"
    DELETED = "Deleted"


class SenderType(StrEnum):
    """Enumeration representing the type of sender in a conversation."""

    USER = "User"
    ASSISTANT = "Assistant"
    SYSTEM = "System"
