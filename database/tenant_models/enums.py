"""This module defines enumerations used across the tenant-specific models in the application."""

from enum import IntEnum


class ExperimentType(IntEnum):
    """Enumeration representing the type of an experiment."""

    EXPERIMENT = 0
    TOPIC_DEEP_DIVE = 1
    DIGITAL_TWIN = 2
    PATENT_TOPIC_ASSOCIATION = 3
