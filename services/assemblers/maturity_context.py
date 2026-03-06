"""Value object grouping the five maturity lookup dicts built once per request."""

from dataclasses import dataclass, field
from typing import Dict, List

from database.tenant_models.models import MaturityScore, MaturityScoreDelta, MaturityScoreSource


@dataclass
class MaturityContext:
    """Lookup context built once per request and shared across all schema assemblies."""

    sources_by_score: Dict[int, List[MaturityScoreSource]] = field(default_factory=dict)
    global_scores_by_id: Dict[int, MaturityScore] = field(default_factory=dict)
    non_global_scores_by_id: Dict[int, List[MaturityScore]] = field(default_factory=dict)
    global_deltas_by_id: Dict[str, MaturityScoreDelta] = field(default_factory=dict)
    non_global_deltas_by_id: Dict[str, List[MaturityScoreDelta]] = field(default_factory=dict)
