"""Value object and factory helpers for the maturity lookup context."""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

from database.tenant_models.models import MaturityScore, MaturityScoreDelta, MaturityScoreSource


@dataclass
class MaturityContext:
    """Lookup context built once per request and shared across all schema assemblies."""

    sources_by_score: Dict[int, List[MaturityScoreSource]] = field(default_factory=dict)
    global_scores_by_id: Dict[int, MaturityScore] = field(default_factory=dict)
    non_global_scores_by_id: Dict[int, List[MaturityScore]] = field(default_factory=dict)
    global_deltas_by_id: Dict[str, MaturityScoreDelta] = field(default_factory=dict)
    non_global_deltas_by_id: Dict[str, List[MaturityScoreDelta]] = field(default_factory=dict)


# ------------------------------------------------------------------
# Low-level split helpers
# ------------------------------------------------------------------


def _build_sources_map(
    sources: List[MaturityScoreSource],
) -> Dict[int, List[MaturityScoreSource]]:
    """Group MaturityScoreSource rows by maturity_score_id."""
    result: Dict[int, List[MaturityScoreSource]] = defaultdict(list)
    for src in sources:
        result[src.maturity_score_id].append(src)
    return result


def _split_scores_impl(
    scores: List[MaturityScore],
    id_field: str,
) -> Tuple[Dict[Any, MaturityScore], Dict[Any, List[MaturityScore]]]:
    global_by_id: Dict[Any, MaturityScore] = {}
    non_global_by_id: Dict[Any, List[MaturityScore]] = defaultdict(list)
    for ms in scores:
        eid = getattr(ms, id_field)
        if eid is None:
            continue
        if str(ms.category) == "global":
            global_by_id.setdefault(eid, ms)
        else:
            non_global_by_id[eid].append(ms)
    return global_by_id, non_global_by_id


def _split_deltas_impl(
    deltas: List[MaturityScoreDelta],
    id_field: str,
) -> Tuple[Dict[Any, MaturityScoreDelta], Dict[Any, List[MaturityScoreDelta]]]:
    global_by_id: Dict[Any, MaturityScoreDelta] = {}
    non_global_by_id: Dict[Any, List[MaturityScoreDelta]] = defaultdict(list)
    for delta in deltas:
        eid = getattr(delta, id_field)
        if eid is None:
            continue
        if str(delta.category) == "global":
            global_by_id.setdefault(eid, delta)
        else:
            non_global_by_id[eid].append(delta)
    return global_by_id, non_global_by_id


def _split_trend_scores(
    scores: List[MaturityScore],
) -> Tuple[Dict[int, MaturityScore], Dict[int, List[MaturityScore]]]:
    """Return (global_by_ssid, non_global_by_ssid) maps keyed by trend ssid."""
    return _split_scores_impl(scores, "trend_id")


def _split_topic_scores(
    scores: List[MaturityScore],
) -> Tuple[Dict[int, MaturityScore], Dict[int, List[MaturityScore]]]:
    """Return (global_by_tid, non_global_by_tid) maps keyed by topic tid."""
    return _split_scores_impl(scores, "topic_id")


def _split_trend_deltas(
    deltas: List[MaturityScoreDelta],
) -> Tuple[Dict[str, MaturityScoreDelta], Dict[str, List[MaturityScoreDelta]]]:
    """Return (global_by_id, non_global_by_id) maps keyed by trend_id string."""
    return _split_deltas_impl(deltas, "trend_id")


def _split_topic_deltas(
    deltas: List[MaturityScoreDelta],
) -> Tuple[Dict[str, MaturityScoreDelta], Dict[str, List[MaturityScoreDelta]]]:
    """Return (global_by_id, non_global_by_id) maps keyed by topic_id string."""
    return _split_deltas_impl(deltas, "topic_id")


# ------------------------------------------------------------------
# MaturityContext factories
# ------------------------------------------------------------------


def _build_trend_context(
    scores: List[MaturityScore],
    sources: List[MaturityScoreSource],
    deltas: List[MaturityScoreDelta],
) -> MaturityContext:
    """Build a MaturityContext keyed by trend ssid (int) and trend_id (str)."""
    global_scores, non_global_scores = _split_trend_scores(scores)
    global_deltas, non_global_deltas = _split_trend_deltas(deltas)
    return MaturityContext(
        sources_by_score=_build_sources_map(sources),
        global_scores_by_id=global_scores,
        non_global_scores_by_id=non_global_scores,
        global_deltas_by_id=global_deltas,
        non_global_deltas_by_id=non_global_deltas,
    )


def _build_topic_context(
    scores: List[MaturityScore],
    sources: List[MaturityScoreSource],
    deltas: List[MaturityScoreDelta],
) -> MaturityContext:
    """Build a MaturityContext keyed by topic tid (int) and topic_id (str)."""
    global_scores, non_global_scores = _split_topic_scores(scores)
    global_deltas, non_global_deltas = _split_topic_deltas(deltas)
    return MaturityContext(
        sources_by_score=_build_sources_map(sources),
        global_scores_by_id=global_scores,
        non_global_scores_by_id=non_global_scores,
        global_deltas_by_id=global_deltas,
        non_global_deltas_by_id=non_global_deltas,
    )
