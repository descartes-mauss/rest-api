"""Shared helpers for assembling maturity score and trend/topic schemas."""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union

from database.schemas.maturity import (
    MaturityScoreDeltaSchema,
    MaturityScoreSchema,
    MaturityScoreSourceSchema,
)
from database.schemas.topic import TopicSchema, UnlinkedTopicSchema
from database.schemas.trend import TrendSchema
from database.tenant_models.models import (
    MaturityScore,
    MaturityScoreDelta,
    MaturityScoreSource,
    Topic,
    Trend,
)
from services.assemblers.maturity_context import MaturityContext


def _build_sources_map(
    sources: List[MaturityScoreSource],
) -> Dict[int, List[MaturityScoreSource]]:
    """Group MaturityScoreSource rows by maturity_score_id."""
    result: Dict[int, List[MaturityScoreSource]] = defaultdict(list)
    for src in sources:
        result[src.maturity_score_id].append(src)
    return result


# ------------------------------------------------------------------
# Generic split helpers
# ------------------------------------------------------------------


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
# Schema assembly helpers
# ------------------------------------------------------------------


def _maturity_score_to_schema(
    ms: MaturityScore,
    sources: List[MaturityScoreSource],
) -> MaturityScoreSchema:
    return MaturityScoreSchema(
        id=ms.id,
        category=str(ms.category),
        score=float(ms.score) if ms.score is not None else None,
        threshold=ms.threshold,
        rationale=ms.rationale,
        sources=[MaturityScoreSourceSchema.model_validate(s) for s in sources],
    )


def _trend_to_schema(
    trend: Trend,
    ctx: MaturityContext,
    rel_topics_by_ssid: Dict[int, List[Topic]],
    driver_count: Optional[int] = None,
) -> TrendSchema:
    """Construct a TrendSchema from a Trend and its pre-built lookup maps."""
    ssid = trend.ssid or 0
    g_ms = ctx.global_scores_by_id.get(ssid)
    g_delta = ctx.global_deltas_by_id.get(trend.trend_id)
    return TrendSchema(
        ssid=trend.ssid,
        sow=trend.sid,
        load_date=trend.load_date,
        trend_id=trend.trend_id,
        trend_name=trend.trend_name,
        trend_description=trend.trend_description,
        shift_id=trend.shift_id,
        shift_name=trend.shift_name,
        shift_description=trend.shift_description,
        trend_image_s3_uri=trend.trend_image_s3_uri,
        masterfile_version=trend.masterfile_version,
        for_deletion=trend.for_deletion,
        new_discovery=trend.new_discovery,
        driver_count=driver_count,
        maturity_scores=[
            _maturity_score_to_schema(ms, ctx.sources_by_score.get(ms.id or 0, []))
            for ms in sorted(
                ctx.non_global_scores_by_id.get(ssid, []), key=lambda x: str(x.category)
            )
        ],
        maturity_scores_deltas=[
            MaturityScoreDeltaSchema.model_validate(d)
            for d in sorted(
                ctx.non_global_deltas_by_id.get(trend.trend_id, []),
                key=lambda x: str(x.category),
            )
        ],
        global_maturity_score=(
            _maturity_score_to_schema(g_ms, ctx.sources_by_score.get(g_ms.id or 0, []))
            if g_ms
            else None
        ),
        global_maturity_score_delta=(
            MaturityScoreDeltaSchema.model_validate(g_delta) if g_delta else None
        ),
        related_topics=[
            UnlinkedTopicSchema.model_validate(rt) for rt in rel_topics_by_ssid.get(ssid, [])
        ],
    )


def _topic_to_schema(
    topic: Topic,
    ctx: MaturityContext,
    trend_schema_by_ssid: Dict[int, TrendSchema],
    drivers_by_tid: Dict[int, List[int]],
) -> TopicSchema:
    """Construct a TopicSchema from a Topic and its pre-built lookup maps."""
    tid = topic.tid or 0
    g_ms = ctx.global_scores_by_id.get(tid)
    g_delta = ctx.global_deltas_by_id.get(topic.topic_id)
    return TopicSchema(
        tid=topic.tid,
        sow=topic.sid,
        load_date=topic.load_date,
        topic_id=topic.topic_id,
        topic_name=topic.topic_name,
        topic_status=topic.topic_status,
        topic_description=topic.topic_description,
        topic_image_s3_uri=topic.topic_image_s3_uri,
        masterfile_version=topic.masterfile_version,
        for_deletion=topic.for_deletion,
        new_discovery=topic.new_discovery,
        trend=trend_schema_by_ssid.get(topic.ssid or 0),
        driver=drivers_by_tid.get(tid, []),
        maturity_scores=[
            _maturity_score_to_schema(ms, ctx.sources_by_score.get(ms.id or 0, []))
            for ms in sorted(
                ctx.non_global_scores_by_id.get(tid, []), key=lambda x: str(x.category)
            )
        ],
        maturity_scores_deltas=[
            MaturityScoreDeltaSchema.model_validate(d)
            for d in sorted(
                ctx.non_global_deltas_by_id.get(topic.topic_id, []),
                key=lambda x: str(x.category),
            )
        ],
        global_maturity_score=(
            _maturity_score_to_schema(g_ms, ctx.sources_by_score.get(g_ms.id or 0, []))
            if g_ms
            else None
        ),
        global_maturity_score_delta=(
            MaturityScoreDeltaSchema.model_validate(g_delta) if g_delta else None
        ),
    )


def _trend_sort_key(trend: Trend, global_score_by_ssid: Dict[int, MaturityScore]) -> Tuple:  # type: ignore[type-arg]
    """Sort key mirroring Django's order_by('-global_maturity_score', 'shift_id', 'trend_name')."""
    ms = global_score_by_ssid.get(trend.ssid or -1)
    score = float(ms.score) if ms and ms.score is not None else None
    # None scores sort last: (True, ...) > (False, ...)
    return (score is None, -(score or 0.0), trend.shift_id or "", trend.trend_name or "")


def _passes_maturity_filter(
    entity: Union[Topic, Trend],
    global_score: Optional[MaturityScore],
    maturity_level: str,
) -> bool:
    """Return True if entity should be included for the given maturity_level filter."""
    if maturity_level == "New":
        return bool(entity.new_discovery)
    if maturity_level == "All":
        return True
    return (global_score.threshold if global_score else None) == maturity_level


# ------------------------------------------------------------------
# MaturityContext factory helpers
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
