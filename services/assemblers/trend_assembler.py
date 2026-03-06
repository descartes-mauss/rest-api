"""Schema assembler for the Trend domain."""

from typing import Dict, List, Optional, Tuple, Union

from database.schemas.maturity import (
    MaturityScoreDeltaSchema,
    MaturityScoreSchema,
    MaturityScoreSourceSchema,
)
from database.schemas.topic import UnlinkedTopicSchema
from database.schemas.trend import TrendSchema
from database.tenant_models.models import (
    MaturityScore,
    MaturityScoreSource,
    Topic,
    Trend,
)
from services.assemblers.maturity_context import MaturityContext


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
