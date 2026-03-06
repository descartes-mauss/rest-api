"""Schema assembler for the Topic domain."""

from typing import Dict, List

from database.schemas.maturity import MaturityScoreDeltaSchema
from database.schemas.topic import TopicSchema
from database.schemas.trend import TrendSchema
from database.tenant_models.models import Topic
from services.assemblers.maturity_context import MaturityContext
from services.assemblers.trend_assembler import _maturity_score_to_schema


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
