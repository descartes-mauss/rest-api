import datetime

from fastapi.testclient import TestClient

from database.tenant_models.models import Topic
from main import app
from repositories import topic_repository

client = TestClient(app)


def make_topic(topic_id: str = "t1") -> Topic:
    return Topic(
        tid=1,
        sid=10,
        load_date=datetime.datetime.utcnow(),
        topic_id=topic_id,
        topic_name="Test Topic",
        topic_status=1,
        topic_description="desc",
        topic_image_s3_uri=None,
        ssid=None,
        industry_sizing=None,
        society_sizing=None,
        consumer_sizing=None,
        average_sizing=None,
        average_sizing_label="",
        timeline=None,
        timeline_display=None,
        timeline_label="",
        topic_growth=None,
        topic_growth_normalized=None,
        topic_consensus=None,
        topic_consensus_normalized=None,
        topic_consensus_label="",
        topic_consensus_icon_uri=None,
        industry_impact=None,
        industry_impact_display=None,
        industry_impact_label="",
        industry_impact_icon_uri=None,
        action_required="",
        masterfile_version=1,
        for_deletion=False,
        new_discovery=False,
    )


def test_list_topics(monkeypatch) -> None:
    topics = [make_topic("topic-1"), make_topic("topic-2")]
    monkeypatch.setattr(topic_repository, "get_all", lambda tenant=None: topics)

    resp = client.get("/api/v2/topics")
    assert resp.status_code == 200
    data = resp.json()
    assert "topics" in data
    assert isinstance(data["topics"], list)
    assert len(data["topics"]) == 2
    assert data["topics"][0]["topic_id"] == "topic-1"


def test_get_topic_found(monkeypatch) -> None:
    t = make_topic("topic-42")
    monkeypatch.setattr(topic_repository, "get_by_topic_id", lambda topic_id, tenant=None: t)

    resp = client.get("/api/v2/topics/topic-42")
    assert resp.status_code == 200
    data = resp.json()
    assert "topic" in data
    assert data["topic"]["topic_id"] == "topic-42"


def test_get_topic_not_found(monkeypatch) -> None:
    monkeypatch.setattr(topic_repository, "get_by_topic_id", lambda topic_id, tenant=None: None)

    resp = client.get("/api/v2/topics/does-not-exist")
    assert resp.status_code == 404
    assert resp.json().get("error") == "Topic not found"
