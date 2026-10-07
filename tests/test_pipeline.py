import base64
import json
from datetime import datetime, timezone

import pytest

from batch.handler import run
from common.events import ValidationError, aggregate, validate
from ingest.handler import handler as ingest
from serving.handler import metrics
from speed.handler import apply_record, handler as speed

T = lambda h, m=0: datetime(2026, 10, 7, h, m, tzinfo=timezone.utc)


def ev(i, typ="page_view", h=10, **kw):
    return validate({"event_id": i, "event_type": typ, **kw}, now=T(h, 5))


def test_validation():
    with pytest.raises(ValidationError):
        validate({"event_type": "nope"})
    with pytest.raises(ValidationError):
        validate({"event_type": "purchase", "amount_cents": -1})
    assert validate({"event_type": "page_view"})["event_id"]


def test_aggregate_dedupes():
    recs = [ev("a"), ev("a"), ev("b", "purchase", amount_cents=500)]
    assert aggregate(recs)["2026-10-07T10"] == {
        "page_views": 1, "add_to_carts": 0, "orders": 1, "revenue_cents": 500}


def test_ingest_to_kinesis(aws, monkeypatch):
    monkeypatch.setenv("STREAM_NAME", "s")
    aws["kinesis"].create_stream(StreamName="s", ShardCount=1)
    body = json.dumps([{"event_type": "page_view"}, {"event_type": "purchase", "amount_cents": 99}])
    r = ingest({"body": body, "headers": {}}, None)
    assert r["statusCode"] == 202 and json.loads(r["body"])["accepted"] == 2
    assert ingest({"body": "{bad", "headers": {}}, None)["statusCode"] == 400
    monkeypatch.setenv("INGEST_API_KEY", "k")
    assert ingest({"body": body, "headers": {}}, None)["statusCode"] == 401


def test_speed_is_idempotent(aws):
    t = aws["speed"]
    purchase = ev("p1", "purchase", amount_cents=250)
    assert apply_record(t, purchase) is True
    assert apply_record(t, purchase) is False
    item = t.get_item(Key={"pk": "global", "sk": "2026-10-07T10"})["Item"]
    assert (item["orders"], item["revenue_cents"]) == (1, 250)


def test_speed_handler_reports_bad_record(aws):
    good = {"kinesis": {"sequenceNumber": "1", "data": base64.b64encode(json.dumps(ev("g")).encode())}}
    bad = {"kinesis": {"sequenceNumber": "2", "data": base64.b64encode(b"nope")}}
    out = speed({"Records": [good, bad]}, None, table=aws["speed"])
    assert out == {"batchItemFailures": [{"itemIdentifier": "2"}]}


def put_raw(s3, key, recs):
    s3.put_object(Bucket="raw", Key=key, Body="".join(json.dumps(r) + "\n" for r in recs).encode())


def test_batch_and_merge(aws):
    s3 = aws["s3"]
    s3.create_bucket(Bucket="raw")
    # hour 10 events; one landed late under the hour=11 prefix, one is a duplicate
    put_raw(s3, "raw/dt=2026-10-07/hour=10/a", [ev("a"), ev("b", "purchase", amount_cents=1000), ev("a")])
    put_raw(s3, "raw/dt=2026-10-07/hour=11/b", [ev("late", "add_to_cart"), ev("h11", h=11)])

    # Not ready until hour 11 has fully ended.
    assert run(s3, aws["batch"], "raw", T(11, 30), start=T(10)) == []
    assert run(s3, aws["batch"], "raw", T(12, 10), start=T(10)) == ["2026-10-07T10"]
    row = aws["batch"].get_item(Key={"pk": "global", "sk": "2026-10-07T10"})["Item"]
    assert (row["page_views"], row["add_to_carts"], row["orders"]) == (1, 1, 1)

    # Speed layer has seen everything (including hour 11, which batch hasn't built yet).
    for r in [ev("a"), ev("b", "purchase", amount_cents=1000), ev("late", "add_to_cart"), ev("h11", h=11)]:
        apply_record(aws["speed"], r)

    out = metrics({"from": "2026-10-07T10:00:00Z", "to": "2026-10-07T11:30:00Z"},
                  aws["batch"], aws["speed"], now=T(12, 10))
    assert out["watermark"] == "2026-10-07T10"
    assert [p["source"] for p in out["series"]] == ["batch", "speed"]
    assert out["totals"]["orders"] == 1 and out["totals"]["revenue_cents"] == 1000
    assert out["totals"]["page_views"] == 2  # 1 batch (h10) + 1 speed (h11)

    # A backfill from earlier must not move the watermark backwards.
    run(s3, aws["batch"], "raw", T(12, 10), start=T(9))
    assert aws["batch"].get_item(Key={"pk": "meta", "sk": "watermark"})["Item"]["hour"] == "2026-10-07T10"
