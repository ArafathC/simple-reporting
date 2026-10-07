"""Batch layer: recompute hourly aggregates from the immutable S3 master dataset.

Runs hourly. Hour H is processed once H+1 has fully ended (Firehose may land
events received in H under the H+1 prefix). Writes are overwrites, so re-runs
and backfills are idempotent. Advancing the watermark tells the serving layer
to prefer the batch view for every hour <= watermark.

Reads whole S3 prefixes in a Lambda: fine for modest volume. For large volume
swap `read_hour` for an Athena/Glue/EMR job; the contract (write one item per
hour + watermark) stays the same.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import boto3

from common.events import METRICS, aggregate, hour_bucket, parse_lines

HOUR_FMT = "%Y-%m-%dT%H"


def _prefix(hour: datetime) -> str:
    return f"raw/dt={hour:%Y-%m-%d}/hour={hour:%H}/"


def read_hour(s3, bucket: str, hour: datetime):
    """Yield events received in `hour`, reading the H and H+1 prefixes."""
    want = hour.strftime(HOUR_FMT)
    for h in (hour, hour + timedelta(hours=1)):
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=_prefix(h)):
            for obj in page.get("Contents", []):
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode()
                for rec in parse_lines(body):
                    if hour_bucket(rec["received_at"]) == want:
                        yield rec


def run(s3, table, bucket: str, now: datetime, max_hours: int = 48, start: datetime | None = None):
    """Process every ready hour after the watermark. Returns hours processed."""
    item = table.get_item(Key={"pk": "meta", "sk": "watermark"}).get("Item")
    if start is not None:
        next_hour = start
    elif item:
        next_hour = datetime.strptime(item["hour"], HOUR_FMT).replace(tzinfo=timezone.utc) + timedelta(hours=1)
    else:
        next_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=24)
    done = []
    while len(done) < max_hours and next_hour + timedelta(hours=2) <= now:
        # (`now` >= end of H+1)
        records = list(read_hour(s3, bucket, next_hour))
        buckets = aggregate(records)
        key = next_hour.strftime(HOUR_FMT)
        row = buckets.get(key, dict.fromkeys(METRICS, 0))
        table.put_item(Item={"pk": "global", "sk": key, **row})
        if not item or key > item["hour"]:  # a backfill must never move the watermark back
            item = {"pk": "meta", "sk": "watermark", "hour": key}
            table.put_item(Item=item)
        done.append(key)
        next_hour += timedelta(hours=1)
    return done


def handler(event, _ctx):
    now = datetime.now(timezone.utc)
    start = None
    if event.get("backfill_from"):  # e.g. {"backfill_from": "2026-10-01T00"}
        start = datetime.strptime(event["backfill_from"], HOUR_FMT).replace(tzinfo=timezone.utc)
    table = boto3.resource("dynamodb").Table(os.environ["BATCH_TABLE"])
    done = run(boto3.client("s3"), table, os.environ["RAW_BUCKET"], now, start=start)
    return {"processed": done}
