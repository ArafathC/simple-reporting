"""Speed layer: Kinesis -> incremental hourly counters in DynamoDB.

Kinesis delivers at-least-once, so each event_id is claimed with a conditional
put before its delta is applied; replays are skipped.
"""
import base64
import json
import os
import time

import boto3

from common.events import hour_bucket, to_delta

SEEN_TTL_S = 2 * 24 * 3600
VIEW_TTL_S = 14 * 24 * 3600  # batch view takes over long before this


class AlreadyProcessed(Exception):
    pass


def apply_record(table, rec: dict, now: int | None = None) -> bool:
    """Apply one event. Returns False if it was a duplicate."""
    now = now or int(time.time())
    try:
        table.put_item(
            Item={"pk": "seen", "sk": rec["event_id"], "ttl": now + SEEN_TTL_S},
            ConditionExpression="attribute_not_exists(pk)",
        )
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return False
    delta = to_delta(rec)
    names = {f"#m{i}": m for i, m in enumerate(delta)}
    values = {f":v{i}": n for i, n in enumerate(delta.values())}
    values[":ttl"] = now + VIEW_TTL_S
    table.update_item(
        Key={"pk": "global", "sk": hour_bucket(rec["received_at"])},
        UpdateExpression="ADD " + ", ".join(f"#m{i} :v{i}" for i in range(len(delta)))
        + " SET #ttl = :ttl",
        ExpressionAttributeNames={**names, "#ttl": "ttl"},
        ExpressionAttributeValues=values,
    )
    return True


def handler(event, _ctx, table=None):
    table = table or boto3.resource("dynamodb").Table(os.environ["SPEED_TABLE"])
    failures = []
    for r in event["Records"]:
        try:
            rec = json.loads(base64.b64decode(r["kinesis"]["data"]))
            apply_record(table, rec)
        except Exception:  # report only this record; Lambda retries from it onward
            failures.append({"itemIdentifier": r["kinesis"]["sequenceNumber"]})
    return {"batchItemFailures": failures}
