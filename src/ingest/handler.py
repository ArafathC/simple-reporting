"""POST /events -> validate -> Kinesis. The only write path into the system."""
import base64
import json
import os

import boto3

from common.events import ValidationError, validate

_kinesis = None
MAX_BATCH = 100


def _client():
    global _kinesis
    if _kinesis is None:
        _kinesis = boto3.client("kinesis")
    return _kinesis


def _resp(status, body):
    return {"statusCode": status, "headers": {"content-type": "application/json"},
            "body": json.dumps(body)}


def handler(event, _ctx, kinesis=None):
    expected_key = os.environ.get("INGEST_API_KEY")
    if expected_key and (event.get("headers") or {}).get("x-api-key") != expected_key:
        return _resp(401, {"error": "unauthorized"})

    try:
        body = event.get("body") or ""
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode()
        payload = json.loads(body)
    except ValueError:
        return _resp(400, {"error": "body must be valid JSON"})

    events = payload if isinstance(payload, list) else [payload]
    if not 1 <= len(events) <= MAX_BATCH:
        return _resp(400, {"error": f"send 1-{MAX_BATCH} events per request"})
    try:
        records = [validate(e) for e in events]
    except ValidationError as exc:
        return _resp(400, {"error": str(exc)})

    client = kinesis or _client()
    result = client.put_records(
        StreamName=os.environ["STREAM_NAME"],
        Records=[{
            # trailing newline so Firehose writes JSON-lines to S3
            "Data": (json.dumps(r) + "\n").encode(),
            "PartitionKey": r["session_id"] or r["event_id"],
        } for r in records],
    )
    failed = result.get("FailedRecordCount", 0)
    if failed:
        # Clients may safely retry the whole batch: event_id de-duplicates downstream.
        return _resp(503, {"error": "partial failure", "failed": failed})
    return _resp(202, {"accepted": len(records), "event_ids": [r["event_id"] for r in records]})
