"""GET /metrics?from=&to= (ISO-8601, default last 24h) and GET /dashboard."""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Key

from .merge import HOUR_FMT, hours_between, merge, parse_time, summarize

MAX_HOURS = 24 * 31


def _rows(table, start: str, end: str) -> dict:
    resp = table.query(KeyConditionExpression=Key("pk").eq("global") & Key("sk").between(start, end))
    rows = resp["Items"]
    while "LastEvaluatedKey" in resp:
        resp = table.query(KeyConditionExpression=Key("pk").eq("global") & Key("sk").between(start, end),
                           ExclusiveStartKey=resp["LastEvaluatedKey"])
        rows += resp["Items"]
    return {r["sk"]: r for r in rows}


def metrics(params: dict, batch_table, speed_table, now=None):
    now = now or datetime.now(timezone.utc)
    end = parse_time(params["to"]) if params.get("to") else now
    start = parse_time(params["from"]) if params.get("from") else end - timedelta(hours=23)
    hours = list(hours_between(start, end))
    if not 1 <= len(hours) <= MAX_HOURS:
        raise ValueError(f"range must be 1-{MAX_HOURS} hours")
    wm = batch_table.get_item(Key={"pk": "meta", "sk": "watermark"}).get("Item")
    watermark = wm["hour"] if wm else None
    series = merge(hours, _rows(batch_table, hours[0], hours[-1]),
                   _rows(speed_table, hours[0], hours[-1]), watermark)
    return {"watermark": watermark, "series": series, "totals": summarize(series)}


def _resp(status, body, ctype="application/json"):
    return {"statusCode": status, "headers": {"content-type": ctype},
            "body": body if isinstance(body, str) else json.dumps(body)}


def handler(event, _ctx, batch_table=None, speed_table=None):
    path = event.get("rawPath", "")
    if path.endswith("/dashboard"):
        return _resp(200, (Path(__file__).parent / "dashboard.html").read_text(), "text/html")
    ddb = None
    if batch_table is None:
        ddb = boto3.resource("dynamodb")
        batch_table = ddb.Table(os.environ["BATCH_TABLE"])
        speed_table = ddb.Table(os.environ["SPEED_TABLE"])
    try:
        return _resp(200, metrics(event.get("queryStringParameters") or {}, batch_table, speed_table))
    except ValueError as exc:
        return _resp(400, {"error": str(exc)})
