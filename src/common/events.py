"""Event schema, validation and the shared aggregation logic.

Both the speed layer and the batch layer use `to_delta` / `hour_bucket`, so
the two views of the data agree by construction.
"""
import json
import uuid
from datetime import datetime, timezone

EVENT_TYPES = {"page_view", "add_to_cart", "purchase"}
METRICS = ("page_views", "add_to_carts", "orders", "revenue_cents")
_METRIC_FOR_TYPE = {
    "page_view": "page_views",
    "add_to_cart": "add_to_carts",
    "purchase": "orders",
}


class ValidationError(ValueError):
    pass


def validate(raw: dict, now: datetime | None = None) -> dict:
    """Validate an incoming event and return the normalised record.

    `received_at` is stamped server-side and is the time both layers bucket on
    (client clocks are not trusted).
    """
    if not isinstance(raw, dict):
        raise ValidationError("event must be a JSON object")
    event_type = raw.get("event_type")
    if event_type not in EVENT_TYPES:
        raise ValidationError(f"event_type must be one of {sorted(EVENT_TYPES)}")
    event_id = raw.get("event_id") or str(uuid.uuid4())
    if not isinstance(event_id, str) or not 1 <= len(event_id) <= 128:
        raise ValidationError("event_id must be a string of 1-128 chars")
    for field in ("user_id", "session_id"):
        if raw.get(field) is not None and not isinstance(raw[field], str):
            raise ValidationError(f"{field} must be a string")
    record = {
        "event_id": event_id,
        "event_type": event_type,
        "user_id": raw.get("user_id"),
        "session_id": raw.get("session_id"),
        "client_ts": raw.get("client_ts"),
        "received_at": (now or datetime.now(timezone.utc)).isoformat(),
    }
    if event_type == "purchase":
        amount = raw.get("amount_cents")
        if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
            raise ValidationError("purchase requires integer amount_cents >= 0")
        record["amount_cents"] = amount
        record["order_id"] = raw.get("order_id")
    return record


def hour_bucket(received_at: str) -> str:
    """'2026-10-07T13:45:01+00:00' -> '2026-10-07T13' (UTC)."""
    dt = datetime.fromisoformat(received_at).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H")


def to_delta(record: dict) -> dict:
    """Metric increments contributed by a single event."""
    delta = {_METRIC_FOR_TYPE[record["event_type"]]: 1}
    if record["event_type"] == "purchase":
        delta["revenue_cents"] = record.get("amount_cents", 0)
    return delta


def aggregate(records) -> dict:
    """Fold events into {hour_bucket: {metric: total}}, de-duplicated by event_id."""
    out: dict[str, dict[str, int]] = {}
    seen: set[str] = set()
    for rec in records:
        if rec["event_id"] in seen:
            continue
        seen.add(rec["event_id"])
        bucket = out.setdefault(hour_bucket(rec["received_at"]), dict.fromkeys(METRICS, 0))
        for metric, n in to_delta(rec).items():
            bucket[metric] += n
    return out


def parse_lines(blob: str):
    for line in blob.splitlines():
        line = line.strip()
        if line:
            yield json.loads(line)
