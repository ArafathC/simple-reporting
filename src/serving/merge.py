"""Serving-layer merge: batch view where available, speed view for the rest."""
from datetime import datetime, timedelta, timezone

from common.events import METRICS

HOUR_FMT = "%Y-%m-%dT%H"


def hours_between(start: datetime, end: datetime):
    cur = start.replace(minute=0, second=0, microsecond=0)
    while cur <= end:
        yield cur.strftime(HOUR_FMT)
        cur += timedelta(hours=1)


def merge(hours, batch_rows: dict, speed_rows: dict, watermark: str | None):
    """Return one point per hour. Hours <= watermark come from batch (the
    authoritative recomputation, zeros if no events); later hours come from
    the speed layer."""
    series = []
    for h in hours:
        batch_final = watermark is not None and h <= watermark
        src = batch_rows if batch_final else speed_rows
        row = src.get(h, {})
        point = {"hour": h, "source": "batch" if batch_final else "speed"}
        point.update({m: int(row.get(m, 0)) for m in METRICS})
        series.append(point)
    return series


def summarize(series):
    totals = {m: sum(p[m] for p in series) for m in METRICS}
    totals["avg_order_value_cents"] = (
        totals["revenue_cents"] // totals["orders"] if totals["orders"] else 0)
    totals["cart_to_order_rate"] = (
        round(totals["orders"] / totals["add_to_carts"], 4) if totals["add_to_carts"] else 0.0)
    return totals


def parse_time(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
