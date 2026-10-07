# simple-reporting

A small, AWS-native **lambda architecture** for e-commerce / web events: HTTP ingest → batch + speed layers → one merged query API and dashboard.

```
                       ┌─► Firehose ─► S3 raw JSONL ─► Batch Lambda (hourly) ─┐
POST /events ─► Ingest │   (master dataset, immutable)   recompute, idempotent │
 (API GW+Lambda)  ─► Kinesis                                                  ▼
                       └─► Speed Lambda ─► DynamoDB "speed" ───────► Serving Lambda ─► GET /metrics
                           (dedup + counters)          DynamoDB "batch" ─►            GET /dashboard
```

**Merge rule** (`src/serving/merge.py`): the batch job stores a *watermark* (last hour it recomputed). Hours `<= watermark` are served from the batch view (authoritative); later hours come from the speed view. Both layers share the same bucketing/aggregation code (`src/common/events.py`) and bucket by the server-side `received_at`, so they agree.

## API

`POST /events` — one event or a list (max 100). Optional `x-api-key` header if `ingest_api_key` is set.

```json
{"event_type": "page_view",    "session_id": "s1", "user_id": "u1"}
{"event_type": "add_to_cart",  "session_id": "s1"}
{"event_type": "purchase",     "session_id": "s1", "order_id": "o1", "amount_cents": 4999}
```
`event_id` is optional but recommended: it makes client retries and Kinesis redeliveries idempotent.

`GET /metrics?from=ISO&to=ISO` (default last 24h) → hourly series (with `source: batch|speed`) and totals: page views, add-to-carts, orders, revenue, AOV, cart→order rate.
`GET /dashboard` — static page polling `/metrics`.

## Deploy

```
cd terraform && terraform init && terraform apply -var ingest_api_key=...
curl -X POST "$(terraform output -raw api_url)/events" -H 'x-api-key: ...' -d '{"event_type":"page_view"}'
```
Backfill/reprocess: invoke the batch Lambda with `{"backfill_from": "2026-10-01T00"}`.

## Develop

```
pip install -r requirements-dev.txt && pytest
```
Tests run against `moto`; the Terraform has not been applied/validated against a real account yet.

## Known limits (MVP)

- Metrics are global and hourly only; add dimensions (product, channel) as extra sort-key/partition-key patterns.
- The batch Lambda reads S3 directly (15 min cap). For high volume, replace `read_hour` with Athena/Glue; the output contract is unchanged.
- Batch only processes hour H once H+1 has ended (Firehose can land events under the next hour's prefix).
- Speed-layer dedup stores one item per event for 2 days; fine for moderate volume.
- Dashboard/metrics endpoints are unauthenticated; put them behind auth before exposing real data.
