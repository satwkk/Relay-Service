# Distributed relay system

## Database migrations

SQL migrations live in `migrations/` and run using the pinned
`golang-migrate` Docker image. The `migrations` Dockerfile target packages those
scripts; the `app` target builds the API, outbox worker, and consumer.

Set `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB` in `.env` (the user and
database default to `relay`). Keep existing credentials when using an existing
PostgreSQL volume.

Start the API and its dependencies:

```sh
docker compose up --build relay-api
```

Compose waits for PostgreSQL to become healthy, runs `relay-migrate` to completion,
then starts the API. The worker has the same migration prerequisite. A failed
migration blocks application startup. Existing Redis and Kafka prerequisites
remain in place.

The API and migration runner receive the same database URL. `PGUSER` and
`PGPASSWORD` supply credentials separately, so passwords containing URL-reserved
characters do not need to be interpolated into the URL. The local Compose
database uses `sslmode=disable`.

The initial migration creates `event_models` and `outbox_record_models`, matching
the current GORM model names and embedded fields. Payloads remain text. Outbox
states match the Go enum: unknown `0`, pending `1`, published `2`. Both tables
have database-generated primary keys and creation timestamps; the Go models can
expose these fields when needed. The outbox currently stores a copy of the event
fields, matching the existing model. Idempotency enforcement is not part of this
schema yet.

Migration 2 adds outbox retry metadata: `attempt_count`, `last_attempt_at`,
`last_error` and `next_attempt_at`. Existing rows start with zero attempts and
become immediately eligible for polling. Failed state `3` is also supported.
Rolling migration 2 back requires resolving any state `3` records first.

Apply pending migrations independently, including on an existing database:

```sh
docker compose build relay-migrate
docker compose run --rm relay-migrate
```

Migrations run against existing persistent volumes as well as fresh databases.
`golang-migrate` records the applied version in `schema_migrations` and uses
PostgreSQL advisory locking to coordinate migration runs. Re-running `up` at the
current version makes no schema changes. Use a new numbered migration for each
schema update instead of editing an already applied migration.

Check the installed migration version:

```sh
docker compose run --rm relay-migrate version
```

The matching `.down.sql` file drops the ingestion tables and their data. Rollback
is explicit and is never part of normal startup. If a migration fails, inspect
the migration logs and database before repairing its dirty version; do not force
a version simply to bypass a failure.

For host execution with `go run ./cmd/api`, export `DATABASE_URL` using
`localhost:5432` instead of `postgres:5432`, plus `PGUSER` and `PGPASSWORD` if
credentials are omitted from the URL. Go does not automatically load `.env`.

## Outbox polling worker

```sh
docker compose up --build relay-worker
```

The worker opens PostgreSQL once, polls immediately, then checks every 500 ms.
Set `OUTBOX_POLL_INTERVAL` (for example, `2s`) in `.env` to change the interval
under Compose. Each query has a five-second timeout and fetches at most 100
pending records whose `next_attempt_at` has arrived, ordered by that timestamp
and then row ID. Query errors are logged and polling continues. Shutdown signals
cancel the query and close the database connection pool.

The outbox worker publishes these records to `relay.events`, then records success
or a scheduled retry in PostgreSQL. `KAFKA_PRODUCE_TIMEOUT` defaults to `10s`.
Keep one outbox dispatcher instance; the consumer service below is the component
that supports multiple replicas.

## Kafka consumers and webhook delivery

Set `CONSUMER_REPLICAS=3` in `.env` to choose how many consumer containers run
(default: three), then start the full pipeline:

```sh
docker compose up --build -d relay-api relay-worker relay-consumer
```

To change the count for one invocation:

```sh
CONSUMER_REPLICAS=5 docker compose up -d --no-deps relay-consumer
```

Each replica runs two independent loops. The Kafka loop joins the shared
`relay-fanout` consumer group and creates delivery jobs. The database loop claims
one due delivery at a time and sends its webhook, including while Kafka is idle
or unavailable. Kafka assigns partitions across the group; with the current
three-partition topic, at most three replicas consume Kafka records concurrently.
All replicas can process PostgreSQL delivery jobs.

The one-shot `kafka-storage-init` service gives Kafka's user ownership of its
named volume before the broker starts, including on a fresh installation.

Migration 4 creates delivery snapshots; migration 5 adds ownership and recovery.
Fanout uses `INSERT ... SELECT` to create jobs for active subscriptions and
subscribers in one transaction. The source outbox ID is carried in Kafka and
stored as `deliveries.record_id`. A `consumer_fanouts` receipt is committed with
the jobs, so replay cannot repeat a completed fanout or change its selected
subscribers, including events with no subscribers. The unique
`(record_id, subscription_id)` constraint provides an additional duplicate guard.
Offsets are committed only after this transaction succeeds. Database or commit
failures restart the Kafka session from committed offsets. Invalid messages are
retained in `consumer_rejections` before their offsets are acknowledged.

Delivery claims use `FOR UPDATE SKIP LOCKED`, a random claim token, and a lease.
Attempts are counted when a job is claimed, including attempts interrupted by a
crash. HTTP happens after the claim transaction finishes. Only the owner of an
unexpired lease can record the result; abandoned claims are reclaimable. An
expired final attempt becomes failed without sending beyond the attempt limit.

HTTP `2xx` marks a job delivered; other responses and request errors schedule
retries after 5, 10, 20, and 40 seconds, then mark the fifth failed attempt as
failed. Each attempt carries the same `X-Relay-Delivery-ID`; receivers should
deduplicate by this ID because a successful webhook followed by a crash or a
database failure can be sent again. `X-Relay-Record-ID` and `X-Relay-Event-Type`
identify the source event. Stored JSON payloads are sent directly as JSON;
plain-text payloads are encoded as JSON strings. Redirects are not followed.

| Setting | Default | Purpose |
|---|---|---|
| `CONSUMER_REPLICAS` | `3` | Consumer container count |
| `KAFKA_CONSUMER_GROUP` | `relay-fanout` | Shared group for all replicas |
| `DELIVERY_POLL_INTERVAL` | `500ms` | Wait when no jobs are due |
| `WEBHOOK_TIMEOUT` | `10s` | HTTP attempt timeout |
| `DELIVERY_LEASE_DURATION` | `30s` | Claim recovery deadline |

The lease must exceed the HTTP timeout plus ten seconds for database operations.
Graceful shutdown cancels both loops; unfinished deliveries recover after their
leases expire. Stop consumers and resolve processing claims before rolling back
migration 5. Outbox records, subscriptions, and subscribers referenced by delivery
history must be retained until that history is explicitly removed. Changing the
consumer group creates a separate consumption history.

## Subscriber and subscription APIs

Subscribers register receiving services; subscriptions bind event types to those
services. Migration 3 creates both tables. Rebuild and run `relay-migrate` before
using these endpoints (or start the API with `docker compose up --build relay-api`).

| Method | Path | Behavior |
|---|---|---|
| POST | `/subscribers` | Register a service and webhook URL |
| GET | `/subscribers` | List subscribers |
| GET | `/subscribers/:id` | Read a subscriber |
| PUT | `/subscribers/:id` | Replace its editable fields |
| DELETE | `/subscribers/:id` | Delete it and its subscriptions |
| POST | `/subscriptions` | Bind an event type to a subscriber |
| GET | `/subscriptions` | List subscriptions |
| GET | `/subscriptions/:id` | Read a subscription |
| PUT | `/subscriptions/:id` | Replace its editable fields |
| DELETE | `/subscriptions/:id` | Remove a binding |

Example request bodies:

```json
{"name":"payments","webhook_url":"http://payments/webhooks","enabled":true}
```

```json
{"subscriber_id":1,"event_type":"ride.completed","enabled":true}
```

Both POST and PUT accept these bodies. PUT requires all editable fields except
`enabled`, which defaults to `true` when omitted. Explicit `false` disables a
subscriber or subscription. Webhook URLs must be absolute HTTP(S) URLs without
credentials or fragments. Event types are exact, case-sensitive strings;
surrounding whitespace is removed. Subscriber names are unique, and each
`(subscriber_id, event_type)` binding is unique.

Create returns `201`, reads/updates return `200`, and deletion returns an empty
`204` response. Lists return JSON arrays in ID order and support `?limit=100&offset=0`
(default limit 100, maximum 1000). Invalid input returns `400`, missing resources
or referenced subscribers return `404`, and duplicates return `409`. Errors have
the shape `{"error":"message"}`.

The existing Go client exposes the same operations:

```go
subscriber, err := c.RegisterSubscriber(ctx, types.SubscriberRequest{
    Name: "payments",
    WebhookURL: "http://payments/webhooks",
})
if err != nil {
    return err
}
_, err = c.Subscribe(ctx, types.SubscriptionRequest{
    SubscriberID: subscriber.ID,
    EventType: "ride.completed",
})
```

Use `GetSubscriber`, `ListSubscribers`, `UpdateSubscriber`, `DeleteSubscriber`,
`GetSubscription`, `ListSubscriptions`, `UpdateSubscription`, and `Unsubscribe`
for the remaining operations. List methods take `types.ListOptions{}` for the
default page. Non-success HTTP responses return `*client.APIError`, including
the status code and server message. Registration persists configuration;
fanout and webhook delivery are separate components.

## Verification

```sh
go test ./...
```

For the database integration test, export `TEST_DATABASE_URL` pointing to a
dedicated PostgreSQL test database, then run:

```sh
go test -race ./test -v
```

The tests use temporary schemas and check atomic fanout, concurrent duplicate
events, frozen subscriber selection, concurrent claims, retry scheduling while
Kafka is idle, stale-owner protection, lease recovery, attempt exhaustion, and
migration 5 rollback/reapplication. The Kafka test additionally requires
`TEST_KAFKA_ADDR` pointing to an isolated broker with a `relay.events` topic. It
runs two consumers, injects a database failure, checks that offsets do not advance
before persistence, then verifies recovery, duplicate suppression, webhook
delivery, rejected-message retention, and committed offsets. Integration tests
are skipped when their connection variables are absent.
