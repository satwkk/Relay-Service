# Distributed relay system

## Database migrations

SQL migrations live in `migrations/` and run using the pinned
`golang-migrate` Docker image. The `migrations` Dockerfile target packages those
scripts; the `app` target builds the API and worker.

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

This stage only fetches records and logs their IDs. Rows remain pending, so
successive polls can return the same rows. It does not claim rows, increment
attempts or publish to Kafka yet. Record ownership and processing will be added
together with Kafka dispatch.

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

## Verify migrations and subscription CRUD

```sh
go test ./...
```

For the database integration test, export `TEST_DATABASE_URL` pointing to a
dedicated PostgreSQL test database, then run:

```sh
go test -race ./internal/subscription -v
```

The test uses its own temporary schema. It applies all migrations and exercises
the client against the real HTTP handlers and PostgreSQL, including disabled
flags, pagination, uniqueness, foreign keys, cascading deletes, cancellation,
and migration 3 rollback/reapplication. Without `TEST_DATABASE_URL`, the database
test is skipped; request-validation tests still run.
