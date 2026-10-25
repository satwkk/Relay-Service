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

## Verify migrations

```sh
go test ./...
```

For the database integration test, export `TEST_DATABASE_URL` pointing to a
dedicated PostgreSQL test database, then run:

```sh
go test -race ./internal/ingestion -v
```

The test uses its own temporary schema. It checks migration up/down, GORM field
mapping, pending outbox insertion and rollback of the event when the outbox
insert fails. Without `TEST_DATABASE_URL`, the database test is skipped.
