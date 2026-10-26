<div align="center">

# Relay

**Publish an event once. Deliver it to the services that need it.**

A Go event relay with a public SDK, a transactional PostgreSQL outbox,
Kafka fanout, and persistent webhook delivery jobs.

**Go · Gin · GORM · PostgreSQL · Kafka · Docker Compose**

[Quick start](#quick-start) · [Architecture](#architecture) · [Go SDK](#go-sdk) · [Configuration](#configuration) · [Deployment](#deployment) · [Operations](#operations)

</div>

---

Relay lets an application publish an event such as `ride.completed` without
knowing which services will handle it. Receiving services register webhook URLs
and subscribe to event types. Relay persists each accepted event, publishes it
to Kafka, creates a delivery for each matching subscription, and retries failed
webhook attempts from PostgreSQL.

The repository includes a working Compose deployment and a reusable Go client.
The current deployment is a single-host setup with one PostgreSQL instance and
one Kafka broker. Delivery is **at least once**: receivers must handle duplicate
requests. See [implementation status](#implementation-status) for the current
boundaries and SDK caveat.

## Contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Go SDK](#go-sdk)
- [HTTP API](#http-api)
- [Webhook contract](#webhook-contract)
- [Reliability and retries](#reliability-and-retries)
- [Data model](#data-model)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Migrations](#migrations)
- [Operations](#operations)
- [Development and tests](#development-and-tests)
- [Implementation status](#implementation-status)
- [Repository layout](#repository-layout)

## Architecture

### Infrastructure view

The Relay processes run alongside the infrastructure they use. Applications
publish through HTTP; receiving services only need an HTTP webhook endpoint.
They do not need direct access to Relay's database or Kafka broker.

```mermaid
flowchart TB
    producer["Producer application<br/>Go SDK or HTTP client"]

    subgraph relay["Relay deployment · Docker Compose network"]
        api["Relay API<br/>event ingestion + subscription CRUD"]
        pg[("PostgreSQL<br/>events · outbox · subscriptions<br/>deliveries · fanout receipts · rejections")]
        dispatcher["Outbox dispatcher<br/>one replica"]
        kafka["Kafka · relay.events<br/>3 partitions"]

        subgraph pool["Consumer pool · N replicas"]
            fanout["Kafka loop<br/>create delivery jobs"]
            delivery["Delivery loop<br/>claim jobs + send webhooks"]
        end

        migrations["Migration job<br/>runs before application startup"]
        redis[("Redis<br/>provisioned; not used by delivery yet")]

        api -->|"transaction: event + outbox"| pg
        api -->|"manage subscribers + subscriptions"| pg
        pg -->|"poll due outbox rows"| dispatcher
        dispatcher -->|"publish full outbox JSON"| kafka
        dispatcher -->|"record publish result"| pg
        kafka -->|"shared consumer group"| fanout
        fanout -->|"transaction: receipt + delivery snapshots"| pg
        pg -->|"claim due jobs with a lease"| delivery
        delivery -->|"record HTTP result + retry schedule"| pg
        migrations -->|"apply versioned SQL"| pg
    end

    receiver["Subscriber services<br/>payments · notifications · analytics"]
    producer -->|"POST /"| api
    delivery -->|"HTTP POST + stable delivery ID"| receiver

    classDef app fill:#e8f1ff,stroke:#2563eb,color:#142d52;
    classDef store fill:#e8f8ef,stroke:#23865b,color:#17452e;
    classDef auxiliary fill:#f5f5f5,stroke:#888,color:#555;
    class api,dispatcher,fanout,delivery app;
    class pg,kafka store;
    class migrations,redis auxiliary;
```

| Component | Responsibility | Runs as |
|---|---|---|
| Public SDK | Publish events and manage registrations over HTTP | Library inside the caller's application |
| API | Persist events and outbox rows atomically; expose subscriber/subscription CRUD | `relay-api` container |
| Outbox dispatcher | Poll pending outbox rows, publish to Kafka, record the outcome | One `relay-worker` container |
| Kafka | Distribute published records across a shared consumer group | `kafka` container |
| Consumer | Create durable delivery jobs and independently process due jobs | N `relay-consumer` containers |
| PostgreSQL | Store event data, routing configuration, work state, and recovery metadata | `postgres` container |
| Migration runner | Apply pending SQL migrations before app startup | One-shot `relay-migrate` container |
| Redis | Available infrastructure; application integration is not implemented | `redis` container |

A **subscriber** is a receiving service and its webhook URL. A **subscription**
is a binding between that subscriber and an exact event type. One subscriber can
subscribe to many types; many subscribers can subscribe to the same type.
Routing configuration is stored centrally in PostgreSQL.

### Event lifecycle

```mermaid
%%{init: {"sequence": {"width": 110, "actorMargin": 20, "wrap": true}}}%%
sequenceDiagram
    autonumber
    participant App as Producer
    participant API as API
    participant DB as PostgreSQL
    participant Outbox as Dispatcher
    participant Kafka as Kafka
    participant Fanout as Fanout loop
    participant Jobs as Delivery loop
    participant Hook as Webhook

    App->>API: POST / with event
    API->>DB: Transaction: insert event and pending outbox
    DB-->>API: Transaction committed
    API-->>App: 200 {success: true}
    Note over App,API: Accepted into storage, webhook delivery is asynchronous

    Outbox->>DB: Poll due pending outbox rows
    DB-->>Outbox: Outbox snapshots
    Outbox->>Kafka: Publish to relay.events
    Kafka-->>Outbox: Publish acknowledged
    Outbox->>DB: Mark outbox published

    Kafka-->>Fanout: Fetch record
    Fanout->>DB: Load source outbox row
    Fanout->>DB: Transaction: insert fanout receipt and matching deliveries
    DB-->>Fanout: Durable delivery jobs
    Fanout->>Kafka: Commit processed offset
    Note over Kafka,Fanout: Offset commit follows persistence, before webhook execution

    Jobs->>DB: Claim due delivery, set token and lease, increment attempt
    DB-->>Jobs: Delivery snapshot
    Jobs->>Hook: POST payload + X-Relay-Delivery-ID
    alt HTTP 2xx
        Hook-->>Jobs: Success
        Jobs->>DB: Mark delivered and clear claim
    else HTTP error or request failure
        Hook-->>Jobs: Failure / timeout
        Jobs->>DB: Schedule retry or mark failed, clear claim
    end
```

Each consumer process runs **two independent loops**. Kafka consumption creates
jobs; database polling delivers them. A slow or failing webhook does not hold
its Kafka offset open, and already-created jobs continue retrying while Kafka
is idle or unavailable.

The dispatcher currently sends the full outbox model as JSON. The consumer
reads its `ID` and reloads the durable source row before fanout. That database
lookup is part of the current implementation.

## Quick start

### 1. Configure the deployment

Install Docker with Docker Compose v2. Run these commands from the repository
root. Docker builds the Go binaries, so a host Go installation is optional for
this path.

Create or update `.env` with your own database password:

```dotenv
POSTGRES_USER=relay
POSTGRES_PASSWORD=replace-with-your-own-password
POSTGRES_DB=relay
RELAY_API_PORT=8080
CONSUMER_REPLICAS=3
```

Keep the credentials consistent with an existing PostgreSQL volume. Compose
constructs `DATABASE_URL` and passes `PGUSER` / `PGPASSWORD` separately; you do
not need to put `DATABASE_URL` in `.env` for the supplied Compose stack. Keep
local credentials out of version control.

```sh
docker compose up --build -d relay-api relay-worker relay-consumer
docker compose ps -a
curl --fail-with-body http://localhost:8080/readyz
```

The readiness endpoint returns `{"status":"ready"}`. It reports that the API
handler is running; it does not perform a live database or Kafka health check.
If you changed `RELAY_API_PORT`, use that port in the following examples.

Compose starts dependencies, initializes Kafka volume ownership, creates the
`relay.events` topic, and applies migrations before starting the application
containers. Successful one-shot initialization containers show as exited with
code `0`.

### 2. Start a receiving service

For a local demonstration, save this as `receiver.go` **outside the Relay
repository**, then run `go run receiver.go` with Go installed:

```go
package main

import (
    "encoding/json"
    "log"
    "net/http"
    "time"
)

func main() {
    mux := http.NewServeMux()
    mux.HandleFunc("POST /webhooks", func(w http.ResponseWriter, r *http.Request) {
        defer r.Body.Close()
        var payload json.RawMessage
        if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&payload); err != nil {
            http.Error(w, "invalid JSON", http.StatusBadRequest)
            return
        }
        log.Printf("delivery=%s record=%s type=%s payload=%s",
            r.Header.Get("X-Relay-Delivery-ID"),
            r.Header.Get("X-Relay-Record-ID"),
            r.Header.Get("X-Relay-Event-Type"), payload)
        w.WriteHeader(http.StatusNoContent)
    })
    server := &http.Server{
        Addr: ":9000", Handler: mux, ReadHeaderTimeout: 5 * time.Second,
    }
    log.Println("Webhook receiver listening on :9000/webhooks")
    log.Fatal(server.ListenAndServe())
}
```

This receiver logs requests. A real receiver should durably deduplicate delivery
IDs together with its business transaction before acknowledging success.

### 3. Register a subscriber and subscription

On Docker Desktop, consumers reach the host receiver through
`host.docker.internal`. Linux and container-to-container alternatives are
covered under [network addresses](#network-addresses).

```sh
curl --fail-with-body -X POST http://localhost:8080/subscribers \
  -H 'Content-Type: application/json' \
  -d '{"name":"payments-demo","webhook_url":"http://host.docker.internal:9000/webhooks","enabled":true}'
```

Use the returned subscriber `id` in the next request. The example assumes `1`;
replace it with your actual value.

```sh
curl --fail-with-body -X POST http://localhost:8080/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"subscriber_id":1,"event_type":"ride.completed","enabled":true}'
```

Register each subscriber/type pair once. Repeating an existing name or binding
returns `409`; use the list/get/update endpoints to manage existing entries.

### 4. Publish an event

```sh
curl --fail-with-body -X POST http://localhost:8080/ \
  -H 'Content-Type: application/json' \
  -d '{"Type":"ride.completed","IdempotenceKey":"ride-123-completed","Payload":"{\"ride_id\":\"ride_123\",\"amount\":420}"}'
```

A successful API response is `{"success":true}`. The receiver should log a
request whose body is:

```json
{"ride_id":"ride_123","amount":420}
```

The event request uses **`IdempotenceKey`** with that exact spelling. `Payload`
is a **string containing your payload**, even when that string holds JSON. The
webhook receives the decoded JSON payload, without the event wrapper. The key
is currently stored as metadata; it does not suppress repeated publications.

## Go SDK

### Install into another Go project

The reusable packages are [`client/`](client/) and [`types/`](types/). Backend
implementation packages remain under `internal/`.

The current module declaration is **`module relay`**. Use a local replacement
from your application, pointing to the absolute path of this checkout:

```sh
# In your application's Go module, after go mod init if needed:
go mod edit -require=relay@v0.0.0
go mod edit -replace=relay=/absolute/path/to/distributed-relay
# Add the imports / example below, then resolve dependencies:
go mod tidy
```

Import `relay/client` and `relay/types`. The `replace` belongs in the consuming
application's `go.mod`; it is a local development setup. Installing a published
release with `go get` first requires a repository-qualified module path and
matching imports in Relay. A published module path is not configured yet.

### Register a receiver and publish

The following complete example uses the host receiver from the quick start.
Run it from a separate application module after configuring the local dependency.
Use a new subscriber name or reuse an existing registration when rerunning it.

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"

    "relay/client"
    "relay/types"
)

func main() {
    c, err := client.NewRelayClient("localhost", "8080")
    if err != nil {
        log.Fatal(err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    subscriber, err := c.RegisterSubscriber(ctx, types.SubscriberRequest{
        Name:       "payments-sdk-demo",
        WebhookURL: "http://host.docker.internal:9000/webhooks",
    })
    if err != nil {
        log.Fatal(err)
    }
    subscription, err := c.Subscribe(ctx, types.SubscriptionRequest{
        SubscriberID: subscriber.ID,
        EventType:    "ride.completed",
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("subscriber=%d subscription=%d", subscriber.ID, subscription.ID)

    payload, err := json.Marshal(map[string]any{
        "ride_id": "ride_456",
        "amount":  420,
    })
    if err != nil {
        log.Fatal(err)
    }
    result, err := c.DispatchEvent(ctx, types.Event{
        Type:           "ride.completed",
        IdempotenceKey: "ride-456-completed",
        Payload:        string(payload),
    })
    if err != nil {
        log.Fatal(err)
    }
    // See the response-handling caveat below before relying on this flag.
    log.Printf("SDK returned Dispatched=%t; inspect delivery separately", result.Dispatched)
}
```

**Current response-handling caveat:** `DispatchEvent` does not check HTTP status
or decode the API's `success` field. It can return `Dispatched: true` for a
JSON-formatted API failure. Its `Body.Content` field also does not match the
current API response. Until that behavior is corrected, use an HTTP request
that checks both the status and `success` when you need a reliable ingestion
acknowledgement. The registration/subscription methods check non-2xx statuses
and return `*client.APIError`.

Even a confirmed ingestion acknowledgement means the event was persisted; it
is not a notification that all webhooks have completed.

### SDK reference

All request methods accept a `context.Context` as their first argument.

| Method | Additional arguments | Successful result |
|---|---|---|
| `NewRelayClient` | `host, port string` | `*client.RelayClient` |
| `DispatchEvent` | `types.Event` | `client.EventResponse` |
| `RegisterSubscriber` | `types.SubscriberRequest` | `types.Subscriber` |
| `GetSubscriber` | `id int64` | `types.Subscriber` |
| `ListSubscribers` | `types.ListOptions` | `[]types.Subscriber` |
| `UpdateSubscriber` | `id int64, types.SubscriberRequest` | `types.Subscriber` |
| `DeleteSubscriber` | `id int64` | `error` only |
| `Subscribe` | `types.SubscriptionRequest` | `types.Subscription` |
| `GetSubscription` | `id int64` | `types.Subscription` |
| `ListSubscriptions` | `types.ListOptions` | `[]types.Subscription` |
| `UpdateSubscription` | `id int64, types.SubscriptionRequest` | `types.Subscription` |
| `Unsubscribe` | `id int64` | `error` only |

The constructor takes no context. Methods returning a value also return an
`error`. Lists use `types.ListOptions{}` for the default page of 100 rows; use
`types.ListOptions{Limit: 50, Offset: 50}` to paginate. List ordering is by ID.

To disable an existing subscription, pass an explicit boolean pointer and all
required editable fields:

```go
// c, ctx, and subscription are from the complete SDK example.
enabled := false
_, err = c.UpdateSubscription(ctx, subscription.ID, types.SubscriptionRequest{
    SubscriberID: subscription.SubscriberID,
    EventType:    subscription.EventType,
    Enabled:      &enabled,
})
if err != nil {
    log.Fatal(err)
}
```

Omitting `Enabled` defaults it to `true` on both create and update. Updates
replace editable fields; they are not partial patches. Disabling routing affects
future fanout, not jobs that already contain a delivery snapshot.

The constructor uses `http://host:port` and a ten-second HTTP timeout. Customize
the exported fields when using a gateway URL or different client timeout:

```go
c.RelayApiUrl = "https://relay.example.com" // Your own configured gateway.
c.HttpClient.Timeout = 15 * time.Second
```

The SDK does not retry requests automatically. For registry errors, use
`errors.As(err, &apiErr)` with `var apiErr *client.APIError` to inspect
`StatusCode` and `Message`. Retrying event publication may insert another event
because publisher idempotency is not enforced yet.

## HTTP API

All paths below are relative to the API base URL. Requests and responses use
JSON, except successful deletion responses which have no body.

| Method | Path | Purpose | Success |
|---|---|---|---|
| `GET` | `/readyz` | API process response | `200` |
| `POST` | `/` | Persist an event and a pending outbox record | `200` |
| `POST` | `/subscribers` | Register a subscriber | `201` |
| `GET` | `/subscribers` | List subscribers | `200` |
| `GET` | `/subscribers/:id` | Get a subscriber | `200` |
| `PUT` | `/subscribers/:id` | Replace editable subscriber fields | `200` |
| `DELETE` | `/subscribers/:id` | Delete a subscriber, subject to references | `204` |
| `POST` | `/subscriptions` | Bind an event type to a subscriber | `201` |
| `GET` | `/subscriptions` | List subscriptions | `200` |
| `GET` | `/subscriptions/:id` | Get a subscription | `200` |
| `PUT` | `/subscriptions/:id` | Replace editable subscription fields | `200` |
| `DELETE` | `/subscriptions/:id` | Delete a binding, subject to references | `204` |

**Event body** — the current Go type has no JSON tags, so these are its encoded
field names:

```json
{
  "Type": "ride.completed",
  "IdempotenceKey": "ride-123-completed",
  "Payload": "{\"ride_id\":\"ride_123\"}"
}
```

**Subscriber body** — used for both `POST` and `PUT`:

```json
{
  "name": "payments",
  "webhook_url": "http://payments:9000/webhooks",
  "enabled": true
}
```

**Subscription body** — used for both `POST` and `PUT`:

```json
{
  "subscriber_id": 1,
  "event_type": "ride.completed",
  "enabled": true
}
```

Registry rules:

- Subscriber names are unique. A `(subscriber_id, event_type)` binding is unique.
- Event-type matching is exact and case-sensitive. Registry inputs trim surrounding whitespace.
- Both the subscriber and subscription must be enabled when fanout runs.
- Webhook URLs must be absolute HTTP(S) URLs with a hostname, without embedded credentials or fragments.
- `PUT` requires `name` and `webhook_url`, or `subscriber_id` and `event_type`. `enabled` is optional and defaults to `true`.
- List endpoints accept `?limit=100&offset=0`: limit 1–1000, nonnegative offset. They return arrays of resource objects with IDs and timestamps.

Registry errors use `{"error":"message"}`: `400` for invalid input, `404` for a
missing resource/reference, `409` for duplicate registrations, and `500` for
other database failures. The event endpoint instead returns `{"success":false}`
with `400` for malformed JSON or a failed ingestion transaction.

Deleting a subscriber cascades to its subscriptions, but delivery history holds
foreign keys to subscriptions. Referenced rows cannot be deleted while that
history remains. Prefer `enabled: false` to retire a registration. The current
foreign-key error mapping can report a blocked deletion as `404`.

There are no event-status, delivery-status, or replay HTTP endpoints yet; inspect
those records through PostgreSQL as shown under [operations](#operations).

## Webhook contract

Relay sends an HTTP `POST` to the URL captured when the delivery job was created.

| Header | Value |
|---|---|
| `Content-Type` | `application/json` |
| `X-Relay-Delivery-ID` | Stable delivery job ID across retries |
| `X-Relay-Record-ID` | Source outbox record ID |
| `X-Relay-Event-Type` | Event type, such as `ride.completed` |

The request body is the payload alone. Valid stored JSON is sent directly;
plain text is encoded as a JSON string. A receiver can return any **2xx** status
to acknowledge the delivery. Redirects are not followed. All non-2xx responses,
including 4xx and 429, and request/network/timeout errors use the same retry
policy. `Retry-After` is not interpreted.

Use `X-Relay-Delivery-ID` as the deduplication key within a Relay deployment. A
receiver should atomically store the processed ID and its business changes,
then acknowledge. A successful HTTP request can be repeated if the consumer
crashes or cannot persist its success afterward. Relay does not currently sign
webhook requests.

## Reliability and retries

### Three durable handoffs

| Boundary | Commit point | Recovery behavior |
|---|---|---|
| API → PostgreSQL | Event and outbox row commit in one transaction | Both records exist, or neither does |
| Outbox → Kafka | Kafka acknowledgement, followed by marking the outbox published | A failed publication is retried; an uncertain result can publish a duplicate |
| Kafka → delivery jobs | Fanout receipt and all jobs commit before Kafka offset commit | Replayed records cannot create another completed fanout |

A Kafka offset is a record's position within its partition. Committing it tells
the consumer group where to resume. Relay commits after durable fanout, so
restarting a consumer cannot skip delivery-job creation after a database error.
The consumer restarts its Kafka session from committed offsets when processing
or offset commits fail.

A `consumer_fanouts` receipt freezes the first successful fanout, even if there
were no active subscriptions. Adding subscribers later does not deliver that old
event retroactively. `(record_id, subscription_id)` is also unique in deliveries.
Malformed Kafka JSON, invalid IDs, and missing source records are retained in
`consumer_rejections` before their offsets are acknowledged. A database failure
while storing the rejection leaves the message unacknowledged for retry.

### Delivery state machine

```mermaid
stateDiagram-v2
    [*] --> pending: Fanout creates job
    pending --> processing: Claim due job and increment attempt
    processing --> delivered: HTTP 2xx and result saved
    processing --> pending: Failure below limit, schedule backoff
    processing --> failed: Fifth failure or exhausted recovery budget
    note right of processing
        Expired leases can be reclaimed.
        Reclaiming uses a new token and attempt.
    end note
    delivered --> [*]
    failed --> [*]
```

Consumers atomically claim due jobs using PostgreSQL `FOR UPDATE SKIP LOCKED`.
Each claim has a random token and an expiry time. The HTTP request runs outside
the database transaction; only an owner whose token and lease are still valid
can record the result. Expired work is reclaimable by another consumer.

Attempts are counted **when claimed**, including work interrupted by a crash.
An expired final attempt is failed without another HTTP request. If saving the
HTTP result fails, the lease eventually expires and recovery uses the same
attempt budget; the previous HTTP outcome may be unknown.

| Policy | Outbox publication | Webhook delivery |
|---|---|---|
| Attempt budget | 5 recorded publish attempts | 5 claimed attempts |
| Retry delay | Fixed 5 seconds | 5, 10, 20, 40 seconds after failures 1–4 |
| Exhaustion | Marked failed on the next eligible poll | Marked failed on the fifth failure or budget-exhausted lease recovery |
| Success state | `state = 2` / published | `status = 'delivered'` |
| Failure state | `state = 3` / failed | `status = 'failed'` |

The budgets and backoff policy are code constants, not environment settings.
An outbox attempt whose database result update fails is not counted durably and
may be repeated. Terminal failed work remains available for inspection; there
is no automatic replay API. Publication and delivery do not guarantee global
ordering, per-entity ordering, or exactly-once receiver side effects.

## Data model

```mermaid
erDiagram
    subscribers ||--o{ subscriptions : owns
    subscriptions ||--o{ deliveries : targets
    outbox_record_models ||--o{ deliveries : produces
    outbox_record_models ||--o| consumer_fanouts : records_fanout

    subscribers {
        bigint id PK
        text name UK
        text webhook_url
        boolean enabled
    }
    subscriptions {
        bigint id PK
        bigint subscriber_id FK
        text event_type
        boolean enabled
    }
    outbox_record_models {
        bigint id PK
        text type
        text idempotence_key
        text payload
        smallint state
        int attempt_count
        timestamptz next_attempt_at
    }
    deliveries {
        bigint id PK
        bigint record_id FK
        bigint subscription_id FK
        text event_type
        text webhook_url
        text payload
        text status
        int attempt_count
        uuid claim_token
        timestamptz lease_expires_at
    }
    consumer_fanouts {
        bigint record_id PK,FK
        timestamptz created_at
    }
    event_models {
        bigint id PK
        text type
        text idempotence_key
        text payload
        timestamptz created_at
    }
    consumer_rejections {
        text topic PK
        int partition PK
        bigint record_offset PK
        bytea record_value
        text last_error
        timestamptz created_at
    }
```

The diagram shows the main fields; the SQL migrations are the schema reference.
`event_models` and `outbox_record_models` are inserted together, but have
independent IDs and no foreign key between them. `deliveries.record_id` refers
to the **outbox ID**, not the event table ID. Outbox states are `0` unknown,
`1` pending, `2` published, and `3` failed.

A delivery stores a snapshot of the webhook URL, event type, and payload so a
retry keeps its original destination and content. `subscription_id` identifies
the routing binding and participates in duplicate prevention; `record_id`
identifies the source publication even when payloads happen to be identical.
Changing or disabling a subscription does not rewrite existing deliveries.

Delivery records also contain `last_error`, `last_attempt_at`, `next_attempt_at`,
`delivered_at`, `created_at`, and `updated_at`. Outbox rows have their own attempt,
error, and scheduling fields. New rows default to zero attempts and
`next_attempt_at = now()`, making them immediately eligible.

Fanout receipts and deliveries reference outbox rows; deliveries reference
subscriptions. Retention or cleanup must respect those relationships. There is
no automatic history cleanup job.

## Configuration

### Compose settings

Set these in `.env` beside `docker-compose.yml`, or in the shell running Compose.
Shell values override `.env` interpolation values. Go binaries do not load
`.env` automatically.

| Variable | Default | Purpose / constraints |
|---|---|---|
| `POSTGRES_USER` | `relay` | PostgreSQL initialization user; passed to apps as `PGUSER` |
| `POSTGRES_PASSWORD` | Required | PostgreSQL initialization password; passed as `PGPASSWORD` |
| `POSTGRES_DB` | `relay` | Database name used by PostgreSQL, apps, and migrations |
| `RELAY_API_PORT` | `8080` | Host API port; published on `127.0.0.1` |
| `CONSUMER_REPLICAS` | `3` | Number of consumer containers |
| `OUTBOX_POLL_INTERVAL` | `500ms` | Positive Go duration between dispatcher polls |
| `KAFKA_PRODUCE_TIMEOUT` | `10s` | Kafka publish timeout; Go duration of at least `1s` |
| `KAFKA_CONSUMER_GROUP` | `relay-fanout` | Shared Kafka consumer group for all replicas |
| `DELIVERY_POLL_INTERVAL` | `500ms` | Positive duration to wait after no due delivery or a claim error |
| `WEBHOOK_TIMEOUT` | `10s` | Positive duration for each outgoing HTTP attempt |
| `DELIVERY_LEASE_DURATION` | `30s` | Must be strictly greater than `WEBHOOK_TIMEOUT + 10s` |

Optional `.env` tuning example:

```dotenv
OUTBOX_POLL_INTERVAL=500ms
KAFKA_PRODUCE_TIMEOUT=10s
KAFKA_CONSUMER_GROUP=relay-fanout
DELIVERY_POLL_INTERVAL=500ms
WEBHOOK_TIMEOUT=10s
DELIVERY_LEASE_DURATION=30s
```

Changing PostgreSQL initialization variables does not change users or passwords
inside an already-initialized volume. Update the database itself when rotating
existing credentials.

### Process-level settings

Compose supplies these values directly. When running binaries yourself, export
them in each process's environment. Setting a same-named key in `.env` does not
override a value hardcoded in the supplied Compose file.

| Variable | Supplied Compose value | Used by |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:5432/<POSTGRES_DB>?sslmode=disable` | API, dispatcher, consumer, migration runner |
| `PGUSER` | Value of `POSTGRES_USER` | PostgreSQL driver and migration runner |
| `PGPASSWORD` | Value of `POSTGRES_PASSWORD` | PostgreSQL driver and migration runner |
| `PORT` | `8080` | API listening port; native default is also `8080` |
| `KAFKA_ADDR` | `kafka:19092` | Dispatcher and consumers; required for consumer startup |
| `POSTGRES_ADDR` | `postgres:5432` | Passed by Compose; currently not read by application code |
| `REDIS_ADDR` | `redis:6379` | Passed by Compose; Redis integration is not implemented |

Credentials are separate from the URL so reserved password characters do not
need URL encoding. The consumer accepts comma-separated `KAFKA_ADDR` seeds.
The dispatcher currently passes the value as a single seed address; use one
address with the current dispatcher. An external PostgreSQL deployment can
supply its own connection URL and TLS settings instead of the local
`sslmode=disable` URL.

### Fixed implementation settings

These require code or Compose changes; they are **not** environment variables.

| Setting | Current value | Defined in |
|---|---|---|
| Kafka event topic | `relay.events` | Dispatcher, consumer, topic initializer |
| Topic partitions / replication factor | `3` / `1` | Compose `kafka-init` |
| Outbox dispatcher count | `1` | Deployment constraint; no row-claim coordination |
| Outbox batch size / fetch timeout | `100` / `5s` | Worker command |
| Outbox attempt limit / retry delay | `5` / `5s` | Worker and dispatcher service |
| Delivery attempt limit | `5` | Consumer results code |
| Delivery backoff | Starts at `5s`, doubles, helper capped at `5m` | Consumer results code; five-attempt budget uses delays through `40s` |
| HTTP attempts in parallel | `1` per consumer replica | Delivery loop |
| Consumer DB timeout | `5s` | Consumer configuration |
| Consumer DB pool | 10 open / 2 idle / 30-minute lifetime per replica | Consumer service |
| Kafka session restart delay | `1s` | Kafka consumer loop |
| SDK HTTP timeout | `10s`, mutable through `HttpClient.Timeout` | Public client constructor |
| Consumer shutdown grace | `20s` | Compose |
| Host dependency ports | PostgreSQL `5432`, Kafka `9092`, Redis `6379`, all loopback | Compose port mappings |

## Deployment

### Docker Compose

Build and start the complete stack:

```sh
docker compose up --build -d relay-api relay-worker relay-consumer
docker compose logs -f relay-api relay-worker relay-consumer
```

The same application image contains `/app/api`, `/app/worker`, and
`/app/consumer`. It runs as a non-root user. The separate migration image
contains the versioned SQL scripts and pinned migration executable.

Startup ordering is enforced through Compose dependencies:

1. PostgreSQL becomes healthy; the migration job applies pending migrations.
2. `kafka-storage-init` prepares named-volume ownership; Kafka becomes healthy.
3. `kafka-init` creates the topic if absent.
4. Application containers start after their database/migration and Kafka prerequisites. The API and dispatcher also wait for Redis.

PostgreSQL, Kafka, and Redis use named persistent volumes. Stop the stack while
retaining its data with:

```sh
docker compose down
```

### Scale the consumer pool

Set `CONSUMER_REPLICAS` in `.env` for a persistent choice. For a one-command
change:

```sh
CONSUMER_REPLICAS=5 docker compose up -d --no-deps relay-consumer
docker compose ps relay-consumer
```

Keep the same consumer group across replicas. With three topic partitions, at
most three replicas actively consume Kafka records at once. **All replicas can
claim database delivery jobs**, so five replicas can still run five HTTP
attempts concurrently. Scaling delivery workers does not require one
subscription map per worker.

Keep **one outbox dispatcher**: its polling query does not claim rows, so
multiple dispatchers can publish the same work concurrently. Changing
`KAFKA_CONSUMER_GROUP` starts a separate Kafka consumption history; it is not a
way to scale a shared group. Durable fanout receipts still suppress repeated
fanout across groups using the same database.

### Network addresses

An address is resolved by the process making the connection. `localhost` in a
consumer container refers to that container.

| Connection | Address with supplied Compose setup |
|---|---|
| Host application → API | `http://localhost:8080` or configured `RELAY_API_PORT` |
| Container on the Relay Compose network → API | `http://relay-api:8080` |
| Relay process → PostgreSQL | `postgres:5432` |
| Relay process → Kafka | `kafka:19092` |
| Host Go process → PostgreSQL / Kafka | `localhost:5432` / `localhost:9092` |
| Consumer → host receiver on Docker Desktop | `http://host.docker.internal:9000/webhooks` |
| Consumer → another service on a shared Docker network | Its service DNS, e.g. `http://payments:9000/webhooks` |

On Linux Docker Engine, add a `host.docker.internal:host-gateway` entry through
`extra_hosts` for `relay-consumer` if using a host-based receiver, or put the
receiver on a shared network. That host mapping is not included in the supplied
Compose file. A receiver must listen on an interface reachable by the container.
Services in another Compose project must explicitly share a network before
they can use each other's service DNS names.

### Run the Go processes on your host

Install the Go version declared in `go.mod` (`1.26.5`). Start infrastructure and
migrations through Compose:

```sh
docker compose up --build -d postgres redis kafka-init relay-migrate
```

In each terminal running a Relay binary, export the connection settings. Supply
your actual password through your shell or environment manager:

```sh
export DATABASE_URL='postgres://localhost:5432/relay?sslmode=disable'
export PGUSER='relay'
export PGPASSWORD='replace-with-your-own-password'
export KAFKA_ADDR='localhost:9092'
```

Run each command in a separate terminal:

```sh
go run ./cmd/api
go run ./cmd/worker
go run ./cmd/consumer
```

Match the user/database to your configured values. Use `localhost:9000` as the
subscriber's webhook host if both the receiver and consumer run on the host.
Avoid running a native dispatcher alongside an existing Compose dispatcher.

### Deploy within a project's infrastructure

Run Relay as a shared internal service. Producer applications call its API;
consumer replicas need network access to PostgreSQL, Kafka, and the registered
webhook destinations. The receiver address must be reachable from the consumer
network, not merely from the registering application.

The supplied stack publishes ports on host loopback. For remote applications,
configure an internal reverse proxy/load balancer or private network route to
the API. The SDK can use that gateway URL through `RelayApiUrl`. API
authentication, TLS termination, and webhook signing are not built into the
current service; an authenticated gateway and controlled subscriber registration
are deployment responsibilities. Registration accepts HTTP(S) destinations,
including private network hosts, so registration access must be trusted.

The bundled Kafka broker uses plaintext listeners and advertises
`localhost:9092` to host clients. Remote Kafka clients need a reachable advertised
listener configured explicitly. The Compose setup has one broker with
replication factor one and one PostgreSQL instance; persistent volumes provide
local persistence, not host-failure tolerance. A resilient multi-host deployment
needs separately configured durable database/broker infrastructure and matching
application connection settings. No cloud or Kubernetes deployment manifests
are included.

## Migrations

Versioned SQL lives in [`migrations/`](migrations/). The Dockerfile's `migrations`
target packages it with `golang-migrate` `v4.20.1`; the wrapper in
[`docker/migrate.sh`](docker/migrate.sh) uses `DATABASE_URL`.

| Version | Purpose |
|---|---|
| `000001` | Event and outbox tables |
| `000002` | Outbox retry metadata, scheduling index, failed state |
| `000003` | Subscribers and subscriptions |
| `000004` | Delivery snapshots and event/subscription uniqueness |
| `000005` | Claims, leases, status constraints, fanout receipts, rejected-message storage |

Apply pending migrations independently or inspect the installed version:

```sh
docker compose build relay-migrate
docker compose run --rm relay-migrate up
docker compose run --rm relay-migrate version
```

Migrations work on existing volumes, not just fresh databases. Applied versions
are tracked in `schema_migrations`; migration execution uses PostgreSQL advisory
locking. A failed migration prevents dependent application startup. Add a new
numbered migration for each schema change instead of editing applied scripts.

Rollbacks are explicit and can remove schema/data. Stop consumers and resolve
processing claims before rolling back migration 5; its down migration rejects
active processing rows. Earlier rollbacks also have data/state constraints.
Inspect any migration failure and dirty state before repairing it; forcing a
version does not repair the underlying schema.

## Operations

### Inspect processes and logs

```sh
docker compose ps -a
docker compose logs --tail=100 relay-api relay-worker relay-consumer
docker compose logs --tail=100 relay-migrate kafka-init kafka
```

Inspect work state through PostgreSQL:

```sh
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT state, count(*) FROM outbox_record_models GROUP BY state ORDER BY state;
SELECT status, count(*) FROM deliveries GROUP BY status ORDER BY status;
SELECT id, record_id, status, attempt_count, next_attempt_at, last_error
FROM deliveries
WHERE status IN ('pending', 'processing', 'failed')
ORDER BY id DESC LIMIT 20;
SELECT id, state, attempt_count, next_attempt_at, last_error
FROM outbox_record_models WHERE state = 3 ORDER BY id DESC LIMIT 20;
SELECT topic, partition, record_offset, last_error, created_at
FROM consumer_rejections ORDER BY created_at DESC LIMIT 20;
SQL
```

Check the Kafka topic and consumer group:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --describe --topic relay.events

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 --describe --group relay-fanout
```

Use your configured group name if changed. Kafka lag measures progress toward
durable fanout; it does **not** measure outstanding webhook deliveries. Inspect
the delivery backlog separately.

### Common situations

| Symptom | What to inspect |
|---|---|
| Application containers do not start | PostgreSQL health, migration exit code, Kafka initialization logs |
| API responds but no webhook arrives | Active subscription/type match, outbox state, Kafka group, delivery rows, receiver reachability |
| Outbox remains pending | Dispatcher logs, broker address, `next_attempt_at`, database result-update errors |
| Delivery returns to pending | `last_error`, attempt count, retry timestamp, receiver response |
| Delivery remains processing after a crash | Lease expiry; another consumer reclaims it or fails it if its budget is exhausted |
| More consumers than active Kafka readers | Three partitions bound active readers; other replicas still process deliveries |
| SDK says dispatched but nothing was accepted | Check the event API HTTP status/body; see the documented SDK response limitation |
| Registry deletion returns an unexpected 404 | Check foreign-key references from delivery history; disable instead of deleting |

Consumers cancel both loops on shutdown. Interrupted claims recover after their
leases expire. Terminal failures and rejected messages remain in PostgreSQL for
operator inspection; recovery/replay tooling is not implemented yet.

## Development and tests

```sh
go build ./...
go vet ./...
go test ./...
```

Tests live in the root [`test/`](test/) directory. Database integration tests
require `TEST_DATABASE_URL`; the Kafka integration test additionally requires
`TEST_KAFKA_ADDR`. Without these settings, the corresponding integration tests
are skipped, so a plain `go test ./...` does not exercise the full pipeline.

Use a dedicated test database and isolated broker with a `relay.events` topic.
The tests create and remove temporary database schemas. The Kafka test consumes
from the configured topic and must not share a broker/topic with application
traffic.

```sh
export TEST_DATABASE_URL='postgres://test_user:test_password@localhost:5432/relay_test?sslmode=disable'
# Optional: enables the Kafka end-to-end test in addition to database tests.
export TEST_KAFKA_ADDR='localhost:9092'
go test -race ./test -v
```

These are test connection examples, not infrastructure provisioned by the
application Compose stack. Point them at your dedicated test infrastructure.
`TEST_DATABASE_URL` must be a PostgreSQL URL, and the database user needs
permission to create/drop schemas. `TEST_KAFKA_ADDR` accepts comma-separated
broker seeds.

The integration suite covers atomic fanout and rollback, duplicate suppression,
frozen subscriber selection including zero matches, concurrent delivery claims,
expired leases, stale owners, attempt exhaustion, retry scheduling independent
of Kafka, and migration 5 rollback/reapplication. The Kafka test runs two
consumers, injects a database failure, verifies that offsets do not advance
before persistence, and checks recovery, delivery, and rejected-message
retention.

## Implementation status

The implemented path includes durable ingestion, outbox publication, subscriber
and subscription CRUD, coordinated consumer fanout, persistent delivery retries,
lease recovery, and configurable consumer replicas.

The following boundaries remain relevant when integrating a project:

| Area | Current behavior |
|---|---|
| SDK distribution | Public packages are importable using a local module replacement; a repository-qualified release module path is still needed |
| SDK event response | `DispatchEvent` can report success on a JSON API error; reliable response checking needs correction |
| Publisher idempotency | `IdempotenceKey` is stored, without uniqueness/deduplication enforcement |
| Event validation | JSON decoding and database constraints; no required-nonempty event-field validation or payload schema validation |
| Access controls | No built-in API authentication, tenant isolation, webhook signatures, or destination network policy |
| Rate limiting | Redis is provisioned; delivery rate limiting is not implemented |
| Ordering | No event partition key or per-subscriber serialization guarantee |
| Dispatcher scaling | One dispatcher; concurrent row claiming is not implemented for outbox publication |
| Delivery controls | Fixed attempt budget/backoff; no jitter, per-subscriber concurrency, or `Retry-After` handling |
| Inspection and replay | Logs and SQL inspection; no delivery dashboard, metrics endpoint, or replay API |
| Retention | No automatic event/outbox/delivery/rejection cleanup |
| Infrastructure availability | Single PostgreSQL instance and single Kafka broker in Compose |

These describe the current implementation; they are not configuration switches.

## Repository layout

```text
.
├── client/                    # Public HTTP SDK: publishing and registry methods
├── types/                     # Public event, subscriber, and subscription types
├── cmd/
│   ├── api/                   # Gin HTTP API entry point
│   ├── worker/                # Outbox-to-Kafka dispatcher entry point
│   └── consumer/              # Kafka fanout + delivery loops entry point
├── internal/
│   ├── ingestion/             # Transactional event/outbox persistence
│   ├── dispatcher/            # Outbox polling and publish-result persistence
│   ├── subscription/          # Registry service and HTTP handlers
│   └── consumer/              # Fanout, claims, webhook execution, and retries
├── migrations/                # Numbered up/down PostgreSQL migrations
├── docker/
│   └── migrate.sh             # Migration runner wrapper
├── test/                      # Coordination and Kafka integration tests
├── Dockerfile                 # App binaries and migration image targets
├── docker-compose.yml         # Local/single-host infrastructure and scaling
├── go.mod
└── README.md
```
