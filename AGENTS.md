# AGENTS.md

Guidance for AI coding agents working in `payment-processor`. The behaviour of the service is described in [README.md](README.md) (Russian, English copy in [i18n/en/README.md](i18n/en/README.md)); this file covers how to work on the code and which invariants must not break. Read the README sections you touch before changing them.

## What this is

A Go service (module `github.com/pos-term/payment-processor`, Go 1.26) that consumes payment transactions from Kafka, decides them, stores everything in Postgres, publishes results back to Kafka and (eventually) serves a REST API. Redis is used only as a fast duplicate filter.

Part of the [pos-term](https://github.com/pos-term) organization: a system for accepting and reliably processing payment transactions with a POS terminal emulator (project description and team: [.github profile](https://github.com/pos-term/.github/blob/main/profile/README.md), in Russian). The target is a working prototype by December 2026. Sibling repos, expected to be checked out next to this one:

- [infra](https://github.com/pos-term/infra) (`../infra`): docker-compose, Kafka topics, JSON Schemas, OpenAPI
- [term-emulator](https://github.com/pos-term/term-emulator) (`../term-emulator`)
- [mobile-dashboard](https://github.com/pos-term/mobile-dashboard) (`../mobile-dashboard`)

## Commands

```bash
go build ./...
go vet ./...
gofmt -l .                # must print nothing
go test ./...             # fast; tests that need Kafka/Postgres/Redis skip themselves

# Full run, including internal/integration:
docker compose -f ../infra/docker-compose.yml up -d kafka kafka-init postgres redis
TEST_KAFKA_BROKERS=localhost:9094 \
TEST_POSTGRES_DSN='postgres://pos:pos@localhost:5432/pos?sslmode=disable' \
TEST_REDIS_ADDR=localhost:6379 \
go test -race ./...

# Run the service locally (the stack from the infra repo must be up):
export KAFKA_BROKERS=localhost:9094 POSTGRES_DSN='postgres://pos:pos@localhost:5432/pos?sslmode=disable' REDIS_ADDR=localhost:6379 API_TOKEN=dev-token
go run ./cmd/payment-processor
```

Before declaring a change done: `gofmt`, `go vet`, and the full `-race` run above. A skipped integration test is not a passing one; say so if the stack was not available.

## Layout

| Path | Role |
|---|---|
| `cmd/payment-processor/main.go` | Wiring only: config, logger, migrations, store, Redis, consumer, HTTP server. Consumer and HTTP server run in one `errgroup`: either failing stops the other. |
| `internal/config` | Env-var loading. Collects all problems and returns them together (`errors.Join`). Takes a `lookup` func so tests do not touch the real environment. |
| `internal/consumer` | franz-go client: consume loop, retry/backoff, offset commits, producing results and DLQ messages. Knows Kafka, not business rules. |
| `internal/processor` | The business flow: `Parse` and validate, dedup, decide, store, deliver. Depends on `Store`, `Deduper`, `Publisher` interfaces so it is testable without infrastructure. |
| `internal/storage` | pgx pool, queries, embedded golang-migrate migrations. |
| `internal/httpserver` | `net/http` `ServeMux` (Go 1.22+ `"METHOD /path"` patterns). All routes are bound in `routes.go`. |
| `internal/integration` | End-to-end tests against real Kafka, Postgres and Redis (dedup, DLQ, results, status, scale-out). |

## Invariants (do not break)

These are the guarantees the README promises. Several of them are only visible by reading the order of calls, so a refactor can break them without any test turning red; keep the integration tests green and add one when you touch this area.

1. **Delivery order.** For every record: result saved in `transaction_results` -> result published and acknowledged by the broker (`acks=all`) -> `published_at` set in Postgres -> input offset committed. Do not reorder, batch across steps, or commit earlier. See `Processor.deliver`.
2. **Handler contract.** `Handler.Handle` returns `nil` only if the offset may be committed. Any other error means transient: the consumer retries the same record and does not commit. Never return `nil` to "skip" a failure.
3. **No skipping, no auto-commit.** `enable.auto.commit` stays off. A failing record blocks its partition and is retried with exponential backoff (200ms doubling to 5s); that is intended. `BlockRebalanceOnPoll` is on and `AllowRebalance` must be called on every path that leaves the poll loop.
4. **Postgres is the source of truth, Redis is a hint.** A payment is decided at most once, enforced by the `transactions` primary key and a single row in `transaction_results`. A Redis answer (`FirstSeen`) may only save work; it must never be the reason a payment is lost or processed twice. Redis errors degrade to Postgres-only with a warning, and the service must still start with Redis down.
5. **Immutability.** A `transactions` row is never updated. A result is written once; a repeated save must not change it, and after saving the code reads the stored result back and publishes *that* (a republish after a crash must be byte-for-byte equivalent).
6. **`pending` is derived.** There is no `pending` status in the database or in Kafka: it means "transaction row without a result row". Never write it, never publish it.
7. **DLQ is only for unfixable input.** `invalid_json`, `unsupported_schema_version` (checked before other fields), `validation_failed`. The original bytes go unchanged. If the DLQ publish fails, the offset is not committed. A business refusal (`failed` / `LIMIT_EXCEEDED`) is a normal result, never a DLQ message.
8. **Keying.** Input and results are keyed by `terminal_id` (per-terminal ordering, one partition, one instance). DLQ is keyed by `transaction_id` when readable. Do not change keys without checking the [Kafka docs](https://github.com/pos-term/infra/blob/main/docs/kafka/README.md).
9. **Scale limit.** At most 3 instances are useful: `pos.transactions.v1` has 3 partitions. Do not add process-local state that assumes a single instance.
10. **No transactional outbox.** The at-least-once design plus terminal-side idempotency is deliberate (see README, "Гарантия доставки"). Do not introduce an outbox unless asked.

## Contracts live in the infra repo

Message formats and the REST API are defined in [pos-term/infra](https://github.com/pos-term/infra), not here:

- Kafka topics and docs: [docs/kafka/README.md](https://github.com/pos-term/infra/blob/main/docs/kafka/README.md)
- JSON Schemas: [pos.transactions.v1](https://github.com/pos-term/infra/blob/main/schemas/pos.transactions.v1.schema.json), [pos.transaction-results.v1](https://github.com/pos-term/infra/blob/main/schemas/pos.transaction-results.v1.schema.json)
- OpenAPI: [api/openapi.yaml](https://github.com/pos-term/infra/blob/main/api/openapi.yaml)

If a change alters a message or an endpoint, the contract change goes into `infra` first (separate repo, separate PR), and the code follows it. `resultMessage` in `internal/consumer` and `Parse` in `internal/processor/message.go` must match those schemas exactly.

## Conventions

**Code**
- Match the surrounding style: short doc comments that explain *why* (especially around ordering and failure modes), no restating of the code. Existing comments are in English.
- Wrap errors with context (`fmt.Errorf("...: %w", err)`); use `errors.As` for typed errors such as `BadMessageError`.
- Logging is structured JSON via `log/slog` to stdout. Pass the logger in; do not use the global one in packages. Processing logs carry `transaction_id`, `terminal_id`, `trace_id`. Use `Warn` for degraded-but-working states (Redis down, bad message), `Error` for retried failures.
- Time: store and publish UTC. Published timestamps use `2006-01-02T15:04:05.000Z` (milliseconds). Inject `now` for testability instead of calling `time.Now` inline in logic.
- Amounts are integers in minor units (`amount_minor`); never floats.
- Dependencies are interfaces defined by the consumer of the dependency (see `processor.Store`, `Deduper`, `Publisher`). Keep it that way.

**Configuration**
- Env vars only. A new variable means: `internal/config/config.go`, a case in `config_test.go`, the table in `README.md` and `i18n/en/README.md`, and the [infra](https://github.com/pos-term/infra) repo (`.env.example` / compose) if the deployment needs to set it.
- Validation errors are collected, not returned one at a time.

**Database**
- Add a new pair `NNNNNN_name.up.sql` / `.down.sql` in `internal/storage/migrations`. Never edit an applied migration. Migrations are embedded in the binary and run at start (`MIGRATE_ON_START`), concurrently safe via golang-migrate's advisory lock.
- Update the ER diagram in both READMEs when the schema changes.

**Tests**
- Unit tests next to the code with fakes for `Store` / `Deduper` / `Publisher`.
- Integration tests in `internal/integration`; they create their own topic and consumer group and skip without `TEST_*` variables. When asserting "processed once", count actual decisions (see `countingStore`): the primary key silently ignores a second insert and would hide double processing.
- Run with `-race`.

**Docs**
- README is bilingual. Russian is primary; update `i18n/en/README.md` in the same commit.

**Git**
- Branches: `feature/...`, `docs/...`, `chore/...`, `fix/...`. Merge through PRs into `main`; do not push to `main` directly.
- Commit messages: `type(payment-processor): summary` (e.g. `feat(payment-processor): publish transaction results to Kafka`).
- `.env` is git-ignored; never commit secrets.

## Status checklist

Keep this section current: tick items in the same PR that finishes them.

### Done

- [x] Service skeleton: config from env, JSON logging, graceful shutdown, `GET /healthz`, Dockerfile (distroless)
- [x] Postgres schema and embedded migrations (`transactions`, `transaction_results`, `published_at`)
- [x] Kafka consumer: group `payment-processor`, manual commits, retry with backoff
- [x] DLQ for invalid JSON, unsupported schema version and failed validation
- [x] Transaction status tracking in Postgres (`pending` -> `success` / `failed`)
- [x] Business rule: amount limit (`MAX_AMOUNT_MINOR`, reason `LIMIT_EXCEEDED`)
- [x] Two-level deduplication (Redis filter + Postgres source of truth), Redis outage tolerated
- [x] Publishing results to `pos.transaction-results.v1` with at-least-once delivery
- [x] Scale-out to 3 instances verified by integration test
- [x] Integration tests: dedup, DLQ, results, status, scale

### To do

- [ ] REST API from [infra/api/openapi.yaml](https://github.com/pos-term/infra/blob/main/api/openapi.yaml): only `GET /healthz` exists. Missing: `GET /v1/transactions`, `GET /v1/transactions/{transaction_id}`, `GET /v1/transactions/stream`, `GET /v1/terminals`
- [ ] Bearer-token auth for the API: `API_TOKEN` is required and validated but nothing checks it yet (`/healthz` must stay open)
- [ ] Terminal history endpoint on top of the existing index `(terminal_id, transaction_id DESC)` with pagination
- [ ] More business rules and `reason` codes (the list is to be extended together with INFRA#1; today only `LIMIT_EXCEEDED`)
- [ ] Readiness probe (Postgres / Kafka reachability) separate from the liveness `/healthz`
- [ ] Metrics (processed / failed / DLQ counts, consumer lag, retry count)
- [ ] Load testing of the transaction processing pipeline (a project goal from the [.github profile](https://github.com/pos-term/.github/blob/main/profile/README.md)): define the target load (terminals, TPS), build a generator that publishes to `pos.transactions.v1`, measure throughput, end-to-end latency (input to result in `pos.transaction-results.v1`), consumer lag and behaviour with 1 to 3 instances, and record the results in the docs. Today only correctness under scale-out is tested, not performance
- [ ] CI: build, vet, gofmt, and tests with services (no workflow files in the repo yet)
- [ ] Publish the Docker image to a registry so [infra](https://github.com/pos-term/infra) no longer builds from the sibling directory
- [ ] Optional: a DLQ replay tool or documented manual procedure

When you finish an item, move it to Done, and update the README (both languages) if behaviour changed.
