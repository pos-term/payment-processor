# payment-processor

> **Language:** English | [Русский](../../README.md)

Go backend service: Kafka consumer with idempotent transaction processing (deduplication by `transaction_id`), REST API for transaction status and history.

## Configuration

The service is configured through environment variables.

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BROKERS` | yes | | Comma-separated broker addresses, `kafka:9092` |
| `POSTGRES_DSN` | yes | | `postgres://user:password@host:5432/db` |
| `REDIS_ADDR` | yes | | `host:port` |
| `API_TOKEN` | yes | | Bearer token for the REST API |
| `HTTP_ADDR` | no | `:8080` | HTTP server address |
| `LOG_LEVEL` | no | `info` | `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | no | `10s` | Time allowed for a graceful shutdown |
| `MIGRATE_ON_START` | no | `true` | Apply Postgres migrations on startup |
| `MAX_AMOUNT_MINOR` | no | `10000000` | Payment limit in minor units (kopecks for RUB): above it the payment is `failed` with `LIMIT_EXCEEDED`. `0` disables the limit |

## Database

Postgres is the source of truth for payments and operation history. The schema is created by the migrations in [`internal/storage/migrations`](../../internal/storage/migrations); they are embedded in the binary and applied on startup (`MIGRATE_ON_START`). Several instances can start at the same time: golang-migrate takes an advisory lock.

```mermaid
erDiagram
    transactions ||--o| transaction_results : "has result"
    transactions {
        uuid transaction_id PK "UUIDv7 from the terminal"
        text terminal_id
        bigint amount_minor
        char3 currency "ISO 4217"
        bigint seq
        timestamptz created_at "terminal time"
        timestamptz received_at
    }
    transaction_results {
        uuid transaction_id PK, FK
        text status "success or failed"
        text reason "NULL on success"
        timestamptz processed_at
    }
```

- `transactions`: the payment as the terminal sent it. A row never changes. The primary key `transaction_id` protects against duplicates.
- `transaction_results`: the processing outcome. At most one result per transaction.
- The `pending` status is not stored: no row in `transaction_results` means the payment is still being processed.
- A terminal's history is served by the `(terminal_id, transaction_id DESC)` index: UUIDv7 is ordered by creation time.

## Kafka consumer

The service reads `pos.transactions.v1` in the `payment-processor` consumer group with manual offset commits (`enable.auto.commit=false`). An offset is committed only for records the handler finished without an error, so no event is lost on restart. A transient handler error is retried with a growing pause; the record is neither skipped nor committed. A new group reads the topic from the beginning.

## Invalid messages (DLQ)

A message that a retry cannot fix goes to `pos.transactions.dlq.v1`; then its offset is committed and the partition keeps moving:

| `error` | When |
|---|---|
| `invalid_json` | the value is not parseable JSON |
| `unsupported_schema_version` | `schema_version` is not `1` (checked before the other fields) |
| `validation_failed` | fields do not match the [schema](https://github.com/pos-term/infra/blob/main/schemas/pos.transactions.v1.schema.json): format, range, an extra or a missing field |

The DLQ gets the original bytes unchanged. Key: `transaction_id` if it could be read, otherwise empty. Headers: `error`, `error_detail`, `original_topic`, `original_partition`, `original_offset`, `failed_at`, and `trace_id` when present. If the DLQ itself is unavailable, the offset is not committed and the message is retried: it is not lost.

## Transaction statuses

A valid transaction is stored in Postgres as `pending` (a row in `transactions` without a row in `transaction_results`) and then gets its outcome:

| Status | `reason` | When |
|---|---|---|
| `success` | `NULL` | the amount is not above `MAX_AMOUNT_MINOR`, or the limit is off |
| `failed` | `LIMIT_EXCEEDED` | the amount is above `MAX_AMOUNT_MINOR` |

A declined payment is an ordinary business result and does not go to the DLQ. There is only one rule for now; the list of `reason` codes will grow together with INFRA#1. The outcome is written once: a second write does not change a stored result. If the service dies between storing the transaction and the result, the transaction stays `pending` and a replay of the same Kafka record completes it. Publishing the result is added by the next steps of epic PP#1.

## Tests

```bash
go test ./...   # tests that need Kafka or Postgres are skipped without the variables below
docker compose -f ../infra/docker-compose.yml up -d kafka kafka-init postgres
TEST_KAFKA_BROKERS=localhost:9094 TEST_POSTGRES_DSN='postgres://pos:pos@localhost:5432/pos?sslmode=disable' go test -race ./...
```

The consumer tests create their own topic and group.

## Contracts

- [REST API](https://github.com/pos-term/infra/blob/main/api/i18n/en/README.md): OpenAPI specification [openapi.yaml](https://github.com/pos-term/infra/blob/main/api/openapi.yaml)
- [Kafka](https://github.com/pos-term/infra/blob/main/docs/kafka/i18n/en/README.md): JSON Schema in [infra/schemas](https://github.com/pos-term/infra/blob/main/schemas)

> General project information and participants: https://github.com/pos-term/.github/blob/main/i18n/README_EN.md
