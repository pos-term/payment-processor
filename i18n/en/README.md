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

## Contracts

- [REST API](https://github.com/pos-term/infra/blob/main/api/i18n/en/README.md): OpenAPI specification [openapi.yaml](https://github.com/pos-term/infra/blob/main/api/openapi.yaml)
- [Kafka](https://github.com/pos-term/infra/blob/main/docs/kafka/i18n/en/README.md): JSON Schema in [infra/schemas](https://github.com/pos-term/infra/blob/main/schemas)

> General project information and participants: https://github.com/pos-term/.github/blob/main/i18n/README_EN.md
