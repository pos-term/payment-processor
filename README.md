# payment-processor

> **Язык / Language:** Русский | [English](i18n/en/README.md)

Backend-сервис на Go: consumer Kafka с идемпотентной обработкой платёжных транзакций (дедупликация по transaction_id), REST API для статуса и истории операций

## Конфигурация

Сервис настраивается переменными окружения.

| Переменная | Обязательна | По умолчанию | Описание |
|---|---|---|---|
| `KAFKA_BROKERS` | да | | Адреса брокеров через запятую, `kafka:9092` |
| `POSTGRES_DSN` | да | | `postgres://user:password@host:5432/db` |
| `REDIS_ADDR` | да | | `host:port` |
| `API_TOKEN` | да | | Bearer-токен REST API |
| `HTTP_ADDR` | нет | `:8080` | Адрес HTTP-сервера |
| `LOG_LEVEL` | нет | `info` | `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | нет | `10s` | Время на корректную остановку |
| `MIGRATE_ON_START` | нет | `true` | Применять миграции Postgres при старте |

## База данных

Источник правды по платежам и истории операций: Postgres. Схема создаётся миграциями из [`internal/storage/migrations`](internal/storage/migrations), они встроены в бинарник и применяются при старте (`MIGRATE_ON_START`). Несколько экземпляров сервиса можно запускать одновременно: golang-migrate берёт advisory lock.

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

- `transactions`: платёж так, как его прислал терминал. Строка не меняется. Первичный ключ `transaction_id` защищает от дублей.
- `transaction_results`: итог обработки. Не более одного результата на транзакцию.
- Статус `pending` не хранится: нет строки в `transaction_results`, значит платёж ещё в обработке.
- История терминала отдаётся по индексу `(terminal_id, transaction_id DESC)`: UUIDv7 упорядочен по времени создания.

## Контракты

- [REST API](https://github.com/pos-term/infra/blob/main/api/README.md): OpenAPI-спецификация [openapi.yaml](https://github.com/pos-term/infra/blob/main/api/openapi.yaml)
- [Kafka](https://github.com/pos-term/infra/blob/main/docs/kafka/README.md): JSON Schema в [infra/schemas](https://github.com/pos-term/infra/blob/main/schemas)

> Общая информация по проекту и участниках: https://github.com/pos-term/.github/blob/main/profile/README.md
