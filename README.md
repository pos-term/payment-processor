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
| `DEDUP_TTL` | нет | `24h` | Время жизни ключа дедупликации в Redis |
| `MAX_AMOUNT_MINOR` | нет | `10000000` | Лимит платежа в минорных единицах (копейки для RUB): больше значит `failed` с `LIMIT_EXCEEDED`. `0` отключает лимит |

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

## Kafka consumer

Сервис читает `pos.transactions.v1` в consumer-группе `payment-processor` с ручным коммитом оффсетов (`enable.auto.commit=false`). Оффсет коммитится только для записей, которые обработчик завершил без ошибки, поэтому при перезапуске ни одно событие не теряется. Временная ошибка обработчика повторяется с растущей паузой, запись не пропускается и не коммитится. Новая группа читает топик с начала.

## Невалидные сообщения (DLQ)

Сообщение, которое повтор не исправит, уходит в `pos.transactions.dlq.v1`, после чего его оффсет коммитится и партиция не блокируется:

| `error` | Когда |
|---|---|
| `invalid_json` | значение не разбирается как JSON |
| `unsupported_schema_version` | `schema_version` не равна `1` (проверяется раньше остальных полей) |
| `validation_failed` | поля не соответствуют [схеме](https://github.com/pos-term/infra/blob/main/schemas/pos.transactions.v1.schema.json): формат, диапазон, лишнее или пропущенное поле |

В DLQ уходят исходные байты без изменений. Ключ: `transaction_id`, если его удалось прочитать, иначе пусто. Заголовки: `error`, `error_detail`, `original_topic`, `original_partition`, `original_offset`, `failed_at` и `trace_id`, если он был. Если сам DLQ недоступен, оффсет не коммитится и сообщение повторяется: оно не теряется.

## Статусы транзакций

Валидная транзакция сохраняется в Postgres как `pending` (строка в `transactions` без строки в `transaction_results`), затем получает итог:

| Статус | `reason` | Когда |
|---|---|---|
| `success` | `NULL` | сумма не больше `MAX_AMOUNT_MINOR` или лимит выключен |
| `failed` | `LIMIT_EXCEEDED` | сумма больше `MAX_AMOUNT_MINOR` |

Отказ платежа это обычный бизнес-результат, в DLQ он не уходит. Правило пока одно, список кодов `reason` будет расширен вместе с INFRA#1. Итог пишется один раз: повторная запись не меняет уже сохранённый результат. Если сервис упал между сохранением транзакции и результатом, транзакция остаётся `pending`, повтор той же записи из Kafka доводит её до итога. Результат публикуется в Kafka, см. следующий раздел.

## Дедупликация

Терминал может повторить отправку, и платёж не должен обработаться дважды. Два уровня, оба по `transaction_id`:

1. **Redis** (быстрый фильтр): `SET dedup:tx:<id> NX` с TTL `DEDUP_TTL`. Ключ уже есть значит id уже видели.
2. **Postgres** (источник правды): первичный ключ `transactions.transaction_id` и единственный результат в `transaction_results`.

Redis это подсказка, а не гарантия, поэтому на нём ничего не держится:

- Повтор, который Redis узнал, всё равно проверяется по Postgres. Для транзакции с сохранённым результатом платёж не решается заново, терминалу уходит сохранённый результат. Если результата нет (сервис упал после записи в Redis, но до записи в Postgres, или посередине), повтор доводит платёж до конца.
- Redis сброшен или ключ истёк: id снова «новый», но Postgres уже знает транзакцию и результат, второй раз платёж не решается.
- Redis недоступен: предупреждение в логе и работа только на Postgres.

Сервис стартует и при недоступном Redis.

## Публикация результатов

Итог каждой валидной транзакции (`success` или `failed`) публикуется в `pos.transaction-results.v1`, чтобы терминал показал клиенту результат:

- ключ `terminal_id`, значение по [схеме](https://github.com/pos-term/infra/blob/main/schemas/pos.transaction-results.v1.schema.json): `schema_version`, `transaction_id`, `terminal_id`, `status`, `reason` (`null` при успехе), `processed_at` (UTC, миллисекунды);
- заголовки `event_type = transaction.processed` и тот же `trace_id`, что во входном сообщении;
- публикуется то, что сохранено в Postgres, а не пересчитанное заново, поэтому повторная публикация даёт точно такое же сообщение;
- `pending` не публикуется, в топике только финальные статусы; сообщение из DLQ результата не порождает.

Публикация завершается подтверждением брокера (`acks=all`), и только после этого оффсет входного сообщения коммитится. Если публикация не удалась, оффсет не коммитится, повтор записи берёт готовый результат из Postgres и публикует его, платёж второй раз не решается. Повторное сообщение с уже обработанным `transaction_id` пока публикует сохранённый результат заново: результат может прийти терминалу больше одного раза, но не теряется. Терминал игнорирует уже обработанный `transaction_id`.

## Тесты

```bash
go test ./...   # тесты, которым нужны Kafka или Postgres, пропускаются без переменных ниже
docker compose -f ../infra/docker-compose.yml up -d kafka kafka-init postgres redis
TEST_KAFKA_BROKERS=localhost:9094 TEST_POSTGRES_DSN='postgres://pos:pos@localhost:5432/pos?sslmode=disable' TEST_REDIS_ADDR=localhost:6379 go test -race ./...
```

Тесты consumer создают свой топик и группу.

## Контракты

- [REST API](https://github.com/pos-term/infra/blob/main/api/README.md): OpenAPI-спецификация [openapi.yaml](https://github.com/pos-term/infra/blob/main/api/openapi.yaml)
- [Kafka](https://github.com/pos-term/infra/blob/main/docs/kafka/README.md): JSON Schema в [infra/schemas](https://github.com/pos-term/infra/blob/main/schemas)

> Общая информация по проекту и участниках: https://github.com/pos-term/.github/blob/main/profile/README.md
