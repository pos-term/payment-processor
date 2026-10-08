# payment-processor

> **Язык / Language:** Русский | [English](i18n/en/README.md)

Backend-сервис на Go: consumer Kafka с идемпотентной обработкой платёжных транзакций (дедупликация по transaction_id), REST API для статуса и истории операций

## Контракты

- [REST API](https://github.com/pos-term/infra/blob/main/api/README.md): OpenAPI-спецификация [openapi.yaml](https://github.com/pos-term/infra/blob/main/api/openapi.yaml)
- [Kafka](https://github.com/pos-term/infra/blob/main/docs/kafka/README.md): JSON Schema в [infra/schemas](https://github.com/pos-term/infra/blob/main/schemas)

> Общая информация по проекту и участниках: https://github.com/pos-term/.github/blob/main/profile/README.md
