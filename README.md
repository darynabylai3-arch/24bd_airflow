# Telegram → Airflow → PostgreSQL

Первая версия пайплайна: Airflow каждые 5 минут забирает новые сообщения из Telegram (Telethon)
и инкрементально загружает их в PostgreSQL.

## Структура

```
docker-compose.yml          # PostgreSQL + Airflow (webserver, scheduler, init)
Dockerfile                  # образ Airflow + Telethon
requirements.txt
.env.example                # шаблон настроек
init-db/01_init.sql         # база telegram_data, таблицы telegram_messages и collector_state
dags/telegram_data_collection.py
scripts/telegram_auth.py    # одноразовая авторизация в Telegram
```

## Запуск

1. Получи `api_id` и `api_hash` на https://my.telegram.org → API development tools.
2. Подготовь настройки:
   ```bash
   cp .env.example .env
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # → AIRFLOW_FERNET_KEY
   python -c "import secrets; print(secrets.token_hex(32))"                                    # → AIRFLOW_SECRET_KEY
   ```
   Заполни `.env`: пароли, ключи, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_CHATS`.
3. Собери образ и инициализируй Airflow:
   ```bash
   docker compose build
   docker compose up airflow-init
   ```
4. **Один раз** авторизуйся в Telegram (спросит телефон, код и пароль 2FA):
   ```bash
   docker compose run --rm --no-deps airflow-scheduler python /opt/airflow/scripts/telegram_auth.py
   ```
   Сессия хранится в томе `telegram_session` и переживает перезапуски.
5. Запусти всё:
   ```bash
   docker compose up -d
   ```
6. Открой http://localhost:8080 (логин/пароль из `.env`), включи DAG `telegram_data_collection`
   и при желании запусти вручную кнопкой Trigger.

## Проверка данных

```bash
docker compose exec postgres psql -U airflow -d telegram_data \
  -c "SELECT count(*), max(sent_at) FROM telegram_messages;"
```

## Как работает инкрементальная загрузка

- В `collector_state` хранится `last_message_id` для каждого чата.
- Каждый запуск берёт только сообщения новее этого ID (от старых к новым), максимум `TELEGRAM_BATCH_LIMIT` за раз.
  Первый запуск догружает историю порциями по несколько запусков.
- Вставка (UPSERT) и обновление `last_message_id` идут в одной транзакции — при сбое ничего не теряется и не дублируется.

## Важно

- Схема БД создаётся только при первом создании тома. Если меняешь `init-db/01_init.sql`, пересоздай данные: `docker compose down -v` (удалит и сессию Telegram!).
- Правки уже собранных сообщений (edit) подхватываются только для сообщений из свежей порции; старые не перечитываются.
- Не публикуй `.env` и файл сессии — это доступ к твоему Telegram-аккаунту.
- `reply_to_msg_id` в чатах с топиками (форумах) может указывать на корневое сообщение топика, а не на конкретный ответ.

## Дальше

DAG `daily_chat_analytics`: число сообщений, активные пользователи, ответы, динамика активности.
