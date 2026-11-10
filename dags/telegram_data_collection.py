"""
DAG telegram_data_collection

Каждые 5 минут забирает НОВЫЕ сообщения из Telegram-чатов (Telethon)
и записывает их в PostgreSQL (таблица telegram_messages).

Инкрементальная загрузка: для каждого чата хранится last_message_id в collector_state.
Вставка сообщений и обновление состояния выполняются в одной транзакции,
поэтому при сбое данные не теряются и не дублируются (UPSERT по (chat_id, message_id)).
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "telegram_db"

UPSERT_MESSAGES_SQL = """
    INSERT INTO telegram_messages
        (chat_id, message_id, sender_id, text, sent_at, reply_to_msg_id, edited_at)
    VALUES %s
    ON CONFLICT (chat_id, message_id) DO UPDATE
        SET text      = EXCLUDED.text,
            edited_at = EXCLUDED.edited_at
"""

UPSERT_STATE_SQL = """
    INSERT INTO collector_state (chat_key, last_message_id, updated_at)
    VALUES (%s, %s, now())
    ON CONFLICT (chat_key) DO UPDATE
        SET last_message_id = GREATEST(collector_state.last_message_id, EXCLUDED.last_message_id),
            updated_at      = now()
"""


def _parse_chat(chat: str):
    """Числовой ID -> int, иначе @username / ссылка остаются строкой."""
    return int(chat) if chat.lstrip("-").isdigit() else chat


async def _collect(hook: PostgresHook, chats: list[str], batch_limit: int) -> dict[str, int]:
    from telethon import TelegramClient, utils
    from telethon.tl.types import MessageService

    api_id = int(os.environ["TELEGRAM_API_ID"])
    api_hash = os.environ["TELEGRAM_API_HASH"]
    session_path = os.environ["TELEGRAM_SESSION_PATH"]

    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    try:
        # В DAG интерактивный вход невозможен — он завис бы на запросе кода.
        if not await client.is_user_authorized():
            raise RuntimeError(
                "Telegram-сессия не авторизована. Один раз запусти: "
                "docker compose run --rm --no-deps airflow-scheduler "
                "python /opt/airflow/scripts/telegram_auth.py"
            )

        # Нужно, чтобы Telethon смог найти чат по числовому ID
        if any(c.lstrip("-").isdigit() for c in chats):
            await client.get_dialogs()

        result: dict[str, int] = {}
        for chat in chats:
            entity = await client.get_entity(_parse_chat(chat))
            chat_id = utils.get_peer_id(entity)

            conn = hook.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT last_message_id FROM collector_state WHERE chat_key = %s", (chat,))
                    row = cur.fetchone()
                last_id = row[0] if row else 0

                rows: list[tuple] = []
                max_id = last_id
                # reverse=True: от старых к новым; min_id: только сообщения новее last_id
                async for msg in client.iter_messages(entity, min_id=last_id, reverse=True, limit=batch_limit):
                    max_id = max(max_id, msg.id)
                    if isinstance(msg, MessageService):  # служебные: вход/выход, закрепы и т.п.
                        continue
                    rows.append(
                        (
                            chat_id,
                            msg.id,
                            msg.sender_id,
                            msg.message or None,
                            msg.date,
                            msg.reply_to.reply_to_msg_id if msg.reply_to else None,
                            msg.edit_date,
                        )
                    )

                # Одна транзакция: сообщения + новая «закладка»
                with conn:
                    with conn.cursor() as cur:
                        if rows:
                            execute_values(cur, UPSERT_MESSAGES_SQL, rows, page_size=1000)
                        if max_id > last_id:
                            cur.execute(UPSERT_STATE_SQL, (chat, max_id))
            finally:
                conn.close()

            log.info("Чат %s (id=%s): новых сообщений %d, last_message_id %d -> %d",
                     chat, chat_id, len(rows), last_id, max_id)
            result[chat] = len(rows)
        return result
    finally:
        await client.disconnect()


@dag(
    dag_id="telegram_data_collection",
    description="Инкрементальный сбор сообщений Telegram в PostgreSQL",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # SQLite-сессия Telethon не любит параллельные запуски
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["telegram", "etl"],
)
def telegram_data_collection():
    @task(execution_timeout=timedelta(minutes=10))
    def extract_and_load() -> dict[str, int]:
        chats = [c.strip() for c in os.environ.get("TELEGRAM_CHATS", "").split(",") if c.strip()]
        if not chats:
            raise ValueError("Переменная TELEGRAM_CHATS пуста — укажи хотя бы один чат в .env")
        batch_limit = int(os.environ.get("TELEGRAM_BATCH_LIMIT", "5000"))
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        return asyncio.run(_collect(hook, chats, batch_limit))

    extract_and_load()


telegram_data_collection()
