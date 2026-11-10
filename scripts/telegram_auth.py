"""
Одноразовая интерактивная авторизация в Telegram.

Запускается ВРУЧНУЮ один раз (спросит номер телефона, код из Telegram и пароль 2FA, если он включён).
Сессия сохраняется в именованный том Docker и дальше используется DAG-ом без повторной авторизации.

    docker compose run --rm --no-deps airflow-scheduler python /opt/airflow/scripts/telegram_auth.py
"""
import os

from telethon.sync import TelegramClient

api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]
session_path = os.environ.get("TELEGRAM_SESSION_PATH", "/opt/airflow/telegram_session/collector")

with TelegramClient(session_path, api_id, api_hash) as client:
    me = client.get_me()
    print(f"Авторизация успешна: {me.first_name} (id={me.id})")
    print(f"Сессия сохранена: {session_path}.session")

    chats = [c.strip() for c in os.environ.get("TELEGRAM_CHATS", "").split(",") if c.strip()]
    for chat in chats:
        key = int(chat) if chat.lstrip("-").isdigit() else chat
        try:
            entity = client.get_entity(key)
            title = getattr(entity, "title", None) or getattr(entity, "username", None) or entity.id
            print(f"Чат доступен: {chat} -> {title}")
        except Exception as exc:  # noqa: BLE001
            print(f"Не удалось получить чат {chat}: {exc}")
