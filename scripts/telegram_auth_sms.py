import os
from telethon.sync import TelegramClient
from telethon.errors import SessionPasswordNeededError

c = TelegramClient(
    os.environ["TELEGRAM_SESSION_PATH"],
    int(os.environ["TELEGRAM_API_ID"]),
    os.environ["TELEGRAM_API_HASH"],
)
c.connect()
phone = input("Телефон (+7...): ").strip()
sent = c.send_code_request(phone)
print("Тип доставки:", type(sent.type).__name__)
print("Следующий способ:", type(sent.next_type).__name__ if sent.next_type else None)
print("Повторно запросить можно через (сек):", getattr(sent, "timeout", None))
if input("Запросить по SMS? (y/n): ").strip().lower() == "y":
    sent = c.send_code_request(phone, force_sms=True)
    print("Теперь тип доставки:", type(sent.type).__name__)
code = input("Код: ").strip()
try:
    c.sign_in(phone, code, phone_code_hash=sent.phone_code_hash)
except SessionPasswordNeededError:
    c.sign_in(password=input("Пароль 2FA: "))
print("Готово, вы вошли как:", c.get_me().first_name)
c.disconnect()

