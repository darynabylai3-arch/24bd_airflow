import asyncio
import os

import qrcode
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError


async def main():
    client = TelegramClient(
        os.environ["TELEGRAM_SESSION_PATH"],
        int(os.environ["TELEGRAM_API_ID"]),
        os.environ["TELEGRAM_API_HASH"],
    )
    await client.connect()
    try:
        if not await client.is_user_authorized():
            qr = await client.qr_login()
            while True:
                q = qrcode.QRCode()
                q.add_data(qr.url)
                q.print_ascii(invert=True)
                print("Telegram на телефоне → Настройки → Устройства → Подключить устройство → наведите камеру на QR")
                try:
                    await qr.wait(timeout=30)
                    break
                except asyncio.TimeoutError:
                    await qr.recreate()  # QR живёт ~30 секунд, обновляем
                    print("\nQR обновлён, сканируйте новый:\n")
                except SessionPasswordNeededError:
                    await client.sign_in(password=input("Пароль 2FA: "))
                    break
        me = await client.get_me()
        print(f"Готово, вы вошли как: {me.first_name} (id={me.id})")
    finally:
        await client.disconnect()


asyncio.run(main())