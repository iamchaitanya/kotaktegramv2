"""
One-time script to generate a Telegram StringSession.
Run this interactively — it will ask for your phone number and a login code.
Copy the printed session string into your .env as TELEGRAM_SESSION_STRING.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from dotenv import load_dotenv

load_dotenv()

from telethon import TelegramClient
from telethon.sessions import StringSession


async def main():
    api_id = int(os.getenv("TELEGRAM_API_ID", "0"))
    api_hash = os.getenv("TELEGRAM_API_HASH", "")

    if not api_id or not api_hash:
        print("❌ Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env first")
        return

    print("Generating Telegram StringSession...")
    print("You'll be asked for your phone number and a login code.\n")

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()

    session_string = client.session.save()
    me = await client.get_me()

    print(f"\n✅ Logged in as: {me.first_name} (@{me.username})")
    print(f"\n{'='*60}")
    print(f"Add this to your .env file:\n")
    print(f'TELEGRAM_SESSION_STRING={session_string}')
    print(f"{'='*60}")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
