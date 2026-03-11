"""Configuration module — loads all settings from .env"""
import os
from dotenv import load_dotenv  # type: ignore

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))


class Config:
    # Kotak Neo
    KOTAK_CONSUMER_KEY = os.getenv("KOTAK_CONSUMER_KEY", "")
    KOTAK_MOBILE_NUMBER = os.getenv("KOTAK_MOBILE_NUMBER", "")
    KOTAK_MPIN = os.getenv("KOTAK_MPIN", "")
    KOTAK_CLIENT_ID = os.getenv("KOTAK_CLIENT_ID", "")
    KOTAK_TOTP_SECRET = os.getenv("KOTAK_TOTP_SECRET", "")

    # Telegram (Telethon — user client)
    TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID", "")
    TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
    TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")
    TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "")

    # Trading
    TRADING_MODE = os.getenv("TRADING_MODE", "paper")  # paper | real
    DEFAULT_LOT_SIZE = int(os.getenv("DEFAULT_LOT_SIZE", "15"))
    MAX_RISK_PER_TRADE = float(os.getenv("MAX_RISK_PER_TRADE", "5000"))

    # Server
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))
    FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")
