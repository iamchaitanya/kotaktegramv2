"""
Kotak Neo Trader — API wrapper for authentication and order management.
Uses neo_api_client with the new access_token auth flow (no consumer_secret).
"""
import logging
import time
from datetime import datetime
from typing import Optional

try:
    from neo_api_client import NeoAPI
except ImportError:
    NeoAPI = None

try:
    import pyotp
except ImportError:
    pyotp = None

from .config import Config

log = logging.getLogger(__name__)


class KotakTrader:
    """Wraps Kotak Neo API for auth, order placement, and data retrieval."""

    def __init__(self):
        self.client: Optional[NeoAPI] = None
        self.is_authenticated = False
        self.session_active = False
        self._last_login: Optional[datetime] = None
        self._last_error: Optional[str] = None

    # ── Authentication ──

    def initialize(self):
        """Create NeoAPI client with consumer_key."""
        if NeoAPI is None:
            log.warning("neo_api_client not installed — running in offline mode")
            self._last_error = "neo_api_client not installed"
            return False

        try:
            self.client = NeoAPI(
                environment="prod",
                consumer_key=Config.KOTAK_CONSUMER_KEY,
            )
            log.info("NeoAPI client initialized")
            return True
        except Exception as e:
            msg = f"Failed to initialize NeoAPI: {e}"
            log.error(msg)
            self._last_error = str(e)
            return False

    def login(self, retries: int = 1) -> dict:
        """Authenticate with Kotak Neo using TOTP + MPIN.

        Adds light retry logic and structured logging so we can distinguish
        between config issues, dependency problems, and transient API failures.
        """
        # Short-circuit if we already have an active session
        if self.session_active and self.is_authenticated:
            log.info("Kotak login called but session is already active; reusing existing session.")
            return {
                "status": "ok",
                "message": "Already authenticated",
                "data": {"last_login": self._last_login.isoformat() if self._last_login else None},
            }

        if not self.client:
            if not self.initialize():
                return {"status": "error", "message": "Client not initialized"}

        # Auto-generate TOTP
        if not Config.KOTAK_TOTP_SECRET:
            self._last_error = "KOTAK_TOTP_SECRET not configured"
            log.error("Cannot login — KOTAK_TOTP_SECRET not configured")
            return {"status": "error", "message": "KOTAK_TOTP_SECRET not configured"}

        if not pyotp:
            self._last_error = "pyotp not installed"
            log.error("Cannot login — pyotp not installed")
            return {"status": "error", "message": "pyotp not installed"}

        attempt = 0
        while True:
            attempt += 1
            try:
                totp_code = pyotp.TOTP(Config.KOTAK_TOTP_SECRET).now()
                log.info("Auto-generated TOTP code (len=%s) [attempt %s]", len(str(totp_code)), attempt)

                result = self.client.totp_login(
                    mobile_number=Config.KOTAK_MOBILE_NUMBER,
                    ucc=Config.KOTAK_CLIENT_ID,
                    totp=totp_code,
                )
                log.info("totp_login result: %s", result)

                # Check for error response (SDK sometimes uses 'Error' instead of 'error')
                if isinstance(result, dict) and (result.get("error") or result.get("Error")):
                    raise Exception(str(result.get("error") or result.get("Error")))

                # Brief pause for Kotak's backend before validating MPIN
                time.sleep(1)

                # Step 2: Validate with MPIN
                result2 = self.client.totp_validate(mpin=Config.KOTAK_MPIN)
                log.info("totp_validate result: %s", result2)

                if isinstance(result2, dict) and (result2.get("error") or result2.get("Error")):
                    raise Exception(str(result2.get("error") or result2.get("Error")))

                self.is_authenticated = True
                self.session_active = True
                self._last_login = datetime.now()
                self._last_error = None
                log.info("✅ Kotak Neo login successful!")
                return {"status": "ok", "message": "Authenticated successfully", "data": result2}

            except Exception as e:
                self.is_authenticated = False
                self.session_active = False
                self._last_error = str(e)
                log.error("Login failed on attempt %s: %s", attempt, e)

                if attempt > retries:
                    return {"status": "error", "message": str(e)}

                # Simple bounded backoff before retrying
                time.sleep(2)

    def complete_2fa(self, otp: str = None) -> dict:
        """Legacy 2FA — now handled automatically in login()."""
        return self.login()

    # ── Websocket Callbacks (for market feed) ──

    def setup_callbacks(self, on_message, on_error=None, on_close=None, on_open=None):
        """Wire up websocket callbacks for live tick data."""
        if not self.client:
            return
        self.client.on_message = on_message
        self.client.on_error = on_error or (lambda e: log.error(f"WS error: {e}"))
        self.client.on_close = on_close or (lambda m: log.info(f"WS closed: {m}"))
        self.client.on_open = on_open or (lambda m: log.info(f"WS open: {m}"))

    def subscribe(self, instrument_tokens: list, is_index: bool = False):
        """Subscribe to live market data for given instruments."""
        if not self.client or not self.is_authenticated:
            log.warning("Cannot subscribe — not authenticated")
            return
        try:
            self.client.subscribe(
                instrument_tokens=instrument_tokens,
                isIndex=is_index,
                isDepth=False,
            )
            log.info(f"Subscribed to {len(instrument_tokens)} instruments")
        except Exception as e:
            log.error(f"Subscribe failed: {e}")

    def unsubscribe(self, instrument_tokens: list):
        """Unsubscribe from market data."""
        if not self.client:
            return
        try:
            self.client.un_subscribe(instrument_tokens=instrument_tokens)
        except Exception as e:
            log.error(f"Unsubscribe failed: {e}")

    # ── Order Management ──

    def place_order(
        self,
        exchange_segment: str = "bse_fo",
        trading_symbol: str = "",
        transaction_type: str = "B",
        order_type: str = "L",
        quantity: int = 15,
        price: float = 0,
        product: str = "NRML",
        validity: str = "DAY",
        trigger_price: float = 0,
        tag: str = None,
    ) -> dict:
        """Place an order on Kotak Neo."""
        if not self.client or not self.is_authenticated:
            return {"status": "error", "message": "Not authenticated"}

        try:
            result = self.client.place_order(
                exchange_segment=exchange_segment,
                product=product,
                price=str(price),
                order_type=order_type,
                quantity=str(quantity),
                validity=validity,
                trading_symbol=trading_symbol,
                transaction_type=transaction_type,
                amo="NO",
                disclosed_quantity="0",
                market_protection="0",
                pf="N",
                trigger_price=str(trigger_price),
                tag=tag,
            )
            log.info(f"Order placed: {result}")
            return {"status": "ok", "data": result}
        except Exception as e:
            log.error(f"Order placement failed: {e}")
            return {"status": "error", "message": str(e)}

    def cancel_order(self, order_id: str) -> dict:
        """Cancel an existing order."""
        if not self.client:
            return {"status": "error", "message": "Not authenticated"}
        try:
            result = self.client.cancel_order(order_id=order_id)
            return {"status": "ok", "data": result}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    # ── Data Retrieval ──

    def search_scrip(self, symbol: str, expiry: str = "", option_type: str = "", strike_price: str = "") -> dict:
        """Search for scrip details to get trading_symbol."""
        if not self.client:
            return {}
        try:
            result = self.client.search_scrip(
                exchange_segment="bse_fo",
                symbol=symbol,
                expiry=expiry,
                option_type=option_type,
                strike_price=strike_price,
            )
            return result or {}
        except Exception as e:
            log.error(f"Scrip search failed: {e}")
            return {}

    def get_positions(self) -> dict:
        if not self.client:
            return {}
        try:
            return self.client.positions() or {}
        except Exception as e:
            log.error(f"Positions fetch failed: {e}")
            return {}

    def get_order_book(self) -> dict:
        if not self.client:
            return {}
        try:
            return self.client.order_report() or {}
        except Exception as e:
            log.error(f"Order book fetch failed: {e}")
            return {}

    def get_trade_book(self) -> dict:
        if not self.client:
            return {}
        try:
            return self.client.trade_report() or {}
        except Exception as e:
            log.error(f"Trade book fetch failed: {e}")
            return {}

    def get_holdings(self) -> dict:
        if not self.client:
            return {}
        try:
            return self.client.holdings() or {}
        except Exception as e:
            log.error(f"Holdings fetch failed: {e}")
            return {}

    def get_status(self) -> dict:
        """Return connection/auth status."""
        if not any(Config.kotak_env().values()):
            login_state = "not_configured"
        elif NeoAPI is None or pyotp is None:
            login_state = "dependency_missing"
        elif self.session_active and self.is_authenticated:
            login_state = "logged_in"
        elif self._last_error:
            login_state = "login_failed"
        else:
            login_state = "unknown"

        return {
            "initialized": self.client is not None,
            "authenticated": self.is_authenticated,
            "session_active": self.session_active,
            "last_login": self._last_login.isoformat() if self._last_login else None,
            "last_error": self._last_error,
            "login_state": login_state,
        }
