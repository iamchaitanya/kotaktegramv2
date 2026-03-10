"""
Contract Master — Downloads and parses the daily BSE contract master from Kotak Neo.
Filters to only SENSEX options for the nearest expiry (~450 contracts).
Used to resolve human-readable signals (e.g. SENSEX 80000 PE) to real instrument tokens.
"""
import csv
import io
import os
import logging
import httpx
from datetime import datetime, date
from typing import Optional

log = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
MASTER_FILE = os.path.join(DATA_DIR, "sensex_contracts.csv")


class ContractMaster:
    """Manages the daily contract master for SENSEX options."""

    def __init__(self):
        self._contracts: list[dict] = []
        self._last_download: Optional[date] = None

    @property
    def is_loaded(self) -> bool:
        return len(self._contracts) > 0

    def download(self, kotak_client) -> bool:
        """Download the full scrip master from Kotak Neo and filter to SENSEX nearest-expiry options."""
        if not kotak_client or not kotak_client.client:
            log.warning("Cannot download contract master — Kotak client not available")
            return False

        try:
            log.info("Downloading scrip master from Kotak Neo...")
            result = kotak_client.client.scrip_master(exchange_segment="bse_fo")

            if not result:
                log.error("scrip_master() returned empty result")
                return False

            # Parse the result — it may be a URL, CSV string, list of dicts, etc.
            contracts = self._parse_scrip_master(result)

            if not contracts:
                log.error("No contracts parsed from scrip master")
                return False

            log.info(f"Total BSE FO contracts downloaded: {len(contracts)}")

            # Filter to SENSEX options only
            sensex_contracts = [
                c for c in contracts
                if self._is_sensex_option(c)
            ]
            log.info(f"SENSEX options found: {len(sensex_contracts)}")

            if not sensex_contracts:
                log.warning("No SENSEX options found in contract master!")
                return False

            # Find the nearest expiry
            nearest_expiry = self._find_nearest_expiry(sensex_contracts)
            if not nearest_expiry:
                log.error("Could not determine nearest expiry")
                return False

            log.info(f"Nearest SENSEX expiry: {nearest_expiry}")

            # Filter to only nearest expiry
            self._contracts = [
                c for c in sensex_contracts
                if str(c.get("pExpiryDate", "") or c.get("expiry_date", "") or c.get("expiry", "")).strip() == nearest_expiry
            ]

            log.info(f"Final SENSEX contracts (nearest expiry): {len(self._contracts)}")
            self._last_download = date.today()

            # Save filtered contracts to CSV for reference
            self._save_to_csv()

            return True

        except Exception as e:
            log.error(f"Contract master download failed: {e}")
            return False

    def _parse_scrip_master(self, result) -> list[dict]:
        """Parse the scrip master result into a list of contract dicts.
        
        Kotak Neo's scrip_master() can return:
        - A URL string pointing to a CSV file (most common)
        - A raw CSV string
        - A list of dicts
        - A dict with a 'data' key
        """
        contracts = []

        try:
            if isinstance(result, list):
                # Already a list of dicts
                contracts = result
            elif isinstance(result, str):
                # Check if it's a URL (Kotak returns a URL to the CSV file)
                if result.strip().startswith("http"):
                    log.info(f"scrip_master returned URL: {result.strip()}")
                    csv_data = self._download_csv_from_url(result.strip())
                    if csv_data:
                        reader = csv.DictReader(io.StringIO(csv_data))
                        contracts = list(reader)
                        log.info(f"Parsed {len(contracts)} contracts from downloaded CSV")
                else:
                    # Raw CSV string — parse it
                    reader = csv.DictReader(io.StringIO(result))
                    contracts = list(reader)
            elif isinstance(result, dict):
                # May contain a 'data' key with the actual list
                data = result.get("data", result)
                if isinstance(data, list):
                    contracts = data
                elif isinstance(data, str):
                    if data.strip().startswith("http"):
                        csv_data = self._download_csv_from_url(data.strip())
                        if csv_data:
                            reader = csv.DictReader(io.StringIO(csv_data))
                            contracts = list(reader)
                    else:
                        reader = csv.DictReader(io.StringIO(data))
                        contracts = list(reader)
        except Exception as e:
            log.error(f"Error parsing scrip master: {e}")

        return contracts

    def _download_csv_from_url(self, url: str) -> Optional[str]:
        """Download CSV content from a URL."""
        try:
            log.info(f"Downloading CSV from {url}...")
            response = httpx.get(url, timeout=30.0)
            response.raise_for_status()
            log.info(f"Downloaded {len(response.text)} bytes of CSV data")
            return response.text
        except Exception as e:
            log.error(f"Failed to download CSV from {url}: {e}")
            return None

    def _is_sensex_option(self, contract: dict) -> bool:
        """Check if a contract is a SENSEX option (CE/PE)."""
        # Kotak Neo scrip master uses various field names
        # When downloading from CSV, field names come from CSV headers
        # When from search_scrip, field names use pXxx convention
        symbol = str(
            contract.get("pSymbolName", "") or
            contract.get("pSymbol", "") or
            contract.get("symbol", "") or
            contract.get("pScripRefKey", "") or
            contract.get("pTrdSymbol", "") or
            ""
        ).upper()

        option_type = str(
            contract.get("pOptionType", "") or
            contract.get("option_type", "") or
            ""
        ).upper()

        # Must be SENSEX and must be an option (CE or PE)
        is_sensex = "SENSEX" in symbol or "BSXOPT" in symbol
        is_option = option_type in ("CE", "PE")

        return is_sensex and is_option

    def _find_nearest_expiry(self, contracts: list[dict]) -> Optional[str]:
        """Find the nearest future expiry date from the contracts.
        
        Handles both:
        - Unix timestamps (from CSV download): "1776364199"
        - Date strings (from API): "16Apr2026", "16-Apr-2026", etc.
        """
        today = date.today()
        expiry_map: dict[str, date] = {}  # raw string -> parsed date

        for c in contracts:
            exp_str = str(c.get("pExpiryDate", "") or c.get("expiry_date", "") or c.get("expiry", "")).strip()
            if not exp_str:
                continue

            if exp_str in expiry_map:
                continue  # Already parsed

            exp_date = None
            try:
                # Try Unix timestamp first (purely numeric or numeric-like)
                ts = float(exp_str)
                if ts > 1_000_000_000:  # Looks like a Unix timestamp
                    exp_date = datetime.fromtimestamp(ts).date()
            except (ValueError, TypeError, OSError):
                pass

            if exp_date is None:
                # Try common date string formats
                for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d%b%Y"):
                    try:
                        exp_date = datetime.strptime(exp_str, fmt).date()
                        break
                    except ValueError:
                        continue

            if exp_date and exp_date >= today:
                expiry_map[exp_str] = exp_date

        if not expiry_map:
            return None

        # Return the nearest future expiry
        return min(expiry_map.keys(), key=lambda s: expiry_map[s])

    def lookup(self, strike: str, option_type: str) -> Optional[dict]:
        """Look up the instrument token for a SENSEX option by strike and type.
        
        Args:
            strike: Strike price as string, e.g. "80000"
            option_type: "CE" or "PE"
            
        Returns:
            dict with instrument_token, trading_symbol, etc. or None
        """
        option_type = option_type.upper()
        
        for c in self._contracts:
            # Kotak has a quirky field name: "dStrikePrice;" (with semicolon)
            c_strike = str(
                c.get("dStrikePrice;", "") or
                c.get("pStrikePrice", "") or
                c.get("strike_price", "") or
                c.get("dStrikePrice", "") or
                ""
            ).strip()
            c_option = str(c.get("pOptionType", "") or c.get("option_type", "") or "").upper()

            # Remove decimals from strike for comparison (e.g. "8000000.0" -> "80000")
            # Kotak stores strike * 100, so "8000000.0" means strike 80000
            c_strike_clean = c_strike.split(".")[0] if "." in c_strike else c_strike
            strike_clean = strike.split(".")[0] if "." in strike else strike

            # Handle Kotak's strike * 100 convention
            try:
                c_strike_val = int(float(c_strike)) if c_strike else 0
                strike_val = int(strike_clean) if strike_clean else 0
                # If the contract strike is 100x the signal strike, adjust
                if c_strike_val == strike_val * 100:
                    c_strike_clean = str(strike_val)
                elif c_strike_val == strike_val:
                    c_strike_clean = str(strike_val)
                else:
                    c_strike_clean = str(c_strike_val)
                strike_clean = str(strike_val)
            except (ValueError, TypeError):
                pass

            if c_strike_clean == strike_clean and c_option == option_type:
                return {
                    "instrument_token": str(c.get("pSymbol", "") or c.get("pInstrumentToken", "") or c.get("instrument_token", "")),
                    "trading_symbol": c.get("pTrdSymbol", "") or c.get("trading_symbol", ""),
                    "exchange_segment": "bse_fo",
                    "strike": strike,
                    "option_type": option_type,
                    "expiry": c.get("pExpiryDate", "") or c.get("expiry_date", ""),
                    "lot_size": c.get("lLotSize", "") or c.get("pLotSize", "") or c.get("lot_size", ""),
                    "tick_size": c.get("dTickSize", "") or c.get("pTickSize", "") or c.get("tick_size", ""),
                    "raw": c,  # Keep the full contract data for debugging
                }

        log.warning(f"No contract found for SENSEX {strike} {option_type}")
        return None

    def get_all(self) -> list[dict]:
        """Return all loaded contracts."""
        return list(self._contracts)

    def _save_to_csv(self):
        """Save filtered contracts to CSV for reference/debugging."""
        os.makedirs(DATA_DIR, exist_ok=True)
        try:
            if not self._contracts:
                return
            keys = self._contracts[0].keys()
            with open(MASTER_FILE, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(self._contracts)
            log.info(f"Saved {len(self._contracts)} contracts to {MASTER_FILE}")
        except Exception as e:
            log.error(f"Failed to save contract master CSV: {e}")

    def _load_from_csv(self) -> bool:
        """Load contracts from previously saved CSV."""
        if not os.path.exists(MASTER_FILE):
            return False
        try:
            with open(MASTER_FILE, "r") as f:
                reader = csv.DictReader(f)
                self._contracts = list(reader)
            if self._contracts:
                log.info(f"Loaded {len(self._contracts)} contracts from cached CSV")
                return True
        except Exception as e:
            log.error(f"Failed to load cached contract master: {e}")
        return False
