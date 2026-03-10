"""
Signal Parser — Sensex Options Trade Signal Parser (Regex-based)

Ported from the user's Telegram Trade Suite JS parser.
Rules:
  1. Must start with "trading floor" (case-insensitive, flexible spacing)
  2. Must contain "sensex"
  3. Strike must be exactly 5 digits followed by CE or PE
  4. Entry price found after the "price" keyword
  5. "average" / "avg" overrides range low
  6. Range high >= range low, difference <= 50
"""
import re
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class ParsedSignal:
    status: str  # 'valid', 'ignored', 'empty'
    reason: Optional[str] = None
    index: Optional[str] = None
    strike: Optional[str] = None
    option_type: Optional[str] = None  # CE / PE
    entry_low: Optional[float] = None
    entry_high: Optional[float] = None
    diff: Optional[float] = None

    def to_dict(self):
        return asdict(self)


def parse_signal(text: str) -> ParsedSignal:
    """
    Parse a Telegram signal message into a structured trade signal.
    Uses the exact same regex rules as the user's JS parser.
    """
    if not text or not text.strip():
        return ParsedSignal(status="empty")

    stripped = text.strip()
    lower = stripped.lower()

    # ── Rule 1: Mandatory "Trading Floor" header (flexible spacing) ──
    # Allow leading non-alphanumeric characters (like ** for bold in Telegram)
    if not re.match(r"^[\W_]*trading\s*floor", lower):
        return ParsedSignal(status="ignored", reason='Does not start with "trading floor"')

    # ── Rule 2: Must contain "sensex" ──
    if "sensex" not in lower:
        return ParsedSignal(status="ignored", reason='Does not contain "sensex"')

    # ── Rule 3: Strike price — exactly 5 digits + CE/PE ──
    option_match = re.search(r"(\d{1,10})\s*(CE|PE)", stripped, re.IGNORECASE)
    if not option_match:
        return ParsedSignal(status="ignored", reason="Strike/Type not found")

    strike = option_match.group(1)
    if len(strike) != 5:
        return ParsedSignal(
            status="ignored",
            reason=f"Strike length is {len(strike)} (Need 5)",
        )
    option_type = option_match.group(2).upper()

    # ── Rule 4: Entry price range — after "price" keyword ──
    remaining = stripped[option_match.end():]
    price_kw = re.search(r"price", remaining, re.IGNORECASE)
    if not price_kw:
        return ParsedSignal(status="ignored", reason='Keyword "price" not found after strike')

    after_price = remaining[price_kw.end():]
    price_match = re.search(r"@?\s*(\d{1,5})(?:\s*[-@]\s*(\d{1,5}))?", after_price)
    if not price_match:
        return ParsedSignal(status="ignored", reason='Price range not found after "price" keyword')

    low = int(price_match.group(1))
    high = int(price_match.group(2)) if price_match.group(2) else low

    # ── Rule 5: Average override ──
    avg_match = re.search(r"(?:average|avg)\s*@?\s*(\d{1,5})", stripped, re.IGNORECASE)
    if avg_match:
        low = int(avg_match.group(1))

    # ── Rule 6: Validation ──
    if high < low:
        return ParsedSignal(status="ignored", reason="High < Low")
    if abs(high - low) > 50:
        return ParsedSignal(status="ignored", reason="Range difference > 50")

    return ParsedSignal(
        status="valid",
        index="SENSEX",
        strike=strike,
        option_type=option_type,
        entry_low=low,
        entry_high=high,
        diff=abs(high - low),
    )


# ── Convenience: bulk parse ──
def parse_bulk(messages: list[str]) -> list[dict]:
    """Parse a list of messages and return results."""
    results = []
    for msg in messages:
        signal = parse_signal(msg)
        results.append({"original": msg, "result": signal.to_dict()})
    return results
