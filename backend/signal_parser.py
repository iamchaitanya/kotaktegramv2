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
    targets: Optional[list[float]] = None
    diff: Optional[float] = None
    stoploss: Optional[float] = None

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

    # ── Rule 1: Mandatory "Trading Floor" or "Risky" header (flexible spacing) ──
    # Allow leading non-alphanumeric characters (like ** for bold in Telegram)
    if not re.search(r"(trading\s*floor|risky)", lower):
        return ParsedSignal(status="ignored", reason='Does not start with "trading floor" or "risky"')

    # ── Rule 2: Must contain "sensex" ──
    if "sensex" not in lower:
        return ParsedSignal(status="ignored", reason='Does not contain "sensex"')

    # ── Rule 3: Strike price — exactly 5 digits + CE/PE ──
    # Improved regex to find 5 digits followed by CE or PE more reliably
    option_match = re.search(r"(\d{5})\s*(CE|PE)", stripped, re.IGNORECASE)
    if not option_match:
        return ParsedSignal(status="ignored", reason="Strike (5 digits) + CE/PE not found")

    strike = option_match.group(1)
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

    # ── Rule 5.5: Stoploss extraction ──
    # Improved to handle "STOPLOSS **415**" or "SL: @200"
    # Matches label, then skips any non-digits, then captures the price.
    sl_match = re.search(r"(?:stoploss|sl|stls)[^\d]*(\d{1,5})", stripped, re.IGNORECASE)
    stoploss = int(sl_match.group(1)) if sl_match else None

    # ── Rule 5.6: Target extraction ──
    targets = []
    # Match T1, T2, T3, Target 1, etc.
    # Updated to handle formats like "T1 280 T2 320" and "Targets: 280, 320"
    target_pattern = re.compile(r"(?:target|tgt|t)(?:\s*\d+)?\s*[:@\-]?\s*(\d{1,5})", re.IGNORECASE)
    for tm in target_pattern.finditer(stripped):
        targets.append(float(tm.group(1)))
    
    if not targets:
        # Also look for a comma/space separated list after "targets:"
        list_match = re.search(r"targets?\s*[:@\-]?\s*([\d\s,]+)", lower)
        if list_match:
            nums = re.findall(r"\d+", list_match.group(1))
            targets = [float(n) for n in nums if len(n) <= 5]

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
        targets=targets if targets else None,
        diff=abs(high - low),
        stoploss=stoploss,
    )


# ── Convenience: bulk parse ──
def parse_bulk(messages: list[str]) -> list[dict]:
    """Parse a list of messages and return results."""
    results = []
    for msg in messages:
        signal = parse_signal(msg)
        results.append({"original": msg, "result": signal.to_dict()})
    return results
