"""Public ticker validation shared by the HTTP API and the research tools."""

from __future__ import annotations

import datetime as dt
import re

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9$._/-]{1,32}$")
OPTION_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.]{1,6} *[0-9]{6}[CP][0-9]{8}$")


def is_option_symbol(symbol: str) -> bool:
    """Recognize the 21-character OCC symbols Schwab returns, preserving padding."""
    if len(symbol) != 21 or not OPTION_SYMBOL_PATTERN.fullmatch(symbol):
        return False
    try:
        dt.date(2000 + int(symbol[6:8]), int(symbol[8:10]), int(symbol[10:12]))
    except ValueError:
        return False
    return True


def is_quote_symbol(symbol: str) -> bool:
    return bool(SYMBOL_PATTERN.fullmatch(symbol)) or is_option_symbol(symbol)
