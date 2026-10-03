"""Public ticker validation shared by the HTTP API and the research tools."""

from __future__ import annotations

import re

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9$._/-]{1,32}$")
