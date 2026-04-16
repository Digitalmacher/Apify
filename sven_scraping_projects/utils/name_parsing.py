from __future__ import annotations

import re
from typing import TypedDict


class ParsedPersonName(TypedDict):
    title: str
    first_name: str
    last_name: str
    name: str


_WHITESPACE_RE = re.compile(r"\s+")

# Canonical academic titles we expect in Hamburg healthcare directories.
# Order matters: longest/most-specific first.
ACADEMIC_TITLES: tuple[str, ...] = (
    "Prof. Dr. med. dent.",
    "Prof. Dr. med.",
    "Prof. Dr.",
    "PD Dr. med. dent.",
    "PD Dr. med.",
    "PD Dr.",
    "Dr. med. dent.",
    "Dr. med.",
    "Dr.",
    "Dipl.-Psych.",
    "Med. pract.",
)


def _normalize_name(raw_name: str | None) -> str:
    if not raw_name:
        return ""
    return _WHITESPACE_RE.sub(" ", str(raw_name).strip())


def parse_person_name(raw_name: str | None) -> ParsedPersonName:
    """Split a human name into academic title, first_name, last_name, and a rebuilt full name.

    Rules:
    - Strips common German prefixes like 'Frau'/'Herr'.
    - Extracts academic titles from the beginning of the name (prefix match).
    - Splits remaining tokens into first token = first_name, remaining = last_name.
    """
    name = _normalize_name(raw_name)
    if not name:
        return {"title": "", "first_name": "", "last_name": "", "name": ""}

    # Strip honorific prefixes that are not academic titles.
    for prefix in ("Frau ", "Herr "):
        if name.startswith(prefix):
            name = name[len(prefix) :].strip()
            break

    title = ""
    for t in ACADEMIC_TITLES:
        if name.startswith(t):
            title = t
            name = name[len(t) :].strip()
            break

    parts = [p for p in name.split(" ") if p]
    if len(parts) <= 1:
        first_name = parts[0] if parts else ""
        last_name = ""
    else:
        first_name = parts[0]
        last_name = " ".join(parts[1:])

    full_name = " ".join(p for p in (title, first_name, last_name) if p)
    return {
        "title": title,
        "first_name": first_name,
        "last_name": last_name,
        "name": full_name,
    }

