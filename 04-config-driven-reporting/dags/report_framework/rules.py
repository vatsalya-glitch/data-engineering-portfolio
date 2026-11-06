"""Small value checks shared by config.py and steps.py. No Airflow imports."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def is_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def is_identifier(value: Any) -> bool:
    return isinstance(value, str) and value.isidentifier()


def is_timezone(value: Any) -> bool:
    if not is_text(value):
        return False
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        return False
    return True


def is_positive_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and value > 0


def is_int(minimum: int) -> Callable[[Any], bool]:
    return lambda value: not isinstance(value, bool) and isinstance(value, int) and value >= minimum


def is_date(value: Any) -> bool:
    return isinstance(value, date)


def is_email_list(value: Any) -> bool:
    return isinstance(value, list) and bool(value) and all(is_text(v) and "@" in v for v in value)


@dataclass(frozen=True)
class Rule:
    """What a setting's value must be."""

    check: Callable[[Any], bool]
    expected: str          # finishes the sentence "<setting> must be ..."
    required: bool = False

    def problem(self, where: str, value: Any) -> str | None:
        if self.check(value):
            return None
        return f"{where} must be {self.expected}, got {value!r}"


DATASET = Rule(is_identifier, "a dataset name (letters, digits, _)")
TEXT = Rule(is_text, "a non-empty string")
POSITIVE = Rule(is_positive_number, "a positive number")
