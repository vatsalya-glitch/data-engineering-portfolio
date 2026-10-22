"""
EVENT CONTRACT VALIDATION

Turns an untrusted dict into a trusted, normalised event - or raises
ContractViolation with a human-readable reason.

Rules come from config/event_contracts.yaml, not from code, so adding an event
type or a field is a config change that every consumer can read.
"""

from datetime import datetime, timedelta

from pipeline.config import load_event_contracts
from pipeline.events import parse_ts, utc_now


class ContractViolation(Exception):
    """The event does not satisfy its contract."""


def _check_type(name: str, value, expected: str) -> None:
    ok = {
        "string": lambda v: isinstance(v, str) and v != "",
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "list": lambda v: isinstance(v, list),
        "timestamp": _is_timestamp,
    }[expected](value)
    if not ok:
        raise ContractViolation(f"field '{name}' must be {expected}, got {value!r}")


def _is_timestamp(value) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parse_ts(value)
        return True
    except ValueError:
        return False


def _check_fields(obj: dict, required: dict, optional: dict, where: str) -> None:
    for name, expected in required.items():
        if obj.get(name) is None:
            raise ContractViolation(f"missing required {where} field '{name}'")
        _check_type(name, obj[name], expected)
    for name, expected in optional.items():
        if obj.get(name) is not None:
            _check_type(name, obj[name], expected)


def validate_event(event, contracts: dict | None = None, now: datetime | None = None) -> dict:
    """Validate and normalise one event. Returns a NEW dict; never mutates input."""
    contracts = contracts or load_event_contracts()
    now = now or utc_now()

    if not isinstance(event, dict):
        raise ContractViolation("event must be a JSON object")

    envelope = contracts["envelope"]
    _check_fields(event, envelope["required"], envelope.get("optional", {}), "envelope")

    version = event["schema_version"]
    if version not in contracts["supported_schema_versions"]:
        raise ContractViolation(f"unsupported schema_version {version}")

    event_type = event["event_type"]
    spec = contracts["event_types"].get(event_type)
    if spec is None:
        raise ContractViolation(f"unknown event_type '{event_type}'")

    event_time = parse_ts(event["event_time"])
    if event_time > now + timedelta(seconds=contracts["max_future_skew_seconds"]):
        raise ContractViolation(f"event_time {event['event_time']} is in the future")

    payload = event.get("payload")
    if not isinstance(payload, dict):
        raise ContractViolation("missing payload object")
    _check_fields(payload, spec["payload"], {}, "payload")

    rules = spec.get("rules", {})
    for name in rules.get("non_negative", []):
        if payload[name] < 0:
            raise ContractViolation(f"payload field '{name}' must be >= 0, got {payload[name]}")
    for name in rules.get("non_empty", []):
        if not payload[name]:
            raise ContractViolation(f"payload field '{name}' must not be empty")

    return upcast(event, contracts)


def upcast(event: dict, contracts: dict) -> dict:
    """Schema evolution: bring an old event up to the latest version.

    Consumers downstream of the validator only ever see the latest shape,
    so they don't each need to know the history of the contract.
    """
    normalised = dict(event)
    original = event["schema_version"]
    for version in range(original, contracts["latest_schema_version"]):
        for field, default in contracts["schema_evolution"].get(version, {}).get("defaults", {}).items():
            normalised.setdefault(field, default)
    normalised["schema_version"] = contracts["latest_schema_version"]
    normalised["source_schema_version"] = original
    return normalised
