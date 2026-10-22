"""Config loading. All paths are resolved relative to the project root."""

from functools import lru_cache
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


@lru_cache
def load_pipeline_config() -> dict:
    with open(CONFIG_DIR / "pipeline.yaml") as f:
        return yaml.safe_load(f)


@lru_cache
def load_event_contracts() -> dict:
    with open(CONFIG_DIR / "event_contracts.yaml") as f:
        return yaml.safe_load(f)
