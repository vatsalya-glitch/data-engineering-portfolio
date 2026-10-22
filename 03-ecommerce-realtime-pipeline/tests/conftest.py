import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.broker.local import LocalBroker  # noqa: E402


class FakeClock:
    """Lets tests fast-forward time instead of sleeping through ack deadlines."""

    def __init__(self, start: float = 1_700_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def broker(tmp_path, clock):
    b = LocalBroker(tmp_path / "broker.sqlite3", clock=clock)
    yield b
    b.close()
