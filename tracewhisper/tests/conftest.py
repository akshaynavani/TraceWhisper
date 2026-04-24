import json
from pathlib import Path

import pytest

from tracewhisper.models import Trace

FIXTURES = Path(__file__).parent / "fixtures"


def load_raw(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def load_traces(name: str) -> list[Trace]:
    return [Trace.from_jaeger(t) for t in load_raw(name)["data"]]


@pytest.fixture
def payment_failure() -> Trace:
    return load_traces("checkout_payment_failure.json")[0]


@pytest.fixture
def orders_dlq() -> Trace:
    return load_traces("checkout_orders_dlq.json")[0]


@pytest.fixture
def healthy() -> list[Trace]:
    return load_traces("healthy_checkouts.json")
