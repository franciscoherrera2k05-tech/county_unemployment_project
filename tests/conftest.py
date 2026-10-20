"""Shared pytest setup.

What it does: skips tests marked `integration` (they query the live MotherDuck warehouse) when no
MOTHERDUCK_TOKEN is available, and gives integration tests one shared connection.
In: environment variables. Out: the `con` fixture (a MotherDuck connection on the configured database).
"""
import os

import pytest

from src.utils.helpers import connect


def pytest_collection_modifyitems(config, items):
    if os.environ.get("MOTHERDUCK_TOKEN"):
        return
    skip = pytest.mark.skip(reason="MOTHERDUCK_TOKEN not set")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def con():
    connection = connect()
    yield connection
    connection.close()
