import os

import pytest


def pytest_collection_modifyitems(config, items):
    if os.environ.get("MOTHERDUCK_TOKEN"):
        return
    skip = pytest.mark.skip(reason="MOTHERDUCK_TOKEN not set")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
