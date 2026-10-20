import pytest


@pytest.fixture(scope="session")
def md():
    from src.utils.db import connect
    con = connect(create=False)
    yield con
    con.close()
