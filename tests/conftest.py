import pytest
from python_hddb.client import HdDB

@pytest.fixture
def db_client():
    client = HdDB()
    yield client
    client.close()