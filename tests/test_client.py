import pandas as pd
import pytest

from python_hddb.client import HdDB
from python_hddb.exceptions import ConnectionError, QueryError


def test_client_initialization(db_client):
    assert isinstance(db_client, HdDB)


def test_create_database_with_one_table(db_client):
    df = pd.DataFrame(
        data={"username": ["ddazal", "lcalderon", "pipeleon"], "age": [30, 28, 29]}
    )
    db_client.create_database(dataframes=[df], names=["users"])
    result = db_client.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()
    table = result[0]
    table_name = table[0]
    assert len(result) == 1
    assert table_name == "users"

def test_create_database_with_multiple_tables(db_client):
    users = pd.DataFrame(
        data={"username": ["ddazal", "lcalderon", "pipeleon"], "age": [30, 28, 29]}
    )
    courses = pd.DataFrame(
        data={"name": ["Backend with Python", "Frontend with React", "DevOps"]}
    )
    db_client.create_database(dataframes=[users, courses], names=["users", "courses"])
    result = db_client.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()
    tables = [t[0] for t in result]
    assert len(result) == 2
    assert 'users' in tables
    assert 'courses' in tables

def test_create_database_value_error(db_client):
    users = pd.DataFrame(
        data={"username": ["ddazal", "lcalderon", "pipeleon"], "age": [30, 28, 29]}
    )
    with pytest.raises(ValueError):
        db_client.create_database(dataframes=[users], names=['users', 'courses'])
    