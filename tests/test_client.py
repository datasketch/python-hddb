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
    create_and_verify_database(db_client, [df], ["users"], 3)


def test_create_database_with_multiple_tables(db_client):
    users = pd.DataFrame(
        data={"username": ["ddazal", "lcalderon", "pipeleon"], "age": [30, 28, 29]}
    )
    courses = pd.DataFrame(
        data={"name": ["Backend with Python", "Frontend with React", "DevOps"]}
    )
    create_and_verify_database(db_client, [users, courses], ["users", "courses"], 4)


def test_create_database_value_error(db_client):
    users = pd.DataFrame(
        data={"username": ["ddazal", "lcalderon", "pipeleon"], "age": [30, 28, 29]}
    )
    with pytest.raises(ValueError):
        db_client.create_database(dataframes=[users], names=["users", "courses"])


def create_and_verify_database(db_client, dataframes, names, expected_table_count):
    db_client.create_database(dataframes=dataframes, names=names)
    result = db_client.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()
    tables = [t[0] for t in result]
    assert len(result) == expected_table_count
    for name in names:
        assert name in tables
    assert "hd_fields" in tables
    assert "hd_tables" in tables
