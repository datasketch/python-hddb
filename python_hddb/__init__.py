# src/python_hddb/__init__.py
from .client import HdDB
from .exceptions import (
    HdDBClientError,
    ConnectionError,
    QueryError,
    TableError,
    TableExistsError,
    TransactionError,
    ValidationError,
    DataTypeError,
)

__all__ = [
    "HdDB",
    "HdDBClientError",
    "ConnectionError",
    "QueryError",
    "TableError",
    "TableExistsError",
    "TransactionError",
    "ValidationError",
    "DataTypeError",
]
