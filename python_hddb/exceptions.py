from typing import Optional, Any, Dict


class HdDBClientError(Exception):
    """
    Base exception class for all HdDB client errors.

    Attributes:
        message (str): Detailed error description
        details (dict): Additional error information
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class ConnectionError(HdDBClientError):
    """
    Raised when there's an issue with the database connection.

    Examples:
        >>> raise ConnectionError("Failed to connect to database",
        ...                      {"host": "localhost", "reason": "timeout"})
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        super().__init__(f"Connection Error: {message}", details)


class QueryError(HdDBClientError):
    """
    Raised when there's an error executing a query.

    Examples:
        >>> raise QueryError("Invalid SQL syntax",
        ...                 {"query": "SELECT * FORM table", "line": 1})
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if "query" in details:
            message = f"{message} in query: {details['query']}"
        super().__init__(f"Query Error: {message}", details)


class TableError(HdDBClientError):
    """
    Base class for table-related errors.

    Attributes:
        table_name (str): Name of the table involved in the error
    """

    def __init__(
        self, message: str, table_name: str, details: Optional[Dict[str, Any]] = None
    ):
        details = details or {}
        details["table_name"] = table_name
        super().__init__(f"Table Error: {message}", details)


class TableExistsError(TableError):
    """
    Raised when attempting to create a table that already exists.

    Examples:
        >>> raise TableExistsError("users", {"schema": "public"})
    """

    def __init__(self, table_name: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(f"Table '{table_name}' already exists", table_name, details)


class TransactionError(HdDBClientError):
    """
    Raised when there's an issue with transaction management.

    Examples:
        >>> raise TransactionError("Transaction rollback failed",
        ...                       {"operation": "commit", "state": "pending"})
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        super().__init__(f"Transaction Error: {message}", details)


class ValidationError(HdDBClientError):
    """
    Raised when input validation fails.

    Examples:
        >>> raise ValidationError("Invalid column name",
        ...                      {"column": "user-id", "reason": "contains hyphen"})
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        super().__init__(f"Validation Error: {message}", details)


class DataTypeError(HdDBClientError):
    """
    Raised when there's a data type mismatch.

    Examples:
        >>> raise DataTypeError("Cannot convert value",
        ...                    {"column": "age", "value": "abc", "expected": "integer"})
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        super().__init__(f"Data Type Error: {message}", details)
