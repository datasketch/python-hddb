class HdDBClientError(Exception):
    """Base exception for HdDBClient errors."""

    pass


class ConnectionError(HdDBClientError):
    """Raised when there's an issue with the database connection."""

    pass


class QueryError(HdDBClientError):
    """Raised when there's an error executing a query."""

    pass


class TableError(HdDBClientError):
    """Raised when there's an issue with table operations."""

    pass


class TransactionError(HdDBClientError):
    """Raised when there's an issue with transaction management."""

    pass


class TableExistsError(TableError):
    """Raised when attempting to create a table that already exists."""

    pass
