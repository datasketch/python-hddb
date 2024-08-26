import os
from typing import Any, List, Optional

import duckdb
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from .exceptions import ConnectionError, QueryError

load_dotenv()


class HdDB:
    def __init__(self, read_only=False):
        try:
            self.conn = duckdb.connect(":memory:", read_only=read_only)
        except duckdb.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

    def execute(
        self, query: str, parameters: Optional[List[Any]] = None
    ) -> duckdb.DuckDBPyConnection:
        try:
            return self.conn.execute(query, parameters)
        except duckdb.Error as e:
            logger.error(f"Error executing query: {e}")
            raise QueryError(f"Error executing query: {e}")

    def create_database(self, dataframes: List[pd.DataFrame], names: List[str]):
        """
        Create in-memory database and create tables from a list of dataframes.

        :param dataframes: List of pandas DataFrames to create tables from
        :param names: List of names for the tables to be created
        :raises ValueError: If the number of dataframes doesn't match the number of table names
        :raises QueryError: If there's an error executing a query
        """
        if len(dataframes) != len(names):
            raise ValueError(
                "The number of dataframes must match the number of table names"
            )

        try:
            # self.conn.execute("CREATE TABLE hd_fields ();")
            # self.conn.execute("CREATE TABLE hd_tables (id TEXT, name TEXT, slug TEXT);")
            for df, table_name in zip(dataframes, names):
                query = f"CREATE TABLE {table_name} AS SELECT * FROM df"
                self.execute(query)
        except duckdb.Error as e:
            raise QueryError(f"Error executing query: {e}")

    def upload_to_motherduck(self, org: str, db: str):
        """
        Upload the current database to Motherduck
        """
        os.environ["motherduck_token"] = os.environ["MOTHERDUCK_TOKEN"]
        if not os.environ["motherduck_token"]:
            raise ValueError("Motherduck token has not been set")

        try:
            # https://motherduck.com/docs/key-tasks/loading-data-into-motherduck/loading-duckdb-database/
            self.execute("ATTACH 'md:'")
            self.execute(
                "CREATE OR REPLACE DATABASE ? from CURRENT_DATABASE();",
                [org + "__" + db],
            )
        except duckdb.Error as e:
            logger.error(f"Error uploading database to MotherDuck: {e}")
            raise ConnectionError(f"Error uploading database to MotherDuck: {e}")

    def close(self):
        try:
            self.conn.close()
            logger.info("Database connection closed")
        except duckdb.Error as e:
            logger.error(f"Error closing connection: {e}")
