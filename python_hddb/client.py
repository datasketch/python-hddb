import os
from functools import wraps
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
from loguru import logger

from .exceptions import ConnectionError, QueryError
from .helpers import generate_field_metadata


def attach_motherduck(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not os.environ.get("motherduck_token"):
            raise ValueError("Motherduck token has not been set")
        self.execute("ATTACH 'md:'")
        return func(self, *args, **kwargs)

    return wrapper


class HdDB:
    def __init__(self, motherduck_token="", read_only=False):
        try:
            if motherduck_token:
                os.environ["motherduck_token"] = motherduck_token
            self.conn = duckdb.connect(":memory:", read_only=read_only)
        except duckdb.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

    def set_motherduck_token(self, motherduck_token: str):
        os.environ["motherduck_token"] = motherduck_token

    def execute(
        self, query: str, parameters: Optional[List[Any]] = None
    ) -> duckdb.DuckDBPyConnection:
        return self.conn.execute(query, parameters)

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
            all_metadata = []
            for df, table_name in zip(dataframes, names):
                metadata = generate_field_metadata(df)

                # Create a mapping of original column names to new IDs
                columns = {field["label"]: field["id"] for field in metadata}

                # Rename the columns in the DataFrame
                df_renamed = df.rename(columns=columns)

                # Create the table with renamed columns
                query = f"CREATE TABLE {table_name} AS SELECT * FROM df_renamed"
                self.execute(query)

                for field in metadata:
                    field["table"] = table_name
                all_metadata.extend(metadata)

            self.create_hd_tables()
            self.create_hd_fields(all_metadata)
        except duckdb.Error as e:
            raise QueryError(f"Error executing query: {e}")

    # TODO: map duckdb data types to datasketch types
    def create_hd_fields(self, metadata: List[Dict[str, str]]):
        try:
            # Create a temporary table with the metadata
            self.execute(
                "CREATE TEMP TABLE temp_metadata (fld__id VARCHAR, id VARCHAR, label VARCHAR, tbl VARCHAR)"
            )
            for field in metadata:
                self.execute(
                    "INSERT INTO temp_metadata VALUES (?, ?, ?, ?)",
                    (field["fld__id"], field["id"], field["label"], field["table"]),
                )

            # Join the temporary table with information_schema.columns
            self.execute("""
                CREATE TABLE hd_fields AS 
                SELECT 
                    tm.fld__id, 
                    tm.id, 
                    tm.label, 
                    ic.table_name AS tbl, 
                    ic.data_type AS type
                FROM 
                    temp_metadata tm
                JOIN 
                    information_schema.columns ic 
                ON 
                    tm.tbl = ic.table_name AND tm.id = ic.column_name
            """)

            # Drop the temporary table
            self.execute("DROP TABLE temp_metadata")
        except duckdb.Error as e:
            logger.error(f"Error creating hd_fields: {e}")
            raise QueryError(f"Error creating hd_fields: {e}")

    def create_hd_tables(self):
        try:
            self.execute(
                "CREATE TABLE hd_tables AS SELECT table_name AS id, table_name AS label, estimated_size AS nrow, column_count AS ncol from duckdb_tables();"
            )
        except duckdb.Error as e:
            logger.error(f"Error creating hd_tables: {e}")
            raise QueryError(f"Error creating hd_tables: {e}")

    @attach_motherduck
    def upload_to_motherduck(self, org: str, db: str):
        """
        Upload the current database to Motherduck
        """
        try:
            self.execute(
                f'CREATE OR REPLACE DATABASE "{org}__{db}" from CURRENT_DATABASE();',
            )
        except duckdb.Error as e:
            logger.error(f"Error uploading database to MotherDuck: {e}")
            raise ConnectionError(f"Error uploading database to MotherDuck: {e}")

    @attach_motherduck
    def drop_database(self, org: str, db: str):
        """
        Delete a database stored in Motherduck

        :param org: Organization name
        :param db: Database name
        :raises ConnectionError: If there's an error deleting the database from Motherduck
        """
        try:
            self.execute(f'DROP DATABASE "{org}__{db}";')
            logger.info(f"Database {org}__{db} successfully deleted from Motherduck")
        except duckdb.Error as e:
            logger.error(f"Error deleting database from MotherDuck: {e}")
            raise ConnectionError(f"Error deleting database from MotherDuck: {e}")

    @attach_motherduck
    def get_data(self, org: str, db: str, tbl: str) -> dict:
        """
        Retrieve data and field information from a specified table in Motherduck

        :param org: Organization name
        :param db: Database name
        :param tbl: Table name
        :return: Dictionary containing 'data' and 'fields' properties as JSON objects
        :raises ConnectionError: If there's an error retrieving data from Motherduck
        """
        try:
            # Fetch data from the specified table
            data_query = f'SELECT * FROM "{org}__{db}"."{tbl}"'
            data = self.execute(data_query).fetchdf()

            # Fetch field information
            fields_query = f'SELECT * FROM "{org}__{db}".hd_fields WHERE tbl = ?'
            fields = self.execute(fields_query, [tbl]).fetchdf()

            # Convert DataFrames to JSON objects
            data_json = data.to_json(orient='records')
            fields_json = fields.to_json(orient='records')

            return {"data": data_json, "fields": fields_json}
        except duckdb.Error as e:
            logger.error(f"Error retrieving data from MotherDuck: {e}")
            raise ConnectionError(f"Error retrieving data from MotherDuck: {e}")

    def close(self):
        try:
            self.conn.close()
            logger.info("Database connection closed")
        except duckdb.Error as e:
            logger.error(f"Error closing connection: {e}")
