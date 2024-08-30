import json
import io
import os
from functools import wraps
from typing import Any, Dict, Generator, List, Optional, Union

import duckdb
import pandas as pd
from loguru import logger

from .exceptions import ConnectionError, QueryError, TableExistsError
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
            self.execute(
                """
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
            """
            )

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
            data_json = data.to_dict(orient="records")
            fields_json = fields.to_dict(orient="records")

            return {"data": data_json, "fields": fields_json}
        except duckdb.Error as e:
            logger.error(f"Error retrieving data from MotherDuck: {e}")
            raise ConnectionError(f"Error retrieving data from MotherDuck: {e}")

    @attach_motherduck
    def drop_table(self, org: str, db: str, tbl: str):
        """
        Deletes a specific table from the database in Motherduck

        :param org: Organization name
        :param db: Database name
        :param tbl: Table name
        :raises ConnectionError: Si hay un error al eliminar la tabla de Motherduck
        """
        try:

            self.execute("BEGIN TRANSACTION;")
            try:

                self.execute(f'DROP TABLE IF EXISTS "{org}__{db}"."{tbl}";')

                self.execute(f'DELETE FROM "{org}__{db}".hd_tables WHERE id = ?', [tbl])
                self.execute(
                    f'DELETE FROM "{org}__{db}".hd_fields WHERE tbl = ?', [tbl]
                )

                self.execute("COMMIT;")
            except Exception as e:

                self.execute("ROLLBACK;")
                raise e

            logger.info(
                f"Table {tbl} successfully deleted from database {org}__{db} in Motherduck and its record in hd_data has been removed"
            )
        except duckdb.Error as e:
            logger.error(f"Error deleting table from MotherDuck: {e}")
            raise ConnectionError(f"Error deleting table from MotherDuck: {e}")

    @attach_motherduck
    def add_table(self, org: str, db: str, tbl: str, df: pd.DataFrame):
        """
        Adds a new table to an existing database in MotherDuck and registers it in hd_tables and hd_fields.

        :param org: Organization name
        :param db: Database name
        :param df: Pandas DataFrame containing the data to be added
        :param table_name: Name of the new table
        :raises ConnectionError: If there's an error adding the table to MotherDuck
        """
        try:
            # Generate metadata for the new table
            metadata = generate_field_metadata(df)

            # Create a mapping of original column names to new IDs
            columns = {field["label"]: field["id"] for field in metadata}

            # Rename the columns in the DataFrame
            df_renamed = df.rename(columns=columns)

            # Begin transaction
            self.execute("BEGIN TRANSACTION;")

            # Create the new table
            self.execute(
                f'CREATE TABLE "{org}__{db}"."{tbl}" AS SELECT * FROM df_renamed'
            )

            # Insert into hd_tables
            self.execute(
                f'INSERT INTO "{org}__{db}".hd_tables (id, label, nrow, ncol) VALUES (?, ?, ?, ?)',
                [tbl, tbl, len(df), len(df.columns)],
            )

            self.execute(
                "CREATE TEMP TABLE temp_metadata (fld__id VARCHAR, id VARCHAR, label VARCHAR, tbl VARCHAR)"
            )

            for field in metadata:
                self.execute(
                    "INSERT INTO temp_metadata VALUES (?, ?, ?, ?)",
                    (field["fld__id"], field["id"], field["label"], tbl),
                )

            logger.info(
                f"temp_metadata: {self.execute('SELECT * FROM temp_metadata').fetchdf()}"
            )

            # Insertar en hd_fields usando una consulta JOIN
            self.execute(
                f"""
            INSERT INTO "{org}__{db}".hd_fields (fld__id, id, label, tbl, type)
            SELECT
                tm.fld__id,
                tm.id,
                tm.label,
                '{tbl}' AS tbl,
                ic.data_type AS type
            FROM
                temp_metadata tm
            JOIN
                information_schema.columns ic
            ON
                '{tbl}' = ic.table_name AND tm.id = ic.column_name
            """
            )

            # # Eliminar la tabla temporal
            self.execute("DROP TABLE temp_metadata")

            # Commit transaction
            self.execute("COMMIT;")

            logger.info(
                f"Table {tbl} successfully added to database {org}__{db} in MotherDuck"
            )
        except duckdb.CatalogException as e:
            self.execute("ROLLBACK;")
            logger.error(f"Table with name {tbl} already exists: {e}")
            raise TableExistsError(f"Table with name {tbl} already exists: {e}")
        except duckdb.Error as e:
            self.execute("ROLLBACK;")
            logger.error(f"Error adding table to MotherDuck: {e}")
            raise ConnectionError(f"Error adding table to MotherDuck: {e}")

    @attach_motherduck
    def download_data(
        self, org: str, db: str, tbl: str, format: str = "csv", path: str = None
    ) -> Union[str, io.BytesIO]:
        """
        Download data from a specified table in CSV or JSON format, using original column names.

        Args:
            org (str): The organization name.
            db (str): The database name.
            tbl (str): The table name.
            format (str, optional): The output format ('csv' or 'json'). Defaults to 'csv'.
            path (str, optional): The path where the file will be saved. If None, data will be returned as BytesIO.

        Returns:
            Union[str, io.BytesIO]: The path of the saved file if path is provided, otherwise a BytesIO object.

        Raises:
            ValueError: If an invalid format is specified.
            duckdb.Error: If there's an error executing the query or writing the file.
        """
        if format not in ["csv", "json"]:
            raise ValueError("Format must be either 'csv' or 'json'")

        try:
            # Construct the full table name
            full_table_name = f'"{org}__{db}".{tbl}'

            # Get the original column names from hd_fields
            original_names_query = f"""
            SELECT id, label
            FROM "{org}__{db}".hd_fields
            WHERE tbl = '{tbl}'
            """
            original_names = self.execute(original_names_query).fetchdf()

            # Construct the SELECT statement with original column names
            select_stmt = ", ".join(
                [f'"{row.id}" AS "{row.label}"' for _, row in original_names.iterrows()]
            )

            # Prepare the query
            query = f"SELECT {select_stmt} FROM {full_table_name}"

            if path:
                # If path is provided, save to file
                if format == "csv":
                    self.execute(f"COPY ({query}) TO '{path}' (HEADER, DELIMITER ',')")
                else:  # json
                    self.execute(f"COPY ({query}) TO '{path}' (FORMAT JSON)")
                logger.info(f"Data from table {tbl} successfully downloaded to {path}")
                return path
            else:
                # If no path is provided, return as BytesIO
                result = self.execute(query).fetchdf()
                buffer = io.BytesIO()
                if format == "csv":
                    result.to_csv(buffer, index=False)
                else:  # json
                    result.to_json(buffer, orient="records")
                buffer.seek(0)
                logger.info(f"Data from table {tbl} successfully exported to memory")
                return buffer

        except duckdb.Error as e:
            logger.error(f"Error downloading/exporting data from table {tbl}: {e}")
            raise

    def close(self):
        try:
            self.conn.close()
            logger.info("Database connection closed")
        except duckdb.Error as e:
            logger.error(f"Error closing connection: {e}")
