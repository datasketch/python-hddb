import io
import json
import os
from functools import wraps
from typing import Any, Dict, List, Optional

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

                # Convert all columns to string type
                df_renamed = df_renamed.astype(str)

                # Create the table with renamed columns and VARCHAR type
                column_definitions = ", ".join(
                    [f'"{col}" VARCHAR' for col in df_renamed.columns]
                )
                create_table_query = f"CREATE TABLE {table_name} ({column_definitions})"
                self.execute(create_table_query)

                # Insert data into the table
                for _, row in df_renamed.iterrows():
                    insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in row])})"
                    self.execute(insert_query, list(row))

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
                "CREATE TEMP TABLE temp_metadata (fld___id VARCHAR, id VARCHAR, label VARCHAR, tbl VARCHAR)"
            )
            for field in metadata:
                self.execute(
                    "INSERT INTO temp_metadata VALUES (?, ?, ?, ?)",
                    (field["fld___id"], field["id"], field["label"], field["table"]),
                )

            # Join the temporary table with information_schema.columns
            self.execute(
                """
                CREATE TABLE hd_fields AS 
                SELECT 
                    tm.fld___id, 
                    tm.id, 
                    tm.label, 
                    ic.table_name AS tbl, 
                    'Txt' AS type
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
            raise QueryError(f"Error deleting database from MotherDuck: {e}")

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
            data_json = json.loads(
                data.fillna("")
                .replace({"NaN": "", "nan": ""})
                .to_json(orient="records")
            )
            fields_json = fields.to_dict(orient="records")

            return {"data": data_json, "fields": fields_json}
        except duckdb.Error as e:
            logger.error(f"Error retrieving data from MotherDuck: {e}")
            raise QueryError(f"Error retrieving data from MotherDuck: {e}")

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
            raise QueryError(f"Error deleting table from MotherDuck: {e}")

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
                "CREATE TEMP TABLE temp_metadata (fld___id VARCHAR, id VARCHAR, label VARCHAR, tbl VARCHAR)"
            )

            for field in metadata:
                self.execute(
                    "INSERT INTO temp_metadata VALUES (?, ?, ?, ?)",
                    (field["fld___id"], field["id"], field["label"], tbl),
                )

            logger.info(
                f"temp_metadata: {self.execute('SELECT * FROM temp_metadata').fetchdf()}"
            )

            # Insertar en hd_fields usando una consulta JOIN, pero siempre usando 'Txt' como tipo
            self.execute(
                f"""
            INSERT INTO "{org}__{db}".hd_fields (fld___id, id, label, tbl, type)
            SELECT
                tm.fld___id,
                tm.id,
                tm.label,
                '{tbl}' AS tbl,
                'Txt' AS type
            FROM
                temp_metadata tm
            """
            )

            # Eliminar la tabla temporal
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
            raise QueryError(f"Error adding table to MotherDuck: {e}")

    @attach_motherduck
    def download_data(
        self, org: str, db: str, tbl: str, format: str = "csv"
    ) -> io.BytesIO:
        """
        Download data from a specified table in CSV or JSON format, using original column names.

        Args:
            org (str): The organization name.
            db (str): The database name.
            tbl (str): The table name.
            format (str, optional): The output format ('csv' or 'json'). Defaults to 'csv'.

        Returns:
            io.BytesIO: A BytesIO object containing the exported data.

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

            # Construct the SELECT statement with original column names, excluding rcd___id from the header
            select_stmt = ", ".join(
                [
                    f'"{row.id}" AS "{row.label}"'
                    for _, row in original_names.iterrows()
                    if row.id != "rcd___id"
                ]
            )

            # Prepare the query
            query = f"SELECT {select_stmt} FROM {full_table_name}"

            # Execute query and return as BytesIO
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
            logger.error(f"Error downloading / exporting data from table {tbl}: {e}")
            raise

    @attach_motherduck
    def update_table_data(
        self, org: str, db: str, tbl: str, field: str, value: str, rcd___id: str
    ) -> bool:
        """
        Update a specific field in a table for a given record.

        :param org: Organization name
        :param db: Database name
        :param table: Table name
        :param field: Field to update
        :param value: New value for the field
        :param rcd___id: Record ID to update
        :return: True if update was successful
        :raises ConnectionError: If there's an error updating data in MotherDuck
        """
        try:
            query = f'UPDATE "{org}__{db}"."{tbl}" SET "{field}" = ? WHERE rcd___id = ?'
            self.execute(query, [value, rcd___id])
            return True
        except duckdb.Error as e:
            logger.error(f"Error updating data in MotherDuck: {e}")
            raise QueryError(f"Error updating data in MotherDuck: {e}")

    def close(self):
        try:
            self.conn.close()
            logger.info("Database connection closed")
        except duckdb.Error as e:
            logger.error(f"Error closing connection: {e}")

    @attach_motherduck
    def delete_table_data(self, org: str, db: str, tbl: str, rcd___id: str) -> bool:
        """
        Delete a specific row from a table.

        :param org: Organization name
        :param db: Database name
        :param tbl: Table name
        :param rcd___id: Record ID to delete
        :return: True if deletion was successful
        :raises ConnectionError: If there's an error deleting data in MotherDuck
        """
        try:
            self.execute("BEGIN TRANSACTION;")
            query = f'DELETE FROM "{org}__{db}"."{tbl}" WHERE rcd___id = ?'
            self.execute(query, [rcd___id])
            query = f'UPDATE "{org}__{db}".hd_tables SET nrow = nrow - 1 WHERE id = ?'
            self.execute(query, [tbl])
            self.execute("COMMIT;")
            logger.info(
                f"Row with rcd___id {rcd___id} successfully deleted from table {tbl}"
            )
            return True
        except duckdb.Error as e:
            self.execute("ROLLBACK;")
            logger.error(f"Error deleting data in MotherDuck: {e}")
            raise QueryError(f"Error deleting data in MotherDuck: {e}")

    @attach_motherduck
    def add_row(self, org: str, db: str, tbl: str, row: dict):
        try:
            columns = ", ".join(f'"{k}"' for k in row.keys())
            placeholders = ", ".join(["?" for _ in row])

            self.execute("BEGIN TRANSACTION;")
            query = (
                f'INSERT INTO "{org}__{db}"."{tbl}" ({columns}) VALUES ({placeholders})'
            )
            self.execute(query, list(row.values()))
            self.execute(
                f'UPDATE "{org}__{db}".hd_tables SET nrow = nrow + 1 WHERE id = ?',
                [tbl],
            )
            self.execute("COMMIT;")
            return True
        except duckdb.Error as e:
            self.execute("ROLLBACK;")
            logger.error(f"Error adding row to table {tbl}: {e}")
            raise QueryError(f"Error adding row to table {tbl}: {e}")

    @attach_motherduck
    def add_column(self, org: str, db: str, tbl: str, column: dict):
        try:
            self.execute("BEGIN TRANSACTION;")
            self.execute(
                f'ALTER TABLE "{org}__{db}"."{tbl}" ADD COLUMN "{column["slug"]}" VARCHAR'
            )
            self.execute(
                f'UPDATE "{org}__{db}".hd_tables SET ncol = ncol + 1 WHERE id = ?',
                [tbl],
            )
            self.execute(
                f'INSERT INTO "{org}__{db}".hd_fields (fld___id, id, label, tbl, type) VALUES (?, ?, ?, ?, ?)',
                [
                    column["fld___id"],
                    column["slug"],
                    column["headerName"],
                    tbl,
                    column["type"],
                ],
            )
            self.execute("COMMIT;")
            return True
        except duckdb.Error as e:
            self.execute("ROLLBACK;")
            logger.error(f"Error adding column to table {tbl}: {e}")
            raise QueryError(f"Error adding column to table {tbl}: {e}")

    @attach_motherduck
    def delete_column(self, org: str, db: str, tbl: str, column: dict):
        try:
            column_name = column["slug"]
            column_id = column["fld___id"]
            self.execute("BEGIN TRANSACTION;")
            self.execute(
                f'ALTER TABLE "{org}__{db}"."{tbl}" DROP COLUMN "{column_name}"'
            )
            self.execute(
                f'UPDATE "{org}__{db}".hd_tables SET ncol = ncol - 1 WHERE id = ?',
                [tbl],
            )
            self.execute(
                f'DELETE FROM "{org}__{db}".hd_fields WHERE fld___id = ? AND tbl = ?',
                [column_id, tbl],
            )
            self.execute("COMMIT;")
            return True
        except duckdb.Error as e:
            self.execute("ROLLBACK;")
            logger.error(f"Error deleting column from table {tbl}: {e}")
            raise QueryError(f"Error deleting column from table {tbl}: {e}")

    @attach_motherduck
    def upload_bulk_data(
        self, org: str, db: str, tbl: str, data: pd.DataFrame, is_json: bool = False
    ):
        try:
            # Fetch the column names and their order from the actual table
            table_columns = self.execute(f'DESCRIBE "{org}__{db}"."{tbl}"').fetchall()
            table_column_names = [col[0] for col in table_columns]

            if not is_json:
                # Fetch the column labels and ids from hd_fields
                hd_fields = self.execute(
                    f'SELECT id, label FROM "{org}__{db}".hd_fields WHERE tbl = ?',
                    [tbl],
                ).fetchall()
                hd_fields_dict = {field[1]: field[0] for field in hd_fields}
                hd_fields_reverse_dict = {field[0]: field[1] for field in hd_fields}

                # Create a mapping from data column names to table column names
                column_mapping = {}
                for data_col in data.columns:
                    if data_col in hd_fields_dict:
                        column_mapping[data_col] = hd_fields_dict[data_col]
                    else:
                        column_mapping[data_col] = (
                            data_col  # Keep unmapped columns as is
                        )

                logger.info(f"column_mapping: {column_mapping}")

                # Check if all required columns are present in the data and identify extra columns
                missing_columns = set(table_column_names) - set(column_mapping.values())
                extra_columns = set(column_mapping.values()) - set(table_column_names)

                if missing_columns or extra_columns:
                    error_message = []
                    if missing_columns:
                        missing_labels = [
                            hd_fields_reverse_dict.get(col, col)
                            for col in missing_columns
                        ]
                        error_message.append(
                            f"Missing columns in data: {', '.join(missing_labels)}"
                        )
                    if extra_columns:
                        extra_labels = [
                            hd_fields_reverse_dict.get(col, col)
                            for col in extra_columns
                        ]
                        error_message.append(
                            f"Extra columns in data: {', '.join(extra_labels)}"
                        )
                    raise ValueError(". ".join(error_message))

                # Reorder and rename the columns in the DataFrame to match the table structure
                data_reordered = data.rename(columns=column_mapping)[table_column_names]
            else:
                # Verify that all columns in the data exist in the table
                missing_columns = set(table_column_names) - set(data.columns)
                extra_columns = set(data.columns) - set(table_column_names)
                if missing_columns or extra_columns:
                    error_message = []
                    if missing_columns:
                        error_message.append(
                            f"Missing columns in data: {', '.join(missing_columns)}"
                        )
                    if extra_columns:
                        error_message.append(
                            f"Extra columns in data: {', '.join(extra_columns)}"
                        )
                    raise ValueError(". ".join(error_message))
                data_reordered = data[table_column_names]

            self.execute("BEGIN TRANSACTION;")

            # Insert data into the table
            self.execute(
                f'INSERT INTO "{org}__{db}"."{tbl}" SELECT * FROM data_reordered'
            )

            # Get the number of rows inserted
            rows_inserted = len(data)

            # Update the number of rows in hd_tables
            self.execute(
                f"""
                UPDATE "{org}__{db}".hd_tables 
                SET nrow = nrow + ? 
                WHERE id = ?
            """,
                [rows_inserted, tbl],
            )

            self.execute("COMMIT;")
            return True
        except duckdb.Error as e:
            self.execute("ROLLBACK;")
            logger.error(f"Error uploading bulk data to table {tbl}: {e}")
            raise QueryError(f"Error uploading bulk data to table {tbl}: {e}")
        except ValueError as e:
            logger.error(str(e))
            raise QueryError(str(e))

    @attach_motherduck
    def update_hd_fields(self, org: str, db: str, fld___id: str, label: str, type: str):
        try:
            query = f'UPDATE "{org}__{db}".hd_fields SET label = ?, type = ? WHERE fld___id = ?'
            self.execute(query, [label, type, fld___id])
        except duckdb.Error as e:
            logger.error(f"Error updating hd_fields: {e}")
            raise QueryError(f"Error updating hd_fields: {e}")

    @attach_motherduck
    def update_row(self, org: str, db: str, tbl: str, id: str, data: dict):
        try:
            # Construct the UPDATE query
            update_query = f'UPDATE "{org}__{db}".{tbl} SET '
            for key in data.keys():  # Removed value
                update_query += f"{key} = ?, "
            update_query = update_query.rstrip(", ") + " WHERE rcd___id = ?"

            # Execute the UPDATE query
            self.execute(update_query, list(data.values()) + [id])
        except duckdb.Error as e:
            logger.error(f"Error updating row in table {tbl}: {e}")
            raise QueryError(f"Error updating row in table {tbl}: {e}")
