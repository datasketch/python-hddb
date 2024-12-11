from typing import List, Dict
import pandas as pd
import duckdb
from loguru import logger


from python_hddb.exceptions import TableExistsError, QueryError
from python_hddb.utils.helpers import generate_field_metadata, clean_dataframe
from python_hddb.utils.decorators import attach_motherduck


class TableOperations:
    """
    Handle all table-level database operations.

    This class provides a clean interface for table operations like:
    - Creating new tables from DataFrames
    - Dropping existing tables
    - Adding/removing columns
    - Managing table metadata

    All operations are transactional and handle errors appropriately.
    """

    def __init__(self, db_client):
        """Initialize with a database client connection."""
        self.client = db_client

    # Database Operations
    @attach_motherduck
    def create_database(
        self, org: str, db: str, dataframes: List[pd.DataFrame], names: List[str]
    ) -> None:
        """
        Create database and tables from a list of dataframes.

        Args:
            org: Organization name
            db: Database name
            dataframes: List of pandas DataFrames for table creation
            names: List of table names

        Raises:
            ValueError: If number of dataframes doesn't match table names
            QueryError: If database creation fails
        """
        if len(dataframes) != len(names):
            raise ValueError(
                "The number of dataframes must match the number of table names"
            )

        try:
            self._create_hd_database(org, db, len(dataframes))

            for df, table_name in zip(dataframes, names):
                self.create_table(org, db, table_name, df)

        except (duckdb.Error, Exception) as e:
            raise QueryError(f"Error creating database: {e}", {"org": org, "db": db})

    @attach_motherduck
    def drop_database(self, org: str, db: str) -> None:
        """
        Delete a database.

        Args:
            org: Organization name
            db: Database name

        Raises:
            QueryError: If database deletion fails
        """
        try:
            self.client.execute(f'DROP DATABASE "{org}__{db}";')
            logger.info(f"Database {org}__{db} successfully deleted")
        except duckdb.Error as e:
            raise QueryError(f"Error deleting database: {e}", {"org": org, "db": db})

    def _create_hd_database(self, org: str, db: str, tables: int) -> None:
        """
        Initialize database metadata tables.

        Args:
            org: Organization name
            db: Database name
            tables: Number of tables to be created

        Raises:
            QueryError: If metadata table creation fails
        """
        try:
            self.client.execute("BEGIN TRANSACTION;")

            create_query = """
                CREATE TABLE hd_database (
                    id VARCHAR, 
                    username VARCHAR, 
                    slug VARCHAR, 
                    db_created_at TIMESTAMP DEFAULT current_timestamp,
                    db_updated_at TIMESTAMP DEFAULT current_timestamp,
                    db_n_tables INTEGER
                );
            """
            self.client.execute(create_query)

            insert_query = "INSERT INTO hd_database (id, username, slug, db_n_tables) VALUES (?, ?, ?, ?);"
            self.client.execute(insert_query, [f"{org}__{db}", org, db, tables])

            self.client.execute("COMMIT;")
        except duckdb.Error as e:
            self.client.execute("ROLLBACK;")
            raise QueryError(f"Error creating database metadata: {e}")

    @attach_motherduck
    def create_table(self, org: str, db: str, tbl: str, df: pd.DataFrame) -> None:
        """
        Create a new table from DataFrame.

        Args:
            org: Organization name
            db: Database name
            tbl: Table name
            df: Source DataFrame

        Raises:
            TableExistsError: If table already exists
            QueryError: For other database errors
        """
        try:
            # Generate metadata for the new table
            metadata = generate_field_metadata(df)

            # Create a mapping of original column names to new IDs and rename columns
            df_renamed = clean_dataframe(
                df.rename(columns={field["label"]: field["id"] for field in metadata})
            )

            # Begin transaction
            self.client.execute("BEGIN TRANSACTION;")

            # Create the new table with all columns as VARCHAR
            column_definitions = ", ".join(
                f'"{col}" VARCHAR' for col in df_renamed.columns
            )
            create_table_query = (
                f'CREATE TABLE "{org}__{db}"."{tbl}" ({column_definitions})'
            )
            self.client.execute(create_table_query)

            # Insert all data at once
            self.client.execute(
                f'INSERT INTO "{org}__{db}"."{tbl}" SELECT * FROM df_renamed'
            )

            # Insert metadata
            self._insert_table_metadata(org, db, tbl, df_renamed, metadata)

            self.client.execute("COMMIT;")
        except (duckdb.CatalogException, duckdb.Error) as e:
            self.client.execute("ROLLBACK;")
            if isinstance(e, duckdb.CatalogException):
                raise TableExistsError(tbl, {"error": str(e), "org": org, "db": db})
            raise QueryError(
                f"Error creating table: {e}", {"table": tbl, "org": org, "db": db}
            )

    @attach_motherduck
    def drop_table(self, org: str, db: str, tbl: str) -> None:
        """
        Delete a table and its metadata.

        Args:
            org: Organization name
            db: Database name
            tbl: Table name to drop

        Raises:
            QueryError: If deletion fails
        """
        try:
            self.client.execute("BEGIN TRANSACTION;")

            # Drop the table
            self.client.execute(f'DROP TABLE IF EXISTS "{org}__{db}"."{tbl}";')

            # Clean up metadata
            self.client.execute(
                f'DELETE FROM "{org}__{db}".hd_tables WHERE id = ?', [tbl]
            )
            self.client.execute(
                f'DELETE FROM "{org}__{db}".hd_fields WHERE tbl = ?', [tbl]
            )

            self.client.execute("COMMIT;")
            logger.info(f"Table {tbl} successfully deleted from database {org}__{db}")
        except duckdb.Error as e:
            self.client.execute("ROLLBACK;")
            raise QueryError(
                f"Error deleting table: {e}", {"table": tbl, "org": org, "db": db}
            )

    def _insert_table_metadata(
        self,
        org: str,
        db: str,
        tbl: str,
        df: pd.DataFrame,
        metadata: List[Dict[str, str]],
    ) -> None:
        """
        Insert table metadata into hd_tables and hd_fields.

        Args:
            org: Organization name
            db: Database name
            tbl: Table name
            df: DataFrame containing the data
            metadata: List of field metadata dictionaries
        """
        # Insert into hd_tables
        self.client.execute(
            f'INSERT INTO "{org}__{db}".hd_tables (id, label, nrow, ncol) VALUES (?, ?, ?, ?)',
            [tbl, tbl, len(df), len(df.columns)],
        )

        # Create and populate temp_metadata
        self.client.execute(
            "CREATE TEMP TABLE temp_metadata (fld___id VARCHAR, id VARCHAR, label VARCHAR, tbl VARCHAR)"
        )
        for field in metadata:
            self.client.execute(
                "INSERT INTO temp_metadata VALUES (?, ?, ?, ?)",
                (field["fld___id"], field["id"], field["label"], tbl),
            )

        # Insert into hd_fields
        self.client.execute(
            f"""
            INSERT INTO "{org}__{db}".hd_fields (fld___id, id, label, tbl, type)
            SELECT
                tm.fld___id,
                tm.id,
                tm.label,
                '{tbl}' AS tbl,
                'Txt' AS type
            FROM temp_metadata tm
            """
        )
        self.client.execute("DROP TABLE temp_metadata")

    @attach_motherduck
    def add_column(self, org: str, db: str, tbl: str, column: dict) -> bool:
        """
        Add a new column to existing table.

        Args:
            org: Organization name
            db: Database name
            tbl: Target table name
            column: Column definition dictionary with keys:
                   - slug: Column identifier
                   - fld___id: Field ID
                   - headerName: Display name
                   - type: Data type

        Returns:
            bool: True if successful

        Raises:
            QueryError: If column addition fails
        """
        try:
            self.client.execute("BEGIN TRANSACTION;")

            # Add the column to the table
            self.client.execute(
                f'ALTER TABLE "{org}__{db}"."{tbl}" ADD COLUMN "{column["slug"]}" VARCHAR'
            )

            # Update metadata
            self.client.execute(
                f'UPDATE "{org}__{db}".hd_tables SET ncol = ncol + 1 WHERE id = ?',
                [tbl],
            )
            self.client.execute(
                f'INSERT INTO "{org}__{db}".hd_fields (fld___id, id, label, tbl, type) VALUES (?, ?, ?, ?, ?)',
                [
                    column["fld___id"],
                    column["slug"],
                    column["headerName"],
                    tbl,
                    column["type"],
                ],
            )

            self.client.execute("COMMIT;")
            return True
        except duckdb.Error as e:
            self.client.execute("ROLLBACK;")
            raise QueryError(
                f"Error adding column: {e}",
                {"table": tbl, "column": column["slug"], "org": org, "db": db},
            )

    @attach_motherduck
    def delete_column(self, org: str, db: str, tbl: str, column: dict) -> bool:
        """
        Delete a column from table.

        Args:
            org: Organization name
            db: Database name
            tbl: Target table name
            column: Column definition dictionary with keys:
                   - slug: Column identifier
                   - fld___id: Field ID

        Returns:
            bool: True if successful

        Raises:
            QueryError: If column deletion fails
        """
        try:
            column_name = column["slug"]
            column_id = column["fld___id"]

            self.client.execute("BEGIN TRANSACTION;")

            # Drop the column
            self.client.execute(
                f'ALTER TABLE "{org}__{db}"."{tbl}" DROP COLUMN "{column_name}"'
            )

            # Update metadata
            self.client.execute(
                f'UPDATE "{org}__{db}".hd_tables SET ncol = ncol - 1 WHERE id = ?',
                [tbl],
            )
            self.client.execute(
                f'DELETE FROM "{org}__{db}".hd_fields WHERE fld___id = ? AND tbl = ?',
                [column_id, tbl],
            )

            self.client.execute("COMMIT;")
            return True
        except duckdb.Error as e:
            self.client.execute("ROLLBACK;")
            raise QueryError(
                f"Error deleting column: {e}",
                {"table": tbl, "column": column_name, "org": org, "db": db},
            )

    # Metadata Operations
    @attach_motherduck
    def get_table_metadata(self, org: str, db: str, tbl: str) -> Dict:
        """
        Get table metadata information.

        Args:
            org: Organization name
            db: Database name
            tbl: Table name

        Returns:
            Dict containing:
                - nrow: Number of rows
                - ncol: Number of columns
                - tbl_name: Table name

        Raises:
            QueryError: If metadata retrieval fails
        """
        try:
            query = f'SELECT * FROM "{org}__{db}".hd_tables WHERE id = ?'
            result = self.client.execute(query, [tbl]).fetchdf()
            data = result.to_dict(orient="records")

            if not data:
                raise QueryError(
                    f"Table {tbl} not found", {"org": org, "db": db, "table": tbl}
                )

            return {
                "nrow": data[0]["nrow"],
                "ncol": data[0]["ncol"],
                "tbl_name": data[0]["id"],
            }
        except duckdb.Error as e:
            raise QueryError(
                f"Error fetching table metadata: {e}",
                {"org": org, "db": db, "table": tbl},
            )

    @attach_motherduck
    def update_field_metadata(
        self, org: str, db: str, fld___id: str, label: str, type: str
    ) -> None:
        """
        Update field metadata.

        Args:
            org: Organization name
            db: Database name
            fld___id: Field ID to update
            label: New field label
            type: New field type

        Raises:
            QueryError: If metadata update fails
        """
        try:
            query = f'UPDATE "{org}__{db}".hd_fields SET label = ?, type = ? WHERE fld___id = ?'
            self.client.execute(query, [label, type, fld___id])
        except duckdb.Error as e:
            raise QueryError(
                f"Error updating field metadata: {e}",
                {
                    "org": org,
                    "db": db,
                    "field_id": fld___id,
                    "new_label": label,
                    "new_type": type,
                },
            )
