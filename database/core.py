# database/core.py (final corrected version)
import psycopg2
from pathlib import Path  # Add this with other imports

from psycopg2.pool import SimpleConnectionPool  # Explicit import
from psycopg2 import (
    sql,
    DatabaseError,  # Primary error handling
    OperationalError,  # Used in connection retry logic
    InterfaceError,  # Used in connection validation
)


from typing import Optional, List, Tuple, Any, Dict, Union
from time import sleep
import logging
from config import DB_CONFIG, SCHEMA_NAME


class DatabaseManager:
    def __init__(self, max_connections: int = 5) -> None:
        """Initialize the database connection pool."""
        self.connection_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
        self.current_schema: str = SCHEMA_NAME
        self.logger: logging.Logger = self._setup_logger()
        self._initialize_pool(max_connections)

    def _setup_logger(self) -> logging.Logger:
        """Configure and return a logger instance."""
        logger = logging.getLogger("DBManager")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _initialize_pool(self, max_connections: int) -> None:
        """Initialize the connection pool with retry logic."""
        try:
            self.connection_pool = SimpleConnectionPool(1, max_connections, **DB_CONFIG)
            self.logger.info("Connection pool initialized successfully")
        except DatabaseError as e:
            self.logger.error("Connection pool initialization failed: %s", e)
            raise ConnectionError("Failed to initialize connection pool") from e

    @property
    def connected(self) -> bool:
        """Check if pool is initialized and has available connections."""
        if not self.connection_pool or self.connection_pool.closed:
            return False

        try:
            # Test by getting/returning a connection
            conn = self.connection_pool.getconn()
            self.connection_pool.putconn(conn)
            return True
        except DatabaseError:
            return False

    def get_connection(
        self, retries: int = 3, delay: float = 1.0
    ) -> psycopg2.extensions.connection:
        """Get a connection from the pool with retry logic."""
        for attempt in range(retries):
            try:
                conn = self.connection_pool.getconn()
                conn.autocommit = False
                self._set_schema(conn, self.current_schema)
                return conn
            except (OperationalError, InterfaceError) as e:
                self.logger.warning("Connection attempt %d failed: %s", attempt + 1, e)
                if attempt == retries - 1:
                    raise ConnectionError(
                        f"Failed to get connection after {retries} attempts"
                    ) from e
                sleep(delay * (attempt + 1))  # Exponential backoff

    def _set_schema(
        self, conn: psycopg2.extensions.connection, schema_name: str
    ) -> None:
        """Internal method to set schema and verify it exists."""
        try:
            with conn.cursor() as cursor:
                # Verify schema exists
                cursor.execute(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                    (schema_name,),
                )
                if not cursor.fetchone():
                    raise ValueError(f"Schema '{schema_name}' does not exist")

                # Set the schema
                cursor.execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
                )
                conn.commit()
                self.current_schema = schema_name
                self.logger.debug("Schema path set to: {schema_name}")

        except Exception as e:
            conn.rollback()
            self.logger.error("Failed to set schema '{schema_name}': {str(e)}")
            raise DatabaseError(f"Schema change failed: {str(e)}") from e

    def execute_query(
        self,
        query: str,
        params: Optional[Union[Tuple[Any, ...], Dict[str, Any]]] = None,  # Fixed
        fetch: bool = True,
        autocommit: bool = True,
    ) -> Optional[Tuple[List[str], List[Tuple[Any, ...]]]]:  # Fixed
        """Execute a SQL query using connection pooling."""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params or ())

                if fetch and cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    if autocommit:
                        conn.commit()
                    return columns, results

                if autocommit:
                    conn.commit()
                return None

        except DatabaseError as e:  # Standardized
            if conn:
                conn.rollback()
            self.logger.error("Query failed: {e}\nQuery: {query}\nParams: {params}")
            raise RuntimeError("Database error: {e}") from e
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def execute_transaction(
        self,
        queries: List[Tuple[str, Optional[Union[Tuple[Any, ...], Dict[str, Any]]]]],
    ) -> bool:
        """Execute multiple queries in a single transaction."""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                for query, params in queries:
                    cursor.execute(query, params or ())
            conn.commit()
            return True

        except DatabaseError as e:
            if conn:
                conn.rollback()
            self.logger.error("Transaction failed: {e}")
            raise RuntimeError("Transaction failed: {e}") from e

        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def list_tables(self) -> List[str]:
        """List all tables in current schema."""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        result = self.execute_query(query, (self.current_schema,))
        return [row[0] for row in result[1]] if result else []

    def describe_table(self, table_name: str) -> List[Tuple[str, str, str, str]]:
        """Get table structure description."""
        query = """
        SELECT column_name, data_type, is_nullable, 
               COALESCE(column_default, 'NULL') as column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        result = self.execute_query(query, (self.current_schema, table_name))
        return result[1] if result else []

    def preview_table(
        self, table_name: str, limit: int = 5
    ) -> Optional[Tuple[List[str], List[Tuple[Any, ...]]]]:
        """Preview table data with limit."""
        query = sql.SQL("SELECT * FROM {} LIMIT {}").format(
            sql.Identifier(table_name), sql.Literal(limit)
        )
        return self.execute_query(query)

    def export_table(self, table_name: str, output_file: str) -> None:
        """Export table data to CSV with proper path handling."""
        conn = None
        try:
            # Convert to Path object for robust path handling
            output_path = Path(output_file)

            # Ensure parent directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            conn = self.get_connection()
            query = sql.SQL("COPY {} TO STDOUT WITH CSV HEADER").format(
                sql.Identifier(table_name)
            )

            with output_path.open("w", encoding="utf-8") as f:
                with conn.cursor() as cursor:
                    cursor.copy_expert(query, f)
            conn.commit()
            self.logger.info("Successfully exported %s to %s", table_name, output_file)

        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error("Export failed: %s", str(e), exc_info=True)
            raise RuntimeError(f"Error exporting table: {str(e)}") from e
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def get_database_info(self) -> Dict[str, Any]:
        """Get database information."""
        query = """
        SELECT 
            current_database() as database_name,
            current_schema() as current_schema,
            version() as postgres_version,
            (SELECT COUNT(*) FROM information_schema.tables 
             WHERE table_schema = current_schema()) as table_count
        """
        result = self.execute_query(query)
        return dict(zip(result[0], result[1][0])) if result else {}

    def __enter__(self) -> "DatabaseManager":
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        self.close_pool()

    def close_pool(self) -> None:
        """Close all connections in the pool."""
        if self.connection_pool and not self.connection_pool.closed:
            self.connection_pool.closeall()
            self.logger.info("Connection pool closed")

    # Backward compatibility methods
    def connect(self) -> bool:
        """Maintained for backward compatibility."""
        self.logger.warning("connect() is deprecated with connection pooling")
        return self.connected

    def disconnect(self) -> None:
        """Maintained for backward compatibility."""
        self.logger.warning("disconnect() is deprecated with connection pooling")
        self.close_pool()

    def set_schema(self, schema_name: str) -> bool:
        """Set the current schema."""
        if not self.connected:
            raise ConnectionError("Connection pool not available")

        conn = None
        try:
            conn = self.get_connection()
            self._set_schema(conn, schema_name)
            return True

        except DatabaseError as e:
            self.logger.error("Schema change failed: {e}")
            raise RuntimeError(f"Error setting schema: {e}") from e

        finally:
            if conn:
                self.connection_pool.putconn(conn)
