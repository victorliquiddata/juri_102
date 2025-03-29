# database/core.py (complete implementation)
import psycopg2
from psycopg2 import pool, sql
from typing import Optional, List, Tuple, Any, Dict, Union
from time import sleep
import logging
from config import DB_CONFIG, SCHEMA_NAME


class DatabaseManager:
    def __init__(self, max_connections: int = 5):
        self.connection_pool = None
        self.current_schema = SCHEMA_NAME  # Should be "jec" from your config
        self.logger = self._setup_logger()
        self._initialize_pool(max_connections)

    def _setup_logger(self):
        logger = logging.getLogger("DBManager")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _initialize_pool(self, max_connections: int):
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, max_connections, **DB_CONFIG
            )
            self.logger.info("Connection pool initialized")
        except psycopg2.Error as e:
            self.logger.error(f"Connection pool initialization failed: {e}")
            raise ConnectionError("Failed to initialize connection pool")

    def get_connection(self, retries: int = 3, delay: float = 1.0):
        """Get a connection from the pool with retry logic"""
        for attempt in range(retries):
            try:
                conn = self.connection_pool.getconn()
                conn.autocommit = False
                self._set_schema(conn, self.current_schema)
                return conn
            except psycopg2.Error as e:
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt == retries - 1:
                    raise ConnectionError(
                        f"Failed to get connection after {retries} attempts"
                    )
                sleep(delay * (attempt + 1))  # Exponential backoff

    def _set_schema(self, conn, schema_name: str) -> bool:
        """Internal method to set schema and verify it exists.

        Args:
            conn: Database connection
            schema_name: Name of schema to activate

        Returns:
            bool: True if schema was set successfully, False otherwise

        Raises:
            ValueError: If schema doesn't exist
            DatabaseError: If schema cannot be set
        """
        try:
            # First verify schema exists
            schema_check = """
            SELECT 1 FROM information_schema.schemata 
            WHERE schema_name = %s
            """
            with conn.cursor() as cursor:
                cursor.execute(schema_check, (schema_name,))
                if not cursor.fetchone():
                    self.logger.error(f"Schema '{schema_name}' does not exist")
                    raise ValueError(f"Schema '{schema_name}' does not exist")

                # Set the schema
                cursor.execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
                )
                conn.commit()

                # Update current schema if successful
                self.current_schema = schema_name
                self.logger.info(f"Schema path set to: {schema_name}")
                return True

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to set schema '{schema_name}': {str(e)}")
            raise psycopg2.DatabaseError(f"Schema change failed: {str(e)}")

    @property
    def connected(self) -> bool:
        """Check if pool is initialized and has available connections"""
        return self.connection_pool is not None and not self.connection_pool.closed

    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        fetch: bool = True,
        autocommit: bool = True,
    ) -> Optional[Tuple[List[str], List[Any]]]:
        """Execute a SQL query using connection pooling"""
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

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Query failed: {e}\nQuery: {query}\nParams: {params}")
            raise RuntimeError(f"Database error: {e}")
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def execute_transaction(self, queries: List[Tuple[str, tuple]]) -> bool:
        """Execute multiple queries in a single transaction using pooling"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                for query, params in queries:
                    cursor.execute(query, params or ())
            conn.commit()
            return True
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Transaction failed: {e}")
            raise RuntimeError(f"Transaction failed: {e}")
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def list_tables(self) -> List[str]:
        """List all tables in current schema (pooled version)"""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        result = self.execute_query(query, (self.current_schema,))
        return [row[0] for row in result[1]] if result else []

    def describe_table(self, table_name: str) -> List[Tuple]:
        """Get table structure description (pooled version)"""
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
    ) -> Optional[Tuple[List[str], List[Any]]]:
        """Preview table data with limit (pooled version)"""
        query = sql.SQL("SELECT * FROM {} LIMIT {}").format(
            sql.Identifier(table_name), sql.Literal(limit)
        )
        return self.execute_query(query)

    def export_table(self, table_name: str, output_file: str) -> None:
        """Export table data to CSV using connection pooling"""
        conn = None
        try:
            conn = self.get_connection()
            query = sql.SQL("COPY {} TO STDOUT WITH CSV HEADER").format(
                sql.Identifier(table_name)
            )

            with open(output_file, "w") as f:
                with conn.cursor() as cursor:
                    cursor.copy_expert(query, f)
            conn.commit()
        except (psycopg2.Error, IOError) as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Export failed: {e}")
            raise RuntimeError(f"Error exporting table: {e}")
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def get_database_info(self) -> dict:
        """Get database information (pooled version)"""
        query = """
        SELECT 
            current_database() as database_name,
            current_schema() as current_schema,
            version() as postgres_version,
            (SELECT COUNT(*) FROM information_schema.tables 
             WHERE table_schema = current_schema()) as table_count
        """
        result = self.execute_query(query)
        if result:
            columns, data = result
            return dict(zip(columns, data[0]))
        return {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_pool()

    def close_pool(self):
        """Close all connections in the pool"""
        if self.connection_pool and not self.connection_pool.closed:
            self.connection_pool.closeall()
            self.logger.info("Connection pool closed")

    # Backward compatibility for existing code
    def connect(self, config=None) -> bool:
        """Maintained for backward compatibility (pool is initialized in __init__)"""
        self.logger.warning("connect() is deprecated with connection pooling")
        return self.connected

    def disconnect(self) -> None:
        """Maintained for backward compatibility"""
        self.logger.warning("disconnect() is deprecated with connection pooling")
        self.close_pool()

    def set_schema(self, schema_name: str) -> bool:
        """Set the current schema (updated for pooling)"""
        if not self.connected:
            raise ConnectionError("Connection pool not available")

        conn = None
        try:
            conn = self.get_connection()
            self._set_schema(conn, schema_name)
            self.current_schema = schema_name
            return True
        except psycopg2.Error as e:
            self.logger.error(f"Schema change failed: {e}")
            raise RuntimeError(f"Error setting schema: {e}")
        finally:
            if conn:
                self.connection_pool.putconn(conn)
