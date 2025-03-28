import psycopg2
from psycopg2 import sql
from typing import Optional, List, Tuple, Any, Union
from config import DB_CONFIG, SCHEMA_NAME


class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.connected = False
        self.current_schema = SCHEMA_NAME

    def connect(self, config=None) -> bool:
        """Connect to database with optional custom config"""
        config = config or DB_CONFIG
        try:
            self.conn = psycopg2.connect(**config)
            self.connected = True
            self.set_schema(self.current_schema)  # Set default schema on connect
            return True
        except psycopg2.Error as e:
            raise ConnectionError(f"Unable to connect to database: {e}")

    def disconnect(self) -> None:
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.connected = False

    def set_schema(self, schema_name: str) -> bool:
        """Set the current schema"""
        if not self.connected:
            raise ConnectionError("Not connected to database")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
                )
                self.conn.commit()
                self.current_schema = schema_name
                return True
        except psycopg2.Error as e:
            raise RuntimeError(f"Error setting schema: {e}")

    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        fetch: bool = True,
        autocommit: bool = True,  # New parameter
    ) -> Optional[Tuple[List[str], List[Any]]]:
        """Execute a SQL query and return results

        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results
            autocommit: Whether to commit automatically (default True)
        """
        if not self.connected:
            raise ConnectionError("Not connected to database")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)

                if fetch and cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    if autocommit:
                        self.conn.commit()  # Explicit commit after fetch
                    return columns, results

                if autocommit:
                    self.conn.commit()  # Explicit commit for non-fetch queries
                return None

        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Error executing query: {e}")

    # Add this new method for explicit transaction control
    def execute_transaction(self, queries: List[Tuple[str, tuple]]) -> bool:
        """Execute multiple queries in a single transaction"""
        if not self.connected:
            raise ConnectionError("Not connected to database")

        try:
            with self.conn.cursor() as cursor:
                for query, params in queries:
                    cursor.execute(query, params)
            self.conn.commit()
            return True
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Transaction failed: {e}")

    def list_tables(self) -> List[str]:
        """List all tables in the current schema"""
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
        """Get table structure description"""
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
        """Preview table data with limit"""
        query = sql.SQL("SELECT * FROM {} LIMIT {}").format(
            sql.Identifier(table_name), sql.Literal(limit)
        )
        return self.execute_query(query)

    def export_table(self, table_name: str, output_file: str) -> None:
        """Export table data to a CSV file"""
        if not self.connected:
            raise ConnectionError("Not connected to database")

        query = sql.SQL("COPY {} TO STDOUT WITH CSV HEADER").format(
            sql.Identifier(table_name)
        )

        try:
            with open(output_file, "w") as f:
                with self.conn.cursor() as cursor:
                    cursor.copy_expert(query, f)
        except (psycopg2.Error, IOError) as e:
            raise RuntimeError(f"Error exporting table: {e}")

    def get_database_info(self) -> dict:
        """Get comprehensive database information"""
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
