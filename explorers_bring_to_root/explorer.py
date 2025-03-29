import os
from dotenv import load_dotenv, dotenv_values
from sqlalchemy import create_engine, text
import pandas as pd


class PostgreSQLSchemaExplorer:
    def __init__(self, db_config, schema):
        """
        Initialize database connection parameters

        Args:
            db_config (dict): Database connection configuration
            schema (str): Schema to explore
        """
        # Construct connection string
        self.connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        self.schema = schema

        # Create SQLAlchemy engine
        try:
            self.engine = create_engine(self.connection_string)
        except Exception as e:
            print(f"Error creating database engine: {e}")
            raise

    def get_table_columns(self):
        """
        Retrieve column information for tables in specified schema

        Returns:
            pandas.DataFrame: Table with column details
        """
        query = text(
            """
        SELECT 
            table_name, 
            column_name, 
            data_type, 
            is_nullable,
            character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = :schema
        ORDER BY table_name, ordinal_position
        """
        )

        with self.engine.connect() as connection:
            return pd.read_sql(query, connection, params={"schema": self.schema})

    def get_foreign_keys(self):
        """
        Retrieve foreign key relationships

        Returns:
            pandas.DataFrame: Table with foreign key details
        """
        query = text(
            """
        SELECT
            tc.table_name, 
            kcu.column_name AS source_column, 
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            tc.constraint_name
        FROM 
            information_schema.table_constraints tc
        JOIN 
            information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN 
            information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE 
            tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_schema = :schema
        """
        )

        with self.engine.connect() as connection:
            return pd.read_sql(query, connection, params={"schema": self.schema})

    def get_table_sizes(self):
        """
        Retrieve table sizes and row counts

        Returns:
            pandas.DataFrame: Table with size and row count details
        """
        query = text(
            """
        SELECT 
            relname AS table_name,
            pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
            pg_size_pretty(pg_relation_size(relid)) AS table_size,
            n_live_tup AS row_count,
            n_dead_tup AS dead_rows
        FROM 
            pg_stat_user_tables
        WHERE 
            schemaname = :schema
        ORDER BY 
            pg_total_relation_size(relid) DESC
        """
        )

        with self.engine.connect() as connection:
            return pd.read_sql(query, connection, params={"schema": self.schema})

    def explore_schema(self, detailed=True):
        """
        Comprehensive schema exploration method

        Args:
            detailed (bool): If True, print full details. If False, print summary.
        """
        print(f"=== Schema Exploration for '{self.schema}' ===\n")

        # Table Columns
        columns_df = self.get_table_columns()
        print("1. Tables and Columns:")
        if detailed:
            print(columns_df.to_string(index=False))
        else:
            # Summary view
            summary = (
                columns_df.groupby("table_name").size().reset_index(name="column_count")
            )
            print(summary.to_string(index=False))
        print("\n")

        # Foreign Keys
        fk_df = self.get_foreign_keys()
        print("2. Foreign Key Relationships:")
        if not fk_df.empty:
            if detailed:
                print(fk_df.to_string(index=False))
            else:
                # Summary view
                print(f"Found {len(fk_df)} foreign key relationships")
        else:
            print("No foreign keys found.")
        print("\n")

        # Table Sizes
        sizes_df = self.get_table_sizes()
        print("3. Table Sizes and Row Counts:")
        if detailed:
            print(sizes_df.to_string(index=False))
        else:
            # Summary view
            summary = sizes_df[["table_name", "total_size", "row_count"]]
            print(summary.to_string(index=False))


def load_configuration():
    """
    Load configuration from .env file

    Returns:
        tuple: Database configuration and schema name
    """
    # Attempt to load .env file
    load_dotenv()

    # Try to get configuration from environment variables
    db_config = {
        "host": os.getenv("DB_HOST", "192.168.1.66"),
        "database": os.getenv("DB_DATABASE", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "1234"),
        "port": os.getenv("DB_PORT", "5432"),
    }

    # Get schema name
    schema_name = os.getenv("SCHEMA_NAME", "jec")

    return db_config, schema_name


def main():
    # Load configuration
    db_config, schema_name = load_configuration()

    # Create explorer
    explorer = PostgreSQLSchemaExplorer(db_config, schema_name)

    # Explore schema (set detailed=False for a quick summary)
    explorer.explore_schema(detailed=True)


if __name__ == "__main__":
    main()
