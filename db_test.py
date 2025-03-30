import pytest
from database.core import DatabaseManager
from config import DB_CONFIG, SCHEMA_NAME


from unittest.mock import patch, MagicMock


@pytest.fixture
def db():
    """Fixture to provide a database manager instance for tests."""
    manager = DatabaseManager()
    yield manager
    manager.close_pool()


class TestDatabaseManager:
    """Test suite for DatabaseManager class."""

    def test_connection(self, db):
        """Test database connection is established."""
        assert db.connected is True

    def test_schema_setting(self, db):
        """Test setting schema."""
        assert db.current_schema == SCHEMA_NAME
        assert db.set_schema(SCHEMA_NAME) is True

    def test_get_database_info(self, db):
        """Test retrieving database information."""
        info = db.get_database_info()
        assert isinstance(info, dict)
        assert "database_name" in info
        assert "current_schema" in info
        assert "postgres_version" in info
        assert info["current_schema"] == SCHEMA_NAME

    def test_list_tables(self, db):
        """Test listing tables in schema."""
        tables = db.list_tables()
        assert isinstance(tables, list)

        # Check for minimum expected tables
        required_tables = ["usuarios", "partes", "processos", "documentos"]

        # Check all required tables exist
        for table in required_tables:
            assert table in tables, f"Required table '{table}' not found"

        # Optional tables (don't fail if missing)
        optional_tables = ["categorias_causas", "partes_processo", "processos_ativos"]

        # Log which optional tables are present
        present_optional = [t for t in optional_tables if t in tables]
        if present_optional:
            print(f"Found optional tables: {', '.join(present_optional)}")

    def test_describe_table(self, db):
        """Test describing a table structure."""
        # First ensure the table exists
        tables = db.list_tables()
        if "usuarios" not in tables:
            pytest.skip("'usuarios' table not available for testing")

        description = db.describe_table("usuarios")
        assert isinstance(description, list)
        assert len(description) > 0

        # Expected column names in usuarios table
        required_columns = ["id", "nome_completo", "email", "tipo"]

        optional_columns = ["cpf", "senha", "telefone", "data_cadastro", "ultimo_login"]

        column_names = [col[0] for col in description]

        # Check required columns
        for col in required_columns:
            assert col in column_names, f"Required column '{col}' not found"

        # Check optional columns
        found_optional = [col for col in optional_columns if col in column_names]
        if found_optional:
            print(f"Found optional columns: {', '.join(found_optional)}")

    def test_preview_table(self, db):
        """Test previewing table data."""
        # First ensure the table exists
        tables = db.list_tables()
        if "usuarios" not in tables:
            pytest.skip("'usuarios' table not available for testing")

        preview = db.preview_table("usuarios", 2)
        assert preview is not None
        columns, rows = preview

        # Check columns returned
        assert "id" in columns
        assert "nome_completo" in columns
        assert "email" in columns

        # Check rows returned
        assert isinstance(rows, list)
        assert len(rows) <= 2  # Might be less if table has fewer rows

    def test_execute_query(self, db):
        """Test executing a query."""
        # First ensure the table exists
        tables = db.list_tables()
        if "usuarios" not in tables:
            pytest.skip("'usuarios' table not available for testing")

        query = "SELECT * FROM usuarios WHERE tipo = %s LIMIT 1"
        result = db.execute_query(query, ("servidor",))

        assert result is not None
        columns, rows = result
        assert len(rows) <= 1  # Should have 0 or 1 rows

        if len(rows) > 0:
            row_dict = dict(zip(columns, rows[0]))
            assert row_dict["tipo"] == "servidor"

    def test_transaction_rollback(self, db):
        """Test transaction rollback on error."""
        # First, get a user to test with
        user_query = "SELECT id FROM usuarios LIMIT 1"
        result = db.execute_query(user_query)
        if not result or not result[1]:
            pytest.skip("No users available for testing")

        user_id = result[1][0][0]

        # Create an invalid transaction (second query will fail)
        queries = [
            ("SELECT 1", None),  # This will succeed
            (
                "UPDATE nonexistent_table SET field = 'value' WHERE id = %s",
                (user_id,),  # Use parameterized query
            ),  # This will fail
        ]

        # Test transaction rollback
        with pytest.raises(RuntimeError):
            db.execute_transaction(queries)

    @pytest.mark.parametrize("table_name", ["usuarios"])
    def test_export_table(self, db, table_name, tmp_path):
        """Test exporting table to CSV."""
        # First ensure the table exists
        tables = db.list_tables()
        if table_name not in tables:
            pytest.skip(f"'{table_name}' table not available for testing")

        # Create a temporary file path
        export_path = tmp_path / f"{table_name}_export.csv"

        # Export the table
        db.export_table(table_name, str(export_path))

        # Check the file exists and is not empty
        assert export_path.exists()
        assert export_path.stat().st_size > 0

        # Verify file contains header line and at least one data line
        with open(export_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            assert len(lines) >= 2  # Header + at least one data row

    def test_connection_context_manager(self):
        """Test using database manager as context manager."""
        with DatabaseManager() as db:
            assert db.connected is True
            info = db.get_database_info()
            assert isinstance(info, dict)
            assert info["current_schema"] == SCHEMA_NAME

        # Verify pool is closed but still accessible
        assert hasattr(db, "connection_pool"), "Pool reference should still exist"
        assert db.connection_pool.closed is True, "Pool should be closed after context"

    @patch("database.core.SimpleConnectionPool")
    def test_initialization(self, mock_pool):
        """Test database manager initialization."""
        mock_pool_instance = MagicMock()
        mock_pool.return_value = mock_pool_instance

        # Need to patch the actual import location, not psycopg2.pool
        with patch("database.core.SimpleConnectionPool", mock_pool):
            db = DatabaseManager()

        assert db.connection_pool is not None
        mock_pool.assert_called_once_with(minconn=1, maxconn=5, **DB_CONFIG)

    @patch("psycopg2.pool.SimpleConnectionPool")
    def test_close_pool(self, mock_pool):
        """Test connection pool closure."""
        mock_pool_instance = MagicMock()
        mock_pool.return_value = mock_pool_instance

        db = DatabaseManager()
        db.close_pool()

        mock_pool_instance.closeall.assert_called_once()


# Additional test functions for specific database operations


def test_get_usuario_by_id(db):
    """Test retrieving a user by ID."""
    # First get a valid user ID
    result = db.execute_query("SELECT id FROM usuarios LIMIT 1")
    if not result or not result[1]:
        pytest.skip("No users available for testing")

    user_id = result[1][0][0]

    # Now retrieve the full user record
    query = "SELECT * FROM usuarios WHERE id = %s"
    user_result = db.execute_query(query, (user_id,))

    assert user_result is not None
    columns, rows = user_result
    assert len(rows) == 1

    # Convert to dict for easier assertion
    user_dict = dict(zip(columns, rows[0]))
    assert user_dict["id"] == user_id
    assert "nome_completo" in user_dict
    assert "email" in user_dict


def test_filter_usuarios_by_tipo(db):
    """Test filtering users by tipo."""
    # First ensure the table exists
    tables = db.list_tables()
    if "usuarios" not in tables:
        pytest.skip("'usuarios' table not available for testing")

    tipos = ["servidor", "parte"]

    for tipo in tipos:
        query = "SELECT * FROM usuarios WHERE tipo = %s"
        result = db.execute_query(query, (tipo,))

        if result and result[1]:
            columns, rows = result
            user_dict = dict(zip(columns, rows[0]))
            assert user_dict["tipo"] == tipo


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
