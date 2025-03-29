from typing import Optional, List, Dict, Any
from database.core import DatabaseManager
from datetime import datetime


class Usuario:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.table_name = f"{db.current_schema}.usuarios"  # Use db's current schema
        self.required_fields = ["cpf", "nome_completo", "email", "senha", "tipo"]
        self.optional_fields = ["telefone"]
        self.valid_types = ["servidor", "juiz", "advogado", "parte"]

    def _validate_fields(self, data: Dict[str, Any]) -> None:
        """Validate required fields and field types"""
        missing_fields = [field for field in self.required_fields if field not in data]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

        if data.get("tipo") not in self.valid_types:
            raise ValueError(f"Tipo must be one of: {', '.join(self.valid_types)}")

    def create(self, data: Dict[str, Any]) -> Optional[str]:
        """Create a new usuario record with guaranteed commit"""
        self._validate_fields(data)

        # Set default values for optional fields
        for field in self.optional_fields:
            if field not in data:
                data[field] = None

        columns = data.keys()
        values = [data[col] for col in columns]

        query = f"""
        INSERT INTO {self.table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(values))})
        RETURNING id
        """

        try:
            # Force commit regardless of fetch
            result = self.db.execute_query(
                query, tuple(values), fetch=True, autocommit=True
            )
            return str(result[1][0][0]) if result else None
        except Exception as e:
            raise RuntimeError(f"Error creating usuario: {str(e)}")

    def get_by_id(self, usuario_id: str) -> Optional[Dict[str, Any]]:
        """Get a usuario by ID"""
        query = f"""
        SELECT * FROM {self.table_name}
        WHERE id = %s
        """

        result = self.db.execute_query(query, (usuario_id,))
        if result and result[1]:
            columns, rows = result
            return dict(zip(columns, rows[0]))
        return None

    def update(self, usuario_id: str, data: Dict[str, Any]) -> bool:
        """Update a usuario record"""
        if not data:
            raise ValueError("No fields to update provided")

        # Validate tipo if it's being updated
        if "tipo" in data and data["tipo"] not in self.valid_types:
            raise ValueError(f"Tipo must be one of: {', '.join(self.valid_types)}")

        set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
        values = list(data.values())
        values.append(usuario_id)

        query = f"""
        UPDATE {self.table_name}
        SET {set_clause}
        WHERE id = %s
        """

        try:
            self.db.execute_query(query, tuple(values), fetch=False)
            return True
        except Exception as e:
            raise RuntimeError(f"Error updating usuario: {str(e)}")

    def delete(self, usuario_id: str) -> bool:
        """Delete a usuario record"""
        query = f"""
        DELETE FROM {self.table_name}
        WHERE id = %s
        """

        try:
            self.db.execute_query(query, (usuario_id,), fetch=False)
            return True
        except Exception as e:
            raise RuntimeError(f"Error deleting usuario: {str(e)}")

    def list_all(self, limit: int = 100) -> List[Dict[str, Any]]:
        """List all usuarios with optional limit"""
        query = f"""
        SELECT * FROM {self.table_name}
        ORDER BY nome_completo
        LIMIT %s
        """

        result = self.db.execute_query(query, (limit,))
        if result:
            columns, rows = result
            return [dict(zip(columns, row)) for row in rows]
        return []

    def search(self, search_term: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search usuarios by name, email, or CPF"""
        query = f"""
        SELECT * FROM {self.table_name}
        WHERE nome_completo ILIKE %s
           OR email ILIKE %s
           OR cpf ILIKE %s
        ORDER BY nome_completo
        LIMIT %s
        """
        search_pattern = f"%{search_term}%"

        result = self.db.execute_query(
            query, (search_pattern, search_pattern, search_pattern, limit)
        )
        if result:
            columns, rows = result
            return [dict(zip(columns, row)) for row in rows]
        return []

    def authenticate(self, email: str, senha: str) -> Optional[Dict[str, Any]]:
        """Authenticate usuario by email and password"""
        query = f"""
        SELECT * FROM {self.table_name}
        WHERE email = %s AND senha = %s
        """

        result = self.db.execute_query(query, (email, senha))
        if result and result[1]:
            columns, rows = result
            return dict(zip(columns, rows[0]))
        return None

    def update_last_login(self, usuario_id: str) -> bool:
        """Update the last login timestamp"""
        query = f"""
        UPDATE {self.table_name}
        SET ultimo_login = CURRENT_TIMESTAMP
        WHERE id = %s
        """

        try:
            self.db.execute_query(query, (usuario_id,), fetch=False)
            return True
        except Exception as e:
            raise RuntimeError(f"Error updating last login: {str(e)}")
