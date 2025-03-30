from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from database.core import DatabaseManager
from models.usuario import Usuario
from pathlib import Path  # Add this with other imports

from datetime import datetime  # Add with other imports

from ui.console import display_table, display_panel, display_error

console = Console()


class MenuSystem:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.usuario_model = Usuario(db_manager)
        self.menu_actions = {
            "1": ("Connect to Database", self._connect_db),
            "2": ("List Tables", self._list_tables),
            "3": ("Describe Table", self._describe_table),
            "4": ("Preview Table", self._preview_table),
            "5": ("Run Custom Query", self._run_custom_query),
            "6": ("Export Table", self._export_table),
            "7": ("Change Schema", self._change_schema),
            "8": ("Database Info", self._show_db_info),
            "9": ("Exit", self._exit_app),
        }

    def _usuario_menu(self):
        """Submenu for usuario operations"""
        while True:
            console.print("\n[bold cyan]Usuario Management[/]")
            options = [
                ("Create Usuario", self._create_usuario),
                ("List Usuarios", self._list_usuarios),
                ("View Usuario", self._view_usuario),
                ("Update Usuario", self._update_usuario),
                ("Delete Usuario", self._delete_usuario),
                ("Back to Main Menu", lambda: None),
            ]

            for i, (option, _) in enumerate(options, 1):
                console.print(f"{i}. {option}")

            choice = Prompt.ask(
                "Select an option", choices=[str(i) for i in range(1, len(options) + 1)]
            )
            _, action = options[int(choice) - 1]
            if action() is None:  # Back to main menu
                break

    def _create_usuario(self):
        """Handle usuario creation with proper tipo validation"""
        try:
            data = {
                "cpf": Prompt.ask("Enter CPF (11 digits only)").strip(),
                "nome_completo": Prompt.ask("Enter full name").strip(),
                "email": Prompt.ask("Enter email").strip(),
                "senha": Prompt.ask("Enter password", password=True).strip(),
                "tipo": Prompt.ask(
                    "Type:",
                    choices=["servidor", "juiz", "advogado", "parte"],
                    default="parte",
                ),
            }

            # Phone is optional
            telefone = Prompt.ask(
                "Enter phone (optional - press Enter to skip)"
            ).strip()
            if telefone:
                data["telefone"] = telefone

            # Validate CPF format
            if not (data["cpf"].isdigit() and len(data["cpf"]) == 11):
                raise ValueError("CPF must be exactly 11 digits")

            # Validate email contains @
            if "@" not in data["email"]:
                raise ValueError("Email must contain @ symbol")

            # Validate password length
            if len(data["senha"]) < 6:
                raise ValueError("Password must be at least 6 characters")

            usuario_id = self.usuario_model.create(data)
            display_panel(f"✅ Usuario created with ID: {usuario_id}", "green")

        except ValueError as e:
            display_error(f"Validation error: {str(e)}")
        except Exception as e:
            display_error(f"❌ Failed to create usuario: {str(e)}")

    def _list_usuarios(self) -> None:
        """List all usuarios with correct schema fields"""
        try:
            limit = int(Prompt.ask("Number of usuarios to show", default="10"))
            usuarios = self.usuario_model.list_all(limit)

            if not usuarios:
                display_panel("No usuarios found", "yellow")
                return

            # Prepare table data with correct fields
            table_data = []
            for usuario in usuarios:
                table_data.append(
                    [
                        str(usuario.get("id", "")),
                        usuario.get("cpf", ""),
                        usuario.get("nome_completo", ""),
                        usuario.get("email", ""),
                        usuario.get("tipo", ""),
                        usuario.get("telefone", "N/A"),
                    ]
                )

            display_table(
                title=f"Usuarios (showing {len(usuarios)} of {limit})",
                columns=["ID", "CPF", "Full Name", "Email", "Type", "Phone"],
                data=table_data,
            )
        except Exception as e:
            display_error(f"❌ Failed to list usuarios: {str(e)}")

    def _view_usuario(self) -> None:
        """View detailed usuario information with correct schema fields"""
        try:
            usuario_id = Prompt.ask("Enter usuario ID")
            usuario = self.usuario_model.get_by_id(usuario_id)

            if not usuario:
                display_panel("Usuario not found", "yellow")
                return

            # Create detailed view with all fields
            info_panel = Panel.fit(
                f"""\
    [bold]ID:[/] {usuario.get('id', 'N/A')}
    [bold]CPF:[/] {usuario.get('cpf', 'N/A')}
    [bold]Full Name:[/] {usuario.get('nome_completo', 'N/A')}
    [bold]Email:[/] {usuario.get('email', 'N/A')}
    [bold]Type:[/] {usuario.get('tipo', 'N/A')}
    [bold]Phone:[/] {usuario.get('telefone', 'N/A')}
    [bold]Registration Date:[/] {usuario.get('data_cadastro', 'N/A')}
    [bold]Last Login:[/] {usuario.get('ultimo_login', 'N/A')}""",
                title="Usuario Details",
                border_style="blue",
            )
            console.print(info_panel)
        except Exception as e:
            display_error(f"❌ Failed to view usuario: {str(e)}")

    def _update_usuario(self) -> None:
        """Update an existing usuario with correct schema fields"""
        try:
            usuario_id = Prompt.ask("Enter usuario ID to update")

            # Get current data
            current = self.usuario_model.get_by_id(usuario_id)
            if not current:
                display_panel("Usuario not found", "yellow")
                return

            # Create editable fields
            update_data = {
                "cpf": Prompt.ask(
                    f"CPF [{current.get('cpf', '')}]", default=current.get("cpf", "")
                ),
                "nome_completo": Prompt.ask(
                    f"Full name [{current.get('nome_completo', '')}]",
                    default=current.get("nome_completo", ""),
                ),
                "email": Prompt.ask(
                    f"Email [{current.get('email', '')}]",
                    default=current.get("email", ""),
                ),
                "tipo": Prompt.ask(
                    f"Type:[{current.get('tipo', '')}]",
                    choices=["servidor", "juiz", "advogado", "parte"],
                    default=current.get("tipo", "parte"),
                ),
                "telefone": Prompt.ask(
                    f"Phone [{current.get('telefone', '')}]",
                    default=current.get("telefone", ""),
                ),
            }

            # Remove unchanged fields
            update_data = {
                k: v
                for k, v in update_data.items()
                if str(v) != str(current.get(k, ""))
            }

            if not update_data:
                display_panel("No changes detected", "yellow")
                return

            if self.usuario_model.update(usuario_id, update_data):
                display_panel("✅ Usuario updated successfully", "green")
            else:
                display_panel("❌ Failed to update usuario", "red")
        except Exception as e:
            display_error(f"❌ Failed to update usuario: {str(e)}")

    def _delete_usuario(self) -> None:
        """Delete a usuario with confirmation"""
        try:
            usuario_id = Prompt.ask("Enter usuario ID to delete")

            # Verify existence
            usuario = self.usuario_model.get_by_id(usuario_id)
            if not usuario:
                display_panel("Usuario not found", "yellow")
                return

            # Show confirmation with details
            console.print("\n[bold]Usuario to delete:[/]")
            console.print(f"ID: {usuario.get('id', '')}")
            console.print(f"Name: {usuario.get('nome_completo', '')}")
            console.print(f"Email: {usuario.get('email', '')}")
            console.print(f"Type: {usuario.get('tipo', '')}")

            if Confirm.ask(
                "\n[bold red]Are you sure you want to delete this usuario?",
                default=False,
            ):
                if self.usuario_model.delete(usuario_id):
                    display_panel("✅ Usuario deleted successfully", "green")
                else:
                    display_panel("❌ Failed to delete usuario", "red")
            else:
                display_panel("Deletion cancelled", "blue")
        except Exception as e:
            display_error(f"❌ Failed to delete usuario: {str(e)}")

    def main_menu(self) -> None:
        """Main interactive menu loop"""
        while True:
            console.clear()
            console.print(
                Panel.fit("[bold cyan]PostgreSQL Database Manager[/]", style="blue")
            )

            # Updated menu actions with Usuario management
            self.menu_actions = {
                "1": ("Connect to Database", self._connect_db),
                "2": ("List Tables", self._list_tables),
                "3": ("Describe Table", self._describe_table),
                "4": ("Preview Table", self._preview_table),
                "5": ("Run Custom Query", self._run_custom_query),
                "6": ("Export Table", self._export_table),
                "7": ("Change Schema", self._change_schema),
                "8": ("Database Info", self._show_db_info),
                "9": ("Usuario Management", self._usuario_menu),  # New option
                "10": ("Exit", self._exit_app),
            }

            # Display menu options
            for key, (option, _) in self.menu_actions.items():
                console.print(f"{key}. {option}")

            # Get user choice
            choice = Prompt.ask(
                "\nSelect an option", choices=list(self.menu_actions.keys())
            )

            # Handle the choice
            self._handle_choice(choice)

    def _handle_choice(self, choice: str) -> None:
        """Execute the selected menu action"""
        try:
            _, action = self.menu_actions[choice]
            action()
        except Exception as e:
            display_error(f"Operation failed: {str(e)}")
        Prompt.ask("\nPress Enter to continue...")

    def _connect_db(self):
        """Handle database connection without config passing"""
        if self.db.connected:
            if not Confirm.ask("Already connected. Reconnect?"):
                return

        try:
            # Just show connection attempt status
            if self.db.connect():  # No config passed
                display_panel("✅ Connected using default configuration", "green")
            else:
                display_error("Connection failed")
        except Exception as e:
            display_error(f"Connection error: {str(e)}")

    def _list_tables(self) -> None:
        """List tables in current schema"""
        self._require_connection()
        tables = self.db.list_tables()
        display_table(
            title=f"Tables in schema '{self.db.current_schema}'",
            columns=["Table Name"],
            data=[[table] for table in tables],
        )

    def _describe_table(self) -> None:
        """Show table structure"""
        self._require_connection()
        table_name = Prompt.ask("Enter table name")

        try:
            columns = ["Column Name", "Data Type", "Nullable", "Default Value"]
            data = self.db.describe_table(table_name)
            display_table(
                title=f"Structure of '{table_name}'", columns=columns, data=data
            )
        except Exception as e:
            display_error(f"Couldn't describe table: {str(e)}")

    def _preview_table(self) -> None:
        """Preview table data"""
        self._require_connection()
        table_name = Prompt.ask("Enter table name")
        limit = Prompt.ask("Row limit", default="5")

        try:
            result = self.db.preview_table(table_name, int(limit))
            if result:
                columns, data = result
                display_table(
                    title=f"First {limit} rows from '{table_name}'",
                    columns=columns,
                    data=data,
                )
        except Exception as e:
            display_error(f"Couldn't preview table: {str(e)}")

    def _run_custom_query(self) -> None:
        """Execute custom SQL query"""
        self._require_connection()
        console.print("Enter SQL query (end with empty line):")
        query_lines = []
        while True:
            line = Prompt.ask("> ")
            if not line:
                break
            query_lines.append(line)
        query = "\n".join(query_lines)

        try:
            result = self.db.execute_query(query)
            if result:
                columns, data = result
                display_table(title="Query Results", columns=columns, data=data)
            else:
                display_panel("Query executed successfully", "green")
        except Exception as e:
            display_error(f"Query failed: {str(e)}")

    def _export_table(self) -> None:
        """Export table to CSV with proper datetime handling"""
        self._require_connection()

        try:
            table_name = Prompt.ask("Enter table name to export")

            # Validate table exists
            if table_name not in self.db.list_tables():
                display_error(f"Table '{table_name}' doesn't exist")
                return

            # Create default export directory if it doesn't exist
            export_dir = Path("exports")
            export_dir.mkdir(exist_ok=True)

            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_file = export_dir / f"{table_name}_{timestamp}.csv"

            output_file = Prompt.ask(
                "Enter output file path", default=str(default_file)
            )

            # Ensure .csv extension
            if not output_file.lower().endswith(".csv"):
                output_file += ".csv"

            # Show confirmation
            console.print("\n[bold]Export Details:[/]")
            console.print(f"Table: {table_name}")
            console.print(f"Output File: {output_file}")

            if Confirm.ask("\nProceed with export?", default=True):
                self.db.export_table(table_name, output_file)
                display_panel(f"✅ Successfully exported to {output_file}", "green")

        except Exception as e:
            display_error(f"Export failed: {str(e)}")

    def _change_schema(self) -> None:
        """Change current schema"""
        self._require_connection()
        schema = Prompt.ask("Enter schema name", default=self.db.current_schema)

        try:
            self.db.set_schema(schema)
            display_panel(f"Schema changed to '{schema}'", "blue")
        except Exception as e:
            display_error(f"Couldn't change schema: {str(e)}")

    def _show_db_info(self) -> None:
        """Display database information"""
        self._require_connection()

        try:
            info = self.db.get_database_info()
            display_table(
                title="Database Information",
                columns=["Property", "Value"],
                data=[[k.replace("_", " ").title(), str(v)] for k, v in info.items()],
            )
        except Exception as e:
            display_error(f"Couldn't get database info: {str(e)}")

    def _exit_app(self) -> None:
        """Clean exit from application"""
        if self.db.connected:
            self.db.disconnect()
        raise SystemExit("Goodbye!")

    def _require_connection(self) -> None:
        """Check if connected to database"""
        if not self.db.connected:
            display_error("Not connected to database")
            if Confirm.ask("Would you like to connect now?"):
                self._connect_db()
            else:
                raise RuntimeError("Operation requires database connection")
