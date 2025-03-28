import sys
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm
from database.core import DatabaseManager
from config import DB_CONFIG

console = Console()


def search_usuario(identifier: str):
    """Search by ID, CPF, or email with full diagnostics"""
    db = DatabaseManager()

    try:
        # Connect to database
        console.print("\n[blue]1. Connecting to database...[/blue]")
        if not db.connect():
            console.print("[red]✖ Connection failed[/red]")
            return

        # Verify schema
        console.print(f"[blue]2. Current schema: {db.current_schema}[/blue]")
        if db.current_schema.lower() != "jec":
            if Confirm.ask(f"Schema is '{db.current_schema}'. Switch to 'jec'?"):
                db.set_schema("jec")

        # Check table exists
        console.print("[blue]3. Verifying table structure...[/blue]")
        if "usuarios" not in db.list_tables():
            console.print("[red]✖ 'usuarios' table missing[/red]")
            return

        # Determine search type
        if "-" in identifier:  # Likely UUID
            query = "SELECT * FROM usuarios WHERE id = %s::uuid"
            param = identifier
            search_type = "UUID"
        elif "@" in identifier:  # Email
            query = "SELECT * FROM usuarios WHERE email = %s"
            param = identifier
            search_type = "email"
        elif identifier.isdigit():  # CPF
            query = "SELECT * FROM usuarios WHERE cpf = %s"
            param = identifier
            search_type = "CPF"
        else:
            query = "SELECT * FROM usuarios WHERE nome_completo ILIKE %s"
            param = f"%{identifier}%"
            search_type = "name"

        # Execute search
        console.print(f"\n[blue]4. Searching by {search_type}: {identifier}[/blue]")
        result = db.execute_query(query, (param,))

        # Handle results
        if not result or not result[1]:
            console.print(f"[yellow]⚠ No usuario found by {search_type}[/yellow]")

            # Show all usuarios for debugging
            console.print("\n[blue]5. Listing all usuarios for reference:[/blue]")
            all_users = db.execute_query(
                "SELECT id, cpf, nome_completo, email FROM usuarios LIMIT 50"
            )
            if all_users and all_users[1]:
                table = Table(title="Existing Usuarios", box=None)
                table.add_column("ID", style="cyan")
                table.add_column("CPF", style="green")
                table.add_column("Name", style="magenta")
                table.add_column("Email", style="blue")
                for row in all_users[1]:
                    table.add_row(str(row[0]), row[1], row[2], row[3])
                console.print(table)
            else:
                console.print("[yellow]⚠ No usuarios exist in the table[/yellow]")
        else:
            # Display found usuario
            columns, rows = result
            usuario = dict(zip(columns, rows[0]))

            table = Table(title=f"Usuario Found", box=None)
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="green")

            for field, value in usuario.items():
                table.add_row(field, str(value))

            console.print(table)

    except Exception as e:
        console.print(f"[red]✖ Error: {str(e)}[/red]", style="bold")
    finally:
        if db.connected:
            db.disconnect()
            console.print("[blue]Database connection closed[/blue]")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        console.print("[red]Usage: python find_usuario.py <id/cpf/email>[/red]")
        sys.exit(1)

    identifier = sys.argv[1]
    console.print(f"\n[bold]Searching for:[/bold] {identifier}")
    search_usuario(identifier)
