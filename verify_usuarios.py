# verify_usuarios.py
from rich.console import Console
from database.core import DatabaseManager
from config import DB_CONFIG

console = Console()


def verify_database_state():
    """Comprehensive verification of database state"""
    db = DatabaseManager()

    try:
        # 1. Verify connection
        console.print("\n[blue]1. Testing connection...[/blue]")
        if not db.connect():
            console.print("[red]✖ Connection failed[/red]")
            return
        console.print("[green]✓ Connection successful[/green]")

        # 2. Verify schema
        console.print(f"\n[blue]2. Current schema: {db.current_schema}[/blue]")

        # 3. Verify table exists
        console.print("\n[blue]3. Verifying table existence...[/blue]")
        tables = db.list_tables()
        if "usuarios" not in tables:
            console.print("[red]✖ 'usuarios' table missing[/red]")
            console.print(f"Existing tables: {', '.join(tables)}")
            return
        console.print("[green]✓ 'usuarios' table exists[/green]")

        # 4. Check transaction isolation level
        console.print("\n[blue]4. Checking transaction settings...[/blue]")
        isolation = db.execute_query("SHOW transaction_isolation")[1][0][0]
        console.print(f"Transaction isolation level: {isolation}")

        # 5. Count records with explicit commit
        console.print("\n[blue]5. Counting records...[/blue]")
        count = db.execute_query(
            "SELECT COUNT(*) FROM usuarios", fetch=True, autocommit=True
        )
        console.print(f"Total usuarios: {count[1][0][0]}")

        # 6. Show sample records
        console.print("\n[blue]6. Sample records:[/blue]")
        sample = db.execute_query(
            "SELECT * FROM usuarios LIMIT 5", fetch=True, autocommit=True
        )
        if sample and sample[1]:
            for row in sample[1]:
                console.print(f" - {row}")
        else:
            console.print("[yellow]No records found[/yellow]")

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
    finally:
        if db.connected:
            db.disconnect()


if __name__ == "__main__":
    verify_database_state()
