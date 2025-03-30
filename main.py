from database.core import DatabaseManager
from ui.menus import MenuSystem
from config import DB_CONFIG
from rich.console import Console

console = Console()


def main():
    # Initialize with default config from config.py
    db_manager = DatabaseManager()

    # Show connection status at startup
    console = Console()
    console.print("\n[bold]Using DB config:[/]")
    console.print(f"Host: {DB_CONFIG['host']}")
    console.print(f"Database: {DB_CONFIG['database']}")

    menu_system = MenuSystem(db_manager)

    try:
        menu_system.main_menu()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    finally:
        if db_manager.connected:
            db_manager.disconnect()


if __name__ == "__main__":
    main()
