from database.core import DatabaseManager
from ui.menus import MenuSystem


def main():
    db_manager = DatabaseManager()
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
