from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.box import SIMPLE

console = Console()


def display_error(message: str) -> None:
    """Display an error message in a red panel"""
    console.print(
        Panel.fit(f"[bold]❌ Error:[/] {message}", style="red", border_style="red")
    )


def display_panel(message: str, style: str = "green") -> None:
    """Display a message in a styled panel"""
    console.print(Panel.fit(message, style=style, border_style=style))


def display_table(title: str, columns: list, data: list, expand: bool = False) -> None:
    """Display data in a rich table format"""
    table = Table(title=title, box=SIMPLE, header_style="bold magenta", expand=expand)

    for col in columns:
        table.add_column(col, style="cyan")

    for row in data:
        table.add_row(*[str(item) for item in row])

    console.print(table)
