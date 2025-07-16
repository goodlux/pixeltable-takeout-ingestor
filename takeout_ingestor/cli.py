"""
CLI interface for Pixeltable Takeout Ingestor
"""

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, TaskID
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = typer.Typer(help="Ingest personal data exports into Pixeltable")
console = Console()


@app.command()
def list_ingestors():
    """List available ingestors and their supported formats."""
    table = Table(title="Available Ingestors")
    table.add_column("Name", style="cyan")
    table.add_column("Formats", style="green")
    table.add_column("Description", style="white")
    
    # TODO: Dynamically discover ingestors
    table.add_row("Claude Conversations", ".txt, .json", "Claude conversation exports")
    table.add_row("Google Takeout", ".zip, directory", "Google Takeout archives")
    table.add_row("Artifacts", ".html, .js, .py", "Isolated artifacts")
    
    console.print(table)


@app.command()
def ingest(
    source: str = typer.Argument(..., help="Path to source data"),
    ingestor_type: str = typer.Option(None, "--type", "-t", help="Ingestor type (auto-detect if not specified)"),
    validate_only: bool = typer.Option(False, "--validate", help="Only validate, don't ingest"),
    batch_size: int = typer.Option(100, "--batch-size", help="Batch size for processing"),
    pixeltable_home: str = typer.Option(None, "--pixeltable-home", help="Override Pixeltable home directory")
):
    """Ingest data from a source into Pixeltable."""
    
    # Use environment variable if not provided
    if not pixeltable_home:
        pixeltable_home = os.getenv('PIXELTABLE_HOME')
    
    console.print(f"[cyan]Starting ingestion of:[/cyan] {source}")
    console.print(f"[dim]Pixeltable home:[/dim] {pixeltable_home or 'default'}")
    
    if validate_only:
        console.print("[yellow]Validation mode enabled[/yellow]")
    
    # TODO: Implement actual ingestion logic
    console.print("[red]Ingestion not yet implemented[/red]")


@app.command()
def setup():
    """Set up Pixeltable and verify installation."""
    console.print("[cyan]Setting up Pixeltable...[/cyan]")
    
    try:
        import pixeltable as pxt
        console.print("[green]✓ Pixeltable imported successfully[/green]")
        
        # Test basic functionality
        # TODO: Create a test table to verify everything works
        console.print("[green]✓ Setup completed[/green]")
        
    except ImportError:
        console.print("[red]✗ Pixeltable not installed[/red]")
        console.print("Install with: [cyan]pip install pixeltable[/cyan]")
    except Exception as e:
        console.print(f"[red]✗ Setup failed: {e}[/red]")


@app.command()
def status():
    """Show current ingestion status and statistics."""
    console.print("[cyan]Pixeltable Status[/cyan]")
    
    # TODO: Show actual statistics
    console.print("[dim]No active ingestions[/dim]")


def main():
    """Main entry point for CLI."""
    app()


if __name__ == "__main__":
    main()
