#!/usr/bin/env python3
"""
NL Embassy Social Media Monitor - CLI

Gebruik:
    python main.py --help              Toon help
    python main.py init                Initialiseer database
    python main.py collect             Verzamel data voor alle accounts
    python main.py collect --country turkije  Verzamel data voor specifiek land
    python main.py backfill            Verzamel 12 maanden historie
    python main.py analyze             Bereken metrics en benchmarks
    python main.py report              Genereer rapporten
    python main.py dashboard           Start Streamlit dashboard
    python main.py status              Toon systeem status
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import click
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn
except ImportError:
    print("Installeer dependencies met: pip install -r requirements.txt")
    sys.exit(1)

# Fix Windows console encoding
import os
if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from src.database.connection import get_connection
from src.database.models import create_schema
from src.database.queries import AccountQueries, MetricsQueries
from src.agents.job_queue import JobQueue, JobType
from src.agents.data_agent import DataAgent, load_accounts_from_yaml
from src.agents.analyse_agent import AnalyseAgent
from src.agents.orchestrator import Orchestrator
from src.config.settings import COUNTRY_NAMES_NL, PLATFORM_NAMES_NL

console = Console(force_terminal=True)


@click.group()
@click.version_option(version="0.1.0", prog_name="nl-embassy-monitor")
def cli():
    """NL Embassy Social Media Monitor - CLI tool."""
    pass


@cli.command()
def init():
    """Initialiseer database en laad account configuratie."""
    console.print("[bold blue]Initialisatie gestart...[/bold blue]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Database schema aanmaken...", total=None)
        create_schema()

        progress.update(task, description="Account configuratie laden...")
        asyncio.run(load_accounts_from_yaml())

        progress.update(task, description="Klaar!")

    # Show summary
    db = get_connection()
    accounts = AccountQueries.get_all(db)
    platform_counts = AccountQueries.count_by_platform(db)

    console.print(f"\n[green]Initialisatie voltooid![/green]")
    console.print(f"Totaal accounts: [bold]{len(accounts)}[/bold]")

    for platform, count in platform_counts.items():
        console.print(f"  - {PLATFORM_NAMES_NL.get(platform, platform)}: {count}")


@cli.command()
@click.option("--country", "-c", help="Specifiek land om te verzamelen")
@click.option("--platform", "-p", help="Specifiek platform (instagram/facebook/twitter)")
def collect(country, platform):
    """Verzamel nieuwe data voor accounts."""
    console.print("[bold blue]Data collectie gestart...[/bold blue]")

    async def run_collection():
        orchestrator = Orchestrator()
        await orchestrator.initialize()

        db = get_connection()
        accounts = AccountQueries.get_all(db)

        if country:
            accounts = [a for a in accounts if a.country == country]
        if platform:
            accounts = [a for a in accounts if a.platform == platform]

        if not accounts:
            console.print("[yellow]Geen accounts gevonden met opgegeven filters[/yellow]")
            return

        console.print(f"Verzamelen voor {len(accounts)} accounts...")

        # Start agents
        await orchestrator.start_agents()

        # Queue collection jobs
        for account in accounts:
            await orchestrator.job_queue.enqueue(
                JobType.COLLECT_ACCOUNT,
                {"account_id": account.id},
                priority=5
            )

        # Wait for completion
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Verzamelen...", total=None)

            while True:
                pending = await orchestrator.job_queue.get_pending_count([JobType.COLLECT_ACCOUNT])
                running = await orchestrator.job_queue.get_running_count()

                if pending == 0 and running == 0:
                    break

                progress.update(task, description=f"Verzamelen... ({pending} wachtend, {running} actief)")
                await asyncio.sleep(2)

        await orchestrator.cleanup()
        console.print("[green]Collectie voltooid![/green]")

    asyncio.run(run_collection())


@cli.command()
@click.option("--country", "-c", help="Specifiek land")
@click.option("--months", "-m", default=12, help="Aantal maanden terug")
def backfill(country, months):
    """Verzamel historische data (12 maanden)."""
    console.print(f"[bold blue]Historische backfill gestart ({months} maanden)...[/bold blue]")

    async def run_backfill():
        orchestrator = Orchestrator()
        await orchestrator.initialize()
        await orchestrator.start_agents()

        result = await orchestrator.run_historical_backfill(country)
        console.print(f"Jobs aangemaakt voor {result['accounts_queued']} accounts")

        # Wait with progress
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Backfill...", total=None)

            while True:
                pending = await orchestrator.job_queue.get_pending_count([JobType.COLLECT_HISTORICAL])
                running = await orchestrator.job_queue.get_running_count()

                if pending == 0 and running == 0:
                    break

                progress.update(task, description=f"Backfill... ({pending} wachtend)")
                await asyncio.sleep(5)

        await orchestrator.cleanup()
        console.print("[green]Backfill voltooid![/green]")

    asyncio.run(run_backfill())


@cli.command()
@click.option("--month", "-m", help="Specifieke maand (YYYY-MM)")
def analyze(month):
    """Bereken metrics en benchmarks."""
    if not month:
        now = datetime.now()
        month = f"{now.year:04d}-{now.month:02d}"

    console.print(f"[bold blue]Analyse voor {month}...[/bold blue]")

    async def run_analysis():
        orchestrator = Orchestrator()
        await orchestrator.initialize()
        await orchestrator.start_agents()

        # Queue analysis jobs
        await orchestrator.job_queue.enqueue(
            JobType.CALCULATE_MONTHLY,
            {"year_month": month},
            priority=3
        )

        await orchestrator.job_queue.enqueue(
            JobType.CALCULATE_BENCHMARKS,
            {"year_month": month},
            priority=4
        )

        await orchestrator.job_queue.enqueue(
            JobType.DETECT_ANOMALIES,
            {"year_month": month},
            priority=5
        )

        # Wait
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Analyseren...", total=None)
            await orchestrator.job_queue.wait_for_completion([
                JobType.CALCULATE_MONTHLY,
                JobType.CALCULATE_BENCHMARKS,
                JobType.DETECT_ANOMALIES
            ])

        await orchestrator.cleanup()
        console.print("[green]Analyse voltooid![/green]")

    asyncio.run(run_analysis())


@cli.command()
@click.option("--type", "-t", "report_type", default="monthly", type=click.Choice(["monthly", "yearly"]))
@click.option("--month", "-m", help="Maand voor monthly rapport (YYYY-MM)")
@click.option("--year", "-y", type=int, help="Jaar voor yearly rapport")
def report(report_type, month, year):
    """Genereer PDF en Excel rapporten."""
    console.print(f"[bold blue]Rapporten genereren ({report_type})...[/bold blue]")

    async def run_reports():
        orchestrator = Orchestrator()
        await orchestrator.initialize()
        await orchestrator.start_agents()

        await orchestrator.generate_reports(report_type, month, year)

        await orchestrator.cleanup()
        console.print("[green]Rapporten gegenereerd![/green]")
        console.print(f"Bekijk exports in: data/exports/")

    asyncio.run(run_reports())


@cli.command()
def dashboard():
    """Start Streamlit dashboard."""
    import subprocess

    console.print("[bold blue]Dashboard starten...[/bold blue]")
    console.print("Open in browser: http://localhost:8501")

    dashboard_path = PROJECT_ROOT / "src" / "outputs" / "dashboard" / "app.py"
    subprocess.run(["streamlit", "run", str(dashboard_path)])


@cli.command()
def status():
    """Toon systeem status."""
    db = get_connection()

    # Accounts
    accounts = AccountQueries.get_all(db)
    platform_counts = AccountQueries.count_by_platform(db)

    console.print("\n[bold]Account Status[/bold]")
    table = Table()
    table.add_column("Platform")
    table.add_column("Accounts", justify="right")

    for platform, count in platform_counts.items():
        table.add_row(PLATFORM_NAMES_NL.get(platform, platform), str(count))

    table.add_row("[bold]Totaal[/bold]", f"[bold]{len(accounts)}[/bold]")
    console.print(table)

    # Recent metrics
    now = datetime.now()
    current_month = f"{now.year:04d}-{now.month:02d}"
    metrics = MetricsQueries.get_all_for_month(current_month, db)

    console.print(f"\n[bold]Metrics ({current_month})[/bold]")

    if metrics:
        total_followers = sum(m.avg_followers or 0 for m in metrics)
        total_posts = sum(m.total_posts for m in metrics)
        avg_engagement = sum(m.avg_engagement_rate or 0 for m in metrics) / len(metrics)

        metrics_table = Table()
        metrics_table.add_column("Metric")
        metrics_table.add_column("Waarde", justify="right")

        metrics_table.add_row("Accounts met data", str(len(metrics)))
        metrics_table.add_row("Totaal volgers", f"{total_followers:,}")
        metrics_table.add_row("Posts deze maand", str(total_posts))
        metrics_table.add_row("Gem. engagement", f"{avg_engagement:.4f}%")

        console.print(metrics_table)
    else:
        console.print("[yellow]Nog geen metrics beschikbaar. Voer 'analyze' uit.[/yellow]")

    # Job queue
    job_queue = JobQueue()
    job_status = asyncio.run(job_queue.get_status_summary())

    if job_status:
        console.print("\n[bold]Job Queue[/bold]")
        queue_table = Table()
        queue_table.add_column("Status")
        queue_table.add_column("Aantal", justify="right")

        for status, count in job_status.items():
            queue_table.add_row(status, str(count))

        console.print(queue_table)


@cli.command()
def accounts():
    """Toon alle geconfigureerde accounts."""
    db = get_connection()
    accounts = AccountQueries.get_all(db)

    table = Table(title="Geconfigureerde Accounts")
    table.add_column("Land")
    table.add_column("Platform")
    table.add_column("Handle")
    table.add_column("Status")

    for account in sorted(accounts, key=lambda a: (a.country, a.platform)):
        table.add_row(
            COUNTRY_NAMES_NL.get(account.country, account.country),
            PLATFORM_NAMES_NL.get(account.platform, account.platform),
            f"@{account.handle}",
            account.status
        )

    console.print(table)


@cli.command()
@click.option("--country", "-c", help="Specifiek land")
@click.option("--limit", "-l", default=100, help="Max aantal posts")
def communicate(country, limit):
    """Analyseer communicatiestijl van posts."""
    console.print("[bold blue]Communicatie analyse gestart...[/bold blue]")

    from src.analysis.communication import (
        get_posts_for_classification, classify_posts_batch,
        calculate_account_comm_profile, get_classification_summary
    )

    db = get_connection()

    # Bepaal welke accounts
    if country:
        accounts = [a for a in AccountQueries.get_all(db) if a.country == country]
    else:
        accounts = AccountQueries.get_all(db)

    if not accounts:
        console.print("[yellow]Geen accounts gevonden[/yellow]")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Classificeren...", total=None)

        total_classified = 0
        for account in accounts:
            posts = get_posts_for_classification(account.id, limit, db)
            if posts:
                progress.update(task, description=f"Classificeren {account.handle}...")
                classify_posts_batch(posts, db)
                total_classified += len(posts)

                # Update profiel
                calculate_account_comm_profile(account.id, db)

    # Toon samenvatting
    summary = get_classification_summary(db)

    console.print(f"\n[green]Analyse voltooid![/green]")
    console.print(f"Totaal geclassificeerd: {summary['total_classified']} posts")

    if summary['by_content_type']:
        console.print("\n[bold]Content types:[/bold]")
        for content_type, count in summary['by_content_type'].items():
            pct = count / summary['total_classified'] * 100 if summary['total_classified'] > 0 else 0
            console.print(f"  - {content_type}: {count} ({pct:.1f}%)")

    console.print(f"\n[bold]Gemiddelde scores:[/bold]")
    console.print(f"  - Formaliteit: {summary['avg_formality']}")
    console.print(f"  - Met call-to-action: {summary['pct_with_cta']}%")
    console.print(f"  - Met link: {summary['pct_with_link']}%")
    console.print(f"  - Volledigheid: {summary['avg_completeness']}")


@cli.command()
@click.option("--country", "-c", help="Specifiek land")
def comm_profile(country):
    """Toon communicatieprofiel per account."""
    db = get_connection()

    # Haal profielen op
    query = """
        SELECT
            a.country, a.platform, a.handle,
            p.total_posts_analyzed, p.dominant_tone, p.avg_formality_score,
            p.pct_procedural, p.pct_promotional, p.pct_with_cta,
            p.pct_with_link, p.avg_completeness
        FROM account_comm_profile p
        JOIN accounts a ON p.account_id = a.id
    """
    params = []

    if country:
        query += " WHERE a.country = ?"
        params.append(country)

    query += " ORDER BY a.country, a.platform"

    rows = db.fetchall(query, params) if params else db.fetchall(query)

    if not rows:
        console.print("[yellow]Geen communicatieprofielen gevonden. Voer eerst 'communicate' uit.[/yellow]")
        return

    table = Table(title="Communicatieprofielen")
    table.add_column("Land")
    table.add_column("Platform")
    table.add_column("Handle")
    table.add_column("Posts", justify="right")
    table.add_column("Toon")
    table.add_column("Formal", justify="right")
    table.add_column("Proced%", justify="right")
    table.add_column("Promo%", justify="right")
    table.add_column("CTA%", justify="right")
    table.add_column("Compl", justify="right")

    for row in rows:
        table.add_row(
            COUNTRY_NAMES_NL.get(row[0], row[0]),
            PLATFORM_NAMES_NL.get(row[1], row[1]),
            f"@{row[2]}",
            str(row[3] or 0),
            row[4] or "-",
            f"{row[5]:.2f}" if row[5] else "-",
            f"{row[6]:.1f}" if row[6] else "-",
            f"{row[7]:.1f}" if row[7] else "-",
            f"{row[8]:.1f}" if row[8] else "-",
            f"{row[10]:.2f}" if row[10] else "-",
        )

    console.print(table)


if __name__ == "__main__":
    cli()
