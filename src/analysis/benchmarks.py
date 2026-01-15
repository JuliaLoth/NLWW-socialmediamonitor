"""
Benchmark berekeningen voor vergelijking tussen ambassade accounts.
"""
from dataclasses import dataclass
from typing import Optional
import logging

from ..database.connection import Database, get_connection
from ..database.queries import MetricsQueries, AccountQueries
from ..config.settings import COUNTRY_NAMES_NL

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    """Resultaat van een benchmark vergelijking."""
    account_id: str
    country: str
    platform: str
    metric: str
    value: float
    rank: int
    total_accounts: int
    percentile: float
    vs_average: float  # Percentage boven/onder gemiddelde


def calculate_benchmarks(
    year_month: str,
    metric: str = "avg_engagement_rate",
    db: Optional[Database] = None
) -> list[BenchmarkResult]:
    """
    Bereken benchmarks voor een specifieke metric.

    Args:
        year_month: Maand om te analyseren (YYYY-MM)
        metric: De metric om te vergelijken

    Returns:
        Lijst met BenchmarkResult objecten, gesorteerd op rank
    """
    db = db or get_connection()

    # Haal alle metrics op voor deze maand
    all_metrics = MetricsQueries.get_all_for_month(year_month, db)

    if not all_metrics:
        return []

    # Filter accounts die excluded zijn van benchmarks
    accounts = {a.id: a for a in AccountQueries.get_all(db)}

    # Haal waarden op voor de metric
    values = []
    for m in all_metrics:
        account = accounts.get(m.account_id)
        if not account:
            continue

        value = getattr(m, metric, None)
        if value is not None and value > 0:
            values.append({
                "account_id": m.account_id,
                "country": account.country,
                "platform": account.platform,
                "value": value,
            })

    if not values:
        return []

    # Sorteer op waarde (hoog naar laag)
    values.sort(key=lambda x: x["value"], reverse=True)

    # Bereken gemiddelde
    avg_value = sum(v["value"] for v in values) / len(values)

    # Genereer benchmark results
    results = []
    total = len(values)

    for rank, v in enumerate(values, 1):
        vs_avg = ((v["value"] - avg_value) / avg_value * 100) if avg_value > 0 else 0
        percentile = ((total - rank + 1) / total) * 100

        results.append(BenchmarkResult(
            account_id=v["account_id"],
            country=v["country"],
            platform=v["platform"],
            metric=metric,
            value=v["value"],
            rank=rank,
            total_accounts=total,
            percentile=round(percentile, 1),
            vs_average=round(vs_avg, 2),
        ))

    return results


def get_top_performers(
    year_month: str,
    n: int = 5,
    metric: str = "avg_engagement_rate",
    db: Optional[Database] = None
) -> list[dict]:
    """
    Haal top N performers op voor een metric.
    """
    benchmarks = calculate_benchmarks(year_month, metric, db)
    return [
        {
            "rank": b.rank,
            "country": COUNTRY_NAMES_NL.get(b.country, b.country),
            "platform": b.platform,
            "value": round(b.value, 4) if b.value < 1 else int(b.value),
            "vs_average": b.vs_average,
        }
        for b in benchmarks[:n]
    ]


def get_bottom_performers(
    year_month: str,
    n: int = 5,
    metric: str = "avg_engagement_rate",
    db: Optional[Database] = None
) -> list[dict]:
    """
    Haal bottom N performers op voor een metric.
    """
    benchmarks = calculate_benchmarks(year_month, metric, db)
    return [
        {
            "rank": b.rank,
            "country": COUNTRY_NAMES_NL.get(b.country, b.country),
            "platform": b.platform,
            "value": round(b.value, 4) if b.value < 1 else int(b.value),
            "vs_average": b.vs_average,
        }
        for b in benchmarks[-n:]
    ]


def get_platform_comparison(
    year_month: str,
    db: Optional[Database] = None
) -> dict:
    """
    Vergelijk prestaties per platform.
    """
    db = db or get_connection()

    all_metrics = MetricsQueries.get_all_for_month(year_month, db)
    accounts = {a.id: a for a in AccountQueries.get_all(db)}

    # Groepeer per platform
    by_platform = {}
    for m in all_metrics:
        account = accounts.get(m.account_id)
        if not account:
            continue

        platform = account.platform
        if platform not in by_platform:
            by_platform[platform] = {
                "engagement_rates": [],
                "followers": [],
                "posts": [],
                "accounts": 0,
            }

        by_platform[platform]["accounts"] += 1

        if m.avg_engagement_rate:
            by_platform[platform]["engagement_rates"].append(m.avg_engagement_rate)
        if m.avg_followers:
            by_platform[platform]["followers"].append(m.avg_followers)
        if m.total_posts:
            by_platform[platform]["posts"].append(m.total_posts)

    # Bereken gemiddelden
    result = {}
    for platform, data in by_platform.items():
        result[platform] = {
            "accounts": data["accounts"],
            "avg_engagement_rate": (
                sum(data["engagement_rates"]) / len(data["engagement_rates"])
                if data["engagement_rates"] else 0
            ),
            "avg_followers": (
                sum(data["followers"]) // len(data["followers"])
                if data["followers"] else 0
            ),
            "avg_posts": (
                sum(data["posts"]) / len(data["posts"])
                if data["posts"] else 0
            ),
            "total_followers": sum(data["followers"]),
            "total_posts": sum(data["posts"]),
        }

    return result


def get_regional_comparison(
    year_month: str,
    db: Optional[Database] = None
) -> dict:
    """
    Vergelijk prestaties per regio/land.
    """
    db = db or get_connection()

    all_metrics = MetricsQueries.get_all_for_month(year_month, db)
    accounts = {a.id: a for a in AccountQueries.get_all(db)}

    # Groepeer per land
    by_country = {}
    for m in all_metrics:
        account = accounts.get(m.account_id)
        if not account:
            continue

        country = account.country
        if country not in by_country:
            by_country[country] = {
                "engagement_rates": [],
                "followers": [],
                "posts": [],
                "platforms": set(),
            }

        by_country[country]["platforms"].add(account.platform)

        if m.avg_engagement_rate:
            by_country[country]["engagement_rates"].append(m.avg_engagement_rate)
        if m.avg_followers:
            by_country[country]["followers"].append(m.avg_followers)
        if m.total_posts:
            by_country[country]["posts"].append(m.total_posts)

    # Bereken gemiddelden
    result = {}
    for country, data in by_country.items():
        result[country] = {
            "display_name": COUNTRY_NAMES_NL.get(country, country),
            "platforms": list(data["platforms"]),
            "avg_engagement_rate": (
                sum(data["engagement_rates"]) / len(data["engagement_rates"])
                if data["engagement_rates"] else 0
            ),
            "total_followers": sum(data["followers"]),
            "total_posts": sum(data["posts"]),
        }

    # Sorteer op engagement rate
    result = dict(sorted(
        result.items(),
        key=lambda x: x[1]["avg_engagement_rate"],
        reverse=True
    ))

    return result


def get_account_ranking(
    account_id: str,
    year_month: str,
    db: Optional[Database] = None
) -> dict:
    """
    Haal ranking positie op voor een specifiek account.
    """
    benchmarks = calculate_benchmarks(year_month, "avg_engagement_rate", db)

    for b in benchmarks:
        if b.account_id == account_id:
            return {
                "rank": b.rank,
                "total": b.total_accounts,
                "percentile": b.percentile,
                "vs_average": b.vs_average,
                "engagement_rate": b.value,
            }

    return {}
