"""
Trend analyse voor social media prestaties.
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional
import logging

from ..database.connection import Database, get_connection
from ..database.queries import MetricsQueries, AccountQueries

logger = logging.getLogger(__name__)


class TrendDirection(str, Enum):
    """Richting van een trend."""
    STRONG_UP = "strong_up"       # > 20% groei
    UP = "up"                     # 5-20% groei
    STABLE = "stable"             # -5% tot +5%
    DOWN = "down"                 # -5% tot -20%
    STRONG_DOWN = "strong_down"  # < -20%


@dataclass
class TrendResult:
    """Resultaat van trend analyse."""
    account_id: str
    metric: str
    direction: TrendDirection
    change_pct: float
    current_value: float
    previous_value: float
    period: str


def determine_trend_direction(change_pct: float) -> TrendDirection:
    """Bepaal trend richting op basis van percentage verandering."""
    if change_pct > 20:
        return TrendDirection.STRONG_UP
    elif change_pct > 5:
        return TrendDirection.UP
    elif change_pct >= -5:
        return TrendDirection.STABLE
    elif change_pct >= -20:
        return TrendDirection.DOWN
    else:
        return TrendDirection.STRONG_DOWN


def analyze_trends(
    account_id: str,
    current_month: str,
    previous_month: str,
    db: Optional[Database] = None
) -> list[TrendResult]:
    """
    Analyseer trends voor een account door twee maanden te vergelijken.

    Args:
        account_id: Account ID
        current_month: Huidige maand (YYYY-MM)
        previous_month: Vorige maand (YYYY-MM)

    Returns:
        Lijst met TrendResult objecten voor elke metric
    """
    db = db or get_connection()

    # Haal metrics op
    current = MetricsQueries.get_by_account(account_id, current_month, current_month, db)
    previous = MetricsQueries.get_by_account(account_id, previous_month, previous_month, db)

    if not current or not previous:
        return []

    current_metrics = current[0]
    previous_metrics = previous[0]

    results = []
    period = f"{previous_month} → {current_month}"

    # Engagement rate trend
    if previous_metrics.avg_engagement_rate and previous_metrics.avg_engagement_rate > 0:
        change = ((current_metrics.avg_engagement_rate or 0) - previous_metrics.avg_engagement_rate)
        change_pct = (change / previous_metrics.avg_engagement_rate) * 100

        results.append(TrendResult(
            account_id=account_id,
            metric="engagement_rate",
            direction=determine_trend_direction(change_pct),
            change_pct=round(change_pct, 2),
            current_value=current_metrics.avg_engagement_rate or 0,
            previous_value=previous_metrics.avg_engagement_rate,
            period=period,
        ))

    # Follower trend
    if previous_metrics.avg_followers and previous_metrics.avg_followers > 0:
        change = (current_metrics.avg_followers or 0) - previous_metrics.avg_followers
        change_pct = (change / previous_metrics.avg_followers) * 100

        results.append(TrendResult(
            account_id=account_id,
            metric="followers",
            direction=determine_trend_direction(change_pct),
            change_pct=round(change_pct, 2),
            current_value=current_metrics.avg_followers or 0,
            previous_value=previous_metrics.avg_followers,
            period=period,
        ))

    # Posts trend
    if previous_metrics.total_posts > 0:
        change = current_metrics.total_posts - previous_metrics.total_posts
        change_pct = (change / previous_metrics.total_posts) * 100

        results.append(TrendResult(
            account_id=account_id,
            metric="posts",
            direction=determine_trend_direction(change_pct),
            change_pct=round(change_pct, 2),
            current_value=current_metrics.total_posts,
            previous_value=previous_metrics.total_posts,
            period=period,
        ))

    # Likes trend
    if previous_metrics.total_likes > 0:
        change = current_metrics.total_likes - previous_metrics.total_likes
        change_pct = (change / previous_metrics.total_likes) * 100

        results.append(TrendResult(
            account_id=account_id,
            metric="likes",
            direction=determine_trend_direction(change_pct),
            change_pct=round(change_pct, 2),
            current_value=current_metrics.total_likes,
            previous_value=previous_metrics.total_likes,
            period=period,
        ))

    return results


def get_trend_summary(
    year_month: str,
    db: Optional[Database] = None
) -> dict:
    """
    Genereer trend samenvatting voor alle accounts voor een maand.

    Returns dict met:
    - growing: accounts met positieve trends
    - declining: accounts met negatieve trends
    - stable: accounts met stabiele prestaties
    """
    db = db or get_connection()

    # Bepaal vorige maand
    year, month = map(int, year_month.split("-"))
    if month == 1:
        prev_year, prev_month = year - 1, 12
    else:
        prev_year, prev_month = year, month - 1
    previous_month = f"{prev_year:04d}-{prev_month:02d}"

    accounts = AccountQueries.get_all(db)

    growing = []
    declining = []
    stable = []

    for account in accounts:
        trends = analyze_trends(account.id, year_month, previous_month, db)

        if not trends:
            continue

        # Kijk naar engagement rate trend
        eng_trend = next((t for t in trends if t.metric == "engagement_rate"), None)

        if eng_trend:
            if eng_trend.direction in (TrendDirection.STRONG_UP, TrendDirection.UP):
                growing.append({
                    "account_id": account.id,
                    "country": account.country,
                    "platform": account.platform,
                    "change_pct": eng_trend.change_pct,
                    "direction": eng_trend.direction.value,
                })
            elif eng_trend.direction in (TrendDirection.STRONG_DOWN, TrendDirection.DOWN):
                declining.append({
                    "account_id": account.id,
                    "country": account.country,
                    "platform": account.platform,
                    "change_pct": eng_trend.change_pct,
                    "direction": eng_trend.direction.value,
                })
            else:
                stable.append({
                    "account_id": account.id,
                    "country": account.country,
                    "platform": account.platform,
                    "change_pct": eng_trend.change_pct,
                })

    # Sorteer op change_pct
    growing.sort(key=lambda x: x["change_pct"], reverse=True)
    declining.sort(key=lambda x: x["change_pct"])

    return {
        "period": f"{previous_month} → {year_month}",
        "growing": growing,
        "declining": declining,
        "stable": stable,
        "summary": {
            "total_accounts": len(accounts),
            "growing_count": len(growing),
            "declining_count": len(declining),
            "stable_count": len(stable),
        }
    }


def get_yearly_trend(
    account_id: str,
    year: int,
    db: Optional[Database] = None
) -> list[dict]:
    """
    Genereer maandelijkse trend data voor een heel jaar.
    Handig voor line charts.
    """
    db = db or get_connection()

    metrics = MetricsQueries.get_by_account(
        account_id,
        f"{year:04d}-01",
        f"{year:04d}-12",
        db
    )

    return [
        {
            "month": m.year_month,
            "engagement_rate": m.avg_engagement_rate,
            "followers": m.avg_followers,
            "posts": m.total_posts,
            "likes": m.total_likes,
            "comments": m.total_comments,
        }
        for m in metrics
    ]
