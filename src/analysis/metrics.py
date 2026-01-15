"""
Metrics berekening voor social media prestaties.
"""
from datetime import datetime, date
from typing import Optional
from dataclasses import dataclass
import logging

from ..database.connection import Database, get_connection
from ..database.models import MonthlyMetrics, generate_uuid
from ..database.queries import AccountQueries, PostQueries, FollowerQueries, MetricsQueries

logger = logging.getLogger(__name__)


def calculate_engagement_rate(
    likes: int,
    comments: int,
    shares: int,
    followers: int
) -> float:
    """
    Bereken engagement rate.

    Formula: (likes + comments * 2 + shares * 3) / followers * 100

    Comments worden zwaarder gewogen (meer effort nodig).
    Shares zijn het meest waardevol (organisch bereik).
    """
    if followers <= 0:
        return 0.0

    # Gewogen engagement
    weighted_engagement = likes + (comments * 2) + (shares * 3)

    return (weighted_engagement / followers) * 100


def calculate_engagement_rate_simple(
    likes: int,
    comments: int,
    followers: int
) -> float:
    """
    Simpele engagement rate (zonder shares).
    Gebruikt voor platforms die geen shares tonen.
    """
    if followers <= 0:
        return 0.0

    return ((likes + comments) / followers) * 100


@dataclass
class MonthlyStats:
    """Maandelijkse statistieken voor een account."""
    account_id: str
    year_month: str
    total_posts: int
    total_likes: int
    total_comments: int
    total_shares: int
    total_views: int
    avg_likes_per_post: float
    avg_comments_per_post: float
    avg_followers: int
    follower_growth: int
    follower_growth_pct: float
    engagement_rate: float
    top_post_id: Optional[str]


def calculate_monthly_metrics(
    account_id: str,
    year: int,
    month: int,
    db: Optional[Database] = None
) -> Optional[MonthlyMetrics]:
    """
    Bereken maandelijkse metrics voor een account.
    """
    db = db or get_connection()
    year_month = f"{year:04d}-{month:02d}"

    # Bepaal date range
    start_date = date(year, month, 1)
    if month == 12:
        end_date = date(year + 1, 1, 1)
    else:
        end_date = date(year, month + 1, 1)

    # Haal posts op voor deze maand
    posts = PostQueries.get_by_account(
        account_id,
        start_date=start_date,
        end_date=end_date,
        limit=1000,
        db=db
    )

    if not posts:
        logger.debug(f"Geen posts voor {account_id} in {year_month}")
        return None

    # Bereken totalen
    total_posts = len(posts)
    total_likes = sum(p.likes for p in posts)
    total_comments = sum(p.comments for p in posts)
    total_shares = sum(p.shares for p in posts)

    # Vind top post (hoogste engagement)
    top_post = max(posts, key=lambda p: p.likes + p.comments * 2 + p.shares * 3)

    # Haal follower data op
    follower_history = FollowerQueries.get_history(
        account_id,
        start_date=start_date,
        end_date=end_date,
        db=db
    )

    avg_followers = 0
    follower_growth = 0
    follower_growth_pct = 0.0

    if follower_history:
        followers_list = [f.followers for f in follower_history if f.followers]
        if followers_list:
            avg_followers = sum(followers_list) // len(followers_list)
            follower_growth = followers_list[-1] - followers_list[0]

            if followers_list[0] > 0:
                follower_growth_pct = (follower_growth / followers_list[0]) * 100
    else:
        # Probeer laatste bekende followers
        latest = FollowerQueries.get_latest(account_id, db)
        if latest and latest.followers:
            avg_followers = latest.followers

    # Bereken engagement rate
    engagement_rate = 0.0
    if avg_followers > 0 and total_posts > 0:
        avg_engagement_per_post = (total_likes + total_comments * 2 + total_shares * 3) / total_posts
        engagement_rate = (avg_engagement_per_post / avg_followers) * 100

    # Maak metrics object
    metrics = MonthlyMetrics(
        id=f"{account_id}_{year_month}",
        account_id=account_id,
        year_month=year_month,
        avg_followers=avg_followers,
        follower_growth=follower_growth,
        follower_growth_pct=round(follower_growth_pct, 4),
        total_posts=total_posts,
        total_likes=total_likes,
        total_comments=total_comments,
        total_shares=total_shares,
        avg_engagement_rate=round(engagement_rate, 6),
        top_post_id=top_post.id if top_post else None,
        calculated_at=datetime.now(),
    )

    return metrics


def calculate_all_monthly_metrics(
    year: int,
    month: int,
    db: Optional[Database] = None
) -> list[MonthlyMetrics]:
    """
    Bereken maandelijkse metrics voor alle actieve accounts.
    """
    db = db or get_connection()

    accounts = AccountQueries.get_all(db)
    results = []

    for account in accounts:
        metrics = calculate_monthly_metrics(account.id, year, month, db)
        if metrics:
            MetricsQueries.upsert(metrics, db)
            results.append(metrics)

    logger.info(f"Metrics berekend voor {len(results)} accounts in {year}-{month:02d}")
    return results


def get_account_performance_summary(
    account_id: str,
    months: int = 12,
    db: Optional[Database] = None
) -> dict:
    """
    Genereer performance samenvatting voor een account.
    """
    db = db or get_connection()

    # Bepaal periode
    now = datetime.now()
    start_year = now.year if now.month > months else now.year - 1
    start_month = (now.month - months) % 12 + 1
    start_month_str = f"{start_year:04d}-{start_month:02d}"
    end_month_str = f"{now.year:04d}-{now.month:02d}"

    # Haal metrics op
    metrics = MetricsQueries.get_by_account(
        account_id,
        start_month=start_month_str,
        end_month=end_month_str,
        db=db
    )

    if not metrics:
        return {}

    # Bereken totalen en gemiddelden
    total_posts = sum(m.total_posts for m in metrics)
    total_likes = sum(m.total_likes for m in metrics)
    total_comments = sum(m.total_comments for m in metrics)
    total_shares = sum(m.total_shares for m in metrics)

    avg_engagement_rates = [m.avg_engagement_rate for m in metrics if m.avg_engagement_rate]
    avg_engagement = sum(avg_engagement_rates) / len(avg_engagement_rates) if avg_engagement_rates else 0

    # Follower groei over periode
    first_month = metrics[0]
    last_month = metrics[-1]

    total_follower_growth = 0
    if first_month.avg_followers and last_month.avg_followers:
        total_follower_growth = last_month.avg_followers - first_month.avg_followers

    # Beste en slechtste maand
    best_month = max(metrics, key=lambda m: m.avg_engagement_rate or 0)
    worst_month = min(metrics, key=lambda m: m.avg_engagement_rate or float('inf'))

    return {
        "period": f"{start_month_str} - {end_month_str}",
        "months_analyzed": len(metrics),
        "totals": {
            "posts": total_posts,
            "likes": total_likes,
            "comments": total_comments,
            "shares": total_shares,
        },
        "averages": {
            "posts_per_month": total_posts / len(metrics) if metrics else 0,
            "engagement_rate": round(avg_engagement, 4),
        },
        "followers": {
            "current": last_month.avg_followers,
            "growth": total_follower_growth,
            "growth_pct": round((total_follower_growth / first_month.avg_followers * 100), 2)
                          if first_month.avg_followers else 0,
        },
        "best_month": {
            "month": best_month.year_month,
            "engagement_rate": best_month.avg_engagement_rate,
        },
        "worst_month": {
            "month": worst_month.year_month,
            "engagement_rate": worst_month.avg_engagement_rate,
        },
    }
