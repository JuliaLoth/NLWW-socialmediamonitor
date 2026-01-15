"""
Database queries voor NL Embassy Monitor.
"""
from datetime import datetime, date, timedelta
from typing import Optional
import json
import logging

from .connection import get_connection, Database
from .models import Account, Post, FollowerSnapshot, MonthlyMetrics, generate_uuid

logger = logging.getLogger(__name__)


class AccountQueries:
    """Queries voor accounts."""

    @staticmethod
    def get_all(db: Optional[Database] = None) -> list[Account]:
        """Haal alle actieve accounts op."""
        db = db or get_connection()
        rows = db.fetchall("""
            SELECT id, country, platform, handle, display_name, status, notes, created_at
            FROM accounts
            WHERE status = 'active'
            ORDER BY country, platform
        """)
        return [Account(*row) for row in rows]

    @staticmethod
    def get_by_country(country: str, db: Optional[Database] = None) -> list[Account]:
        """Haal accounts op voor een land."""
        db = db or get_connection()
        rows = db.fetchall("""
            SELECT id, country, platform, handle, display_name, status, notes, created_at
            FROM accounts
            WHERE country = ? AND status = 'active'
        """, [country])
        return [Account(*row) for row in rows]

    @staticmethod
    def get_by_platform(platform: str, db: Optional[Database] = None) -> list[Account]:
        """Haal accounts op per platform."""
        db = db or get_connection()
        rows = db.fetchall("""
            SELECT id, country, platform, handle, display_name, status, notes, created_at
            FROM accounts
            WHERE platform = ? AND status = 'active'
        """, [platform])
        return [Account(*row) for row in rows]

    @staticmethod
    def get_by_id(account_id: str, db: Optional[Database] = None) -> Optional[Account]:
        """Haal account op via ID."""
        db = db or get_connection()
        row = db.fetchone("""
            SELECT id, country, platform, handle, display_name, status, notes, created_at
            FROM accounts WHERE id = ?
        """, [account_id])
        return Account(*row) if row else None

    @staticmethod
    def upsert(account: Account, db: Optional[Database] = None):
        """Insert of update een account."""
        db = db or get_connection()
        db.execute("""
            INSERT INTO accounts (id, country, platform, handle, display_name, status, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            ON CONFLICT (id) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                status = EXCLUDED.status,
                notes = EXCLUDED.notes
        """, [
            account.id, account.country, account.platform, account.handle,
            account.display_name, account.status, account.notes, account.created_at
        ])

    @staticmethod
    def count_by_platform(db: Optional[Database] = None) -> dict[str, int]:
        """Tel accounts per platform."""
        db = db or get_connection()
        rows = db.fetchall("""
            SELECT platform, COUNT(*) as count
            FROM accounts
            WHERE status = 'active'
            GROUP BY platform
        """)
        return {row[0]: row[1] for row in rows}


class PostQueries:
    """Queries voor posts."""

    @staticmethod
    def get_by_account(
        account_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
        db: Optional[Database] = None
    ) -> list[Post]:
        """Haal posts op voor een account."""
        db = db or get_connection()

        query = """
            SELECT id, account_id, platform_post_id, posted_at, content_type,
                   likes, comments, shares, views, url, caption_snippet, hashtags,
                   collected_at, last_updated
            FROM posts
            WHERE account_id = ?
        """
        params = [account_id]

        if start_date:
            query += " AND posted_at >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND posted_at <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY posted_at DESC LIMIT ?"
        params.append(limit)

        rows = db.fetchall(query, params)
        return [Post(*row) for row in rows]

    @staticmethod
    def get_latest_post_date(account_id: str, db: Optional[Database] = None) -> Optional[datetime]:
        """Haal datum van laatste post op."""
        db = db or get_connection()
        row = db.fetchone("""
            SELECT MAX(posted_at) FROM posts WHERE account_id = ?
        """, [account_id])
        return row[0] if row and row[0] else None

    @staticmethod
    def get_posts_for_update(days: int = 7, db: Optional[Database] = None) -> list[Post]:
        """Haal recente posts op die update nodig hebben."""
        db = db or get_connection()
        cutoff = datetime.now() - timedelta(days=days)
        rows = db.fetchall("""
            SELECT id, account_id, platform_post_id, posted_at, content_type,
                   likes, comments, shares, views, url, caption_snippet, hashtags,
                   collected_at, last_updated
            FROM posts
            WHERE posted_at >= ?
            ORDER BY posted_at DESC
        """, [cutoff.isoformat()])
        return [Post(*row) for row in rows]

    @staticmethod
    def upsert(post: Post, db: Optional[Database] = None):
        """Insert of update een post."""
        db = db or get_connection()
        hashtags_str = json.dumps(post.hashtags) if post.hashtags else None
        db.execute("""
            INSERT INTO posts
            (id, account_id, platform_post_id, posted_at, content_type,
             likes, comments, shares, views, url, caption_snippet, hashtags,
             collected_at, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?)
            ON CONFLICT (account_id, platform_post_id) DO UPDATE SET
                likes = EXCLUDED.likes,
                comments = EXCLUDED.comments,
                shares = EXCLUDED.shares,
                views = EXCLUDED.views,
                last_updated = EXCLUDED.last_updated
        """, [
            post.id, post.account_id, post.platform_post_id, post.posted_at,
            post.content_type, post.likes, post.comments, post.shares, post.views,
            post.url, post.caption_snippet, hashtags_str, post.collected_at,
            post.last_updated or datetime.now()
        ])

    @staticmethod
    def get_top_posts(
        start_date: date,
        end_date: date,
        limit: int = 10,
        db: Optional[Database] = None
    ) -> list[Post]:
        """Haal top performing posts op."""
        db = db or get_connection()
        rows = db.fetchall("""
            SELECT id, account_id, platform_post_id, posted_at, content_type,
                   likes, comments, shares, views, url, caption_snippet, hashtags,
                   collected_at, last_updated
            FROM posts
            WHERE posted_at BETWEEN ? AND ?
            ORDER BY (likes + comments * 2 + shares * 3) DESC
            LIMIT ?
        """, [start_date.isoformat(), end_date.isoformat(), limit])
        return [Post(*row) for row in rows]


class FollowerQueries:
    """Queries voor follower snapshots."""

    @staticmethod
    def get_history(
        account_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        db: Optional[Database] = None
    ) -> list[FollowerSnapshot]:
        """Haal follower historie op."""
        db = db or get_connection()

        query = """
            SELECT id, account_id, date, followers, following, collected_at
            FROM follower_snapshots
            WHERE account_id = ?
        """
        params = [account_id]

        if start_date:
            query += " AND date >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND date <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY date"
        rows = db.fetchall(query, params)
        return [FollowerSnapshot(*row) for row in rows]

    @staticmethod
    def get_latest(account_id: str, db: Optional[Database] = None) -> Optional[FollowerSnapshot]:
        """Haal laatste follower snapshot op."""
        db = db or get_connection()
        row = db.fetchone("""
            SELECT id, account_id, date, followers, following, collected_at
            FROM follower_snapshots
            WHERE account_id = ?
            ORDER BY date DESC
            LIMIT 1
        """, [account_id])
        return FollowerSnapshot(*row) if row else None

    @staticmethod
    def upsert(snapshot: FollowerSnapshot, db: Optional[Database] = None):
        """Insert of update een follower snapshot."""
        db = db or get_connection()
        db.execute("""
            INSERT INTO follower_snapshots
            (id, account_id, date, followers, following, collected_at)
            VALUES (?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            ON CONFLICT (account_id, date) DO UPDATE SET
                followers = EXCLUDED.followers,
                following = EXCLUDED.following,
                collected_at = EXCLUDED.collected_at
        """, [
            snapshot.id, snapshot.account_id, snapshot.date,
            snapshot.followers, snapshot.following, snapshot.collected_at
        ])

    @staticmethod
    def get_growth_by_month(
        account_id: str,
        db: Optional[Database] = None
    ) -> list[tuple[str, int, int]]:
        """Bereken follower groei per maand."""
        db = db or get_connection()
        rows = db.fetchall("""
            WITH monthly AS (
                SELECT
                    strftime('%Y-%m', date) as year_month,
                    MIN(followers) as start_followers,
                    MAX(followers) as end_followers
                FROM follower_snapshots
                WHERE account_id = ?
                GROUP BY strftime('%Y-%m', date)
            )
            SELECT
                year_month,
                end_followers - start_followers as growth,
                end_followers
            FROM monthly
            ORDER BY year_month
        """, [account_id])
        return rows


class MetricsQueries:
    """Queries voor monthly metrics."""

    @staticmethod
    def get_by_account(
        account_id: str,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        db: Optional[Database] = None
    ) -> list[MonthlyMetrics]:
        """Haal maandelijkse metrics op voor een account."""
        db = db or get_connection()

        query = """
            SELECT id, account_id, year_month, avg_followers, follower_growth,
                   follower_growth_pct, total_posts, total_likes, total_comments,
                   total_shares, avg_engagement_rate, top_post_id, calculated_at
            FROM monthly_metrics
            WHERE account_id = ?
        """
        params = [account_id]

        if start_month:
            query += " AND year_month >= ?"
            params.append(start_month)
        if end_month:
            query += " AND year_month <= ?"
            params.append(end_month)

        query += " ORDER BY year_month"
        rows = db.fetchall(query, params)
        return [MonthlyMetrics(*row) for row in rows]

    @staticmethod
    def get_all_for_month(year_month: str, db: Optional[Database] = None) -> list[MonthlyMetrics]:
        """Haal metrics op voor alle accounts voor een specifieke maand."""
        db = db or get_connection()
        rows = db.fetchall("""
            SELECT id, account_id, year_month, avg_followers, follower_growth,
                   follower_growth_pct, total_posts, total_likes, total_comments,
                   total_shares, avg_engagement_rate, top_post_id, calculated_at
            FROM monthly_metrics
            WHERE year_month = ?
            ORDER BY avg_engagement_rate DESC NULLS LAST
        """, [year_month])
        return [MonthlyMetrics(*row) for row in rows]

    @staticmethod
    def upsert(metrics: MonthlyMetrics, db: Optional[Database] = None):
        """Insert of update monthly metrics."""
        db = db or get_connection()
        db.execute("""
            INSERT INTO monthly_metrics
            (id, account_id, year_month, avg_followers, follower_growth,
             follower_growth_pct, total_posts, total_likes, total_comments,
             total_shares, avg_engagement_rate, top_post_id, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            ON CONFLICT (account_id, year_month) DO UPDATE SET
                avg_followers = EXCLUDED.avg_followers,
                follower_growth = EXCLUDED.follower_growth,
                follower_growth_pct = EXCLUDED.follower_growth_pct,
                total_posts = EXCLUDED.total_posts,
                total_likes = EXCLUDED.total_likes,
                total_comments = EXCLUDED.total_comments,
                total_shares = EXCLUDED.total_shares,
                avg_engagement_rate = EXCLUDED.avg_engagement_rate,
                top_post_id = EXCLUDED.top_post_id,
                calculated_at = EXCLUDED.calculated_at
        """, [
            metrics.id, metrics.account_id, metrics.year_month, metrics.avg_followers,
            metrics.follower_growth, metrics.follower_growth_pct, metrics.total_posts,
            metrics.total_likes, metrics.total_comments, metrics.total_shares,
            metrics.avg_engagement_rate, metrics.top_post_id, metrics.calculated_at
        ])

    @staticmethod
    def get_benchmark_ranking(
        year_month: str,
        metric: str = "avg_engagement_rate",
        db: Optional[Database] = None
    ) -> list[tuple[str, str, float, int]]:
        """Haal ranking op voor een specifieke metric."""
        db = db or get_connection()
        valid_metrics = ["avg_engagement_rate", "follower_growth_pct", "total_posts", "total_likes"]
        if metric not in valid_metrics:
            metric = "avg_engagement_rate"

        rows = db.fetchall(f"""
            SELECT
                m.account_id,
                a.country,
                m.{metric},
                ROW_NUMBER() OVER (ORDER BY m.{metric} DESC NULLS LAST) as rank
            FROM monthly_metrics m
            JOIN accounts a ON m.account_id = a.id
            WHERE m.year_month = ? AND a.status = 'active'
            ORDER BY rank
        """, [year_month])
        return rows


class CommentQueries:
    """Queries voor post comments."""

    @staticmethod
    def upsert(comment, db: Optional[Database] = None):
        """Insert of update een comment."""
        db = db or get_connection()
        db.execute("""
            INSERT INTO post_comments
            (id, post_id, comment_id, author_handle, comment_text,
             is_from_account, parent_comment_id, posted_at, likes, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            ON CONFLICT (id) DO UPDATE SET
                comment_text = EXCLUDED.comment_text,
                likes = EXCLUDED.likes,
                collected_at = EXCLUDED.collected_at
        """, [
            comment.id, comment.post_id, comment.comment_id,
            comment.author_handle, comment.comment_text,
            comment.is_from_account, comment.parent_comment_id,
            comment.posted_at, comment.likes, comment.collected_at
        ])

    @staticmethod
    def get_by_post(post_id: str, db: Optional[Database] = None) -> list:
        """Haal comments op voor een post."""
        db = db or get_connection()
        rows = db.fetchall("""
            SELECT id, post_id, comment_id, author_handle, comment_text,
                   is_from_account, parent_comment_id, posted_at, likes, collected_at
            FROM post_comments
            WHERE post_id = ?
            ORDER BY posted_at
        """, [post_id])
        return rows

    @staticmethod
    def get_questions_by_account(account_id: str, db: Optional[Database] = None) -> list:
        """Haal comments op die vragen bevatten voor een account."""
        db = db or get_connection()
        rows = db.fetchall("""
            SELECT c.id, c.post_id, c.comment_text, c.author_handle,
                   c.posted_at, c.is_from_account
            FROM post_comments c
            JOIN posts p ON c.post_id = p.id
            WHERE p.account_id = ?
              AND c.comment_text LIKE '%?%'
              AND c.is_from_account = FALSE
            ORDER BY c.posted_at DESC
        """, [account_id])
        return rows

    @staticmethod
    def count_by_account(account_id: str, db: Optional[Database] = None) -> dict:
        """Tel comments per account."""
        db = db or get_connection()
        result = db.fetchone("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN c.is_from_account THEN 1 ELSE 0 END) as from_account,
                SUM(CASE WHEN c.comment_text LIKE '%?%' AND NOT c.is_from_account THEN 1 ELSE 0 END) as questions
            FROM post_comments c
            JOIN posts p ON c.post_id = p.id
            WHERE p.account_id = ?
        """, [account_id])
        return {
            "total": result[0] or 0,
            "from_account": result[1] or 0,
            "questions": result[2] or 0,
        }
