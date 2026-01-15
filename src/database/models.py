"""
Database schema en data models voor NL Embassy Monitor.
"""
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional
from enum import Enum
import uuid
import logging

from .connection import get_connection

logger = logging.getLogger(__name__)


class Platform(str, Enum):
    """Ondersteunde social media platforms."""
    INSTAGRAM = "instagram"
    FACEBOOK = "facebook"
    TWITTER = "twitter"


class AccountStatus(str, Enum):
    """Account status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    HACKED = "hacked"


class ContentType(str, Enum):
    """Type content."""
    IMAGE = "image"
    VIDEO = "video"
    CAROUSEL = "carousel"
    REEL = "reel"
    TEXT = "text"
    LINK = "link"
    STORY = "story"


@dataclass
class Account:
    """Social media account."""
    id: str
    country: str
    platform: str
    handle: str
    display_name: Optional[str] = None
    status: str = "active"
    notes: Optional[str] = None
    created_at: Optional[datetime] = None

    @classmethod
    def generate_id(cls, country: str, platform: str, handle: str) -> str:
        """Generate consistent account ID."""
        return f"{country}_{platform}_{handle}".lower()


@dataclass
class FollowerSnapshot:
    """Dagelijkse follower snapshot."""
    id: str
    account_id: str
    date: date
    followers: int
    following: Optional[int] = None
    collected_at: Optional[datetime] = None


@dataclass
class Post:
    """Social media post."""
    id: str
    account_id: str
    platform_post_id: str
    posted_at: datetime
    content_type: Optional[str] = None
    likes: int = 0
    comments: int = 0
    shares: int = 0
    views: Optional[int] = None
    url: Optional[str] = None
    caption_snippet: Optional[str] = None
    hashtags: Optional[list[str]] = None
    collected_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


@dataclass
class MonthlyMetrics:
    """Berekende maandelijkse metrics per account."""
    id: str
    account_id: str
    year_month: str  # Format: '2025-01'
    avg_followers: Optional[int] = None
    follower_growth: Optional[int] = None
    follower_growth_pct: Optional[float] = None
    total_posts: int = 0
    total_likes: int = 0
    total_comments: int = 0
    total_shares: int = 0
    avg_engagement_rate: Optional[float] = None
    top_post_id: Optional[str] = None
    calculated_at: Optional[datetime] = None


@dataclass
class CollectionLog:
    """Log van data collectie runs."""
    id: str
    account_id: str
    platform: str
    status: str  # success, partial, failed
    posts_collected: int = 0
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


# ============================================================
# COMMUNICATIE ANALYSE MODELS
# ============================================================

class PostContentCategory(str, Enum):
    """Categorie van post inhoud."""
    PROCEDUREEL = "procedureel"      # visumstappen, documenten, deadlines
    WIJZIGING = "wijziging"          # gewijzigde openingstijden, nieuwe regels
    WAARSCHUWING = "waarschuwing"    # sluitingen, vertragingen, storingen
    PROMOTIONEEL = "promotioneel"    # evenementen, cultuur, handel
    INTERACTIE = "interactie"        # antwoorden op vragen, verwijzingen


class TimingClass(str, Enum):
    """Classificatie van timing vooraf."""
    PROACTIEF = "proactief"          # >14 dagen vooraf
    ADEQUAAT = "adequaat"            # 7-14 dagen vooraf
    REACTIEF = "reactief"            # <7 dagen vooraf
    LAST_MINUTE = "last_minute"      # <48 uur vooraf
    NVT = "nvt"                       # niet van toepassing


class ResponseType(str, Enum):
    """Type antwoord op vraag."""
    INHOUDELIJK = "inhoudelijk"      # vraag wordt beantwoord
    DOORVERWIJZING = "doorverwijzing"  # link naar website/formulier
    STANDAARD = "standaard"          # generiek antwoord
    GEEN = "geen"                    # niet gereageerd


class QuestionType(str, Enum):
    """Type vraag in comments."""
    PROCEDURE = "procedure"          # hoe werkt iets
    STATUS = "status"                # waar staat mijn aanvraag
    KLACHT = "klacht"                # ontevreden uiting
    OVERIG = "overig"


class Sentiment(str, Enum):
    """Sentiment classificatie."""
    POSITIEF = "positief"
    NEUTRAAL = "neutraal"
    NEGATIEF = "negatief"
    GEFRUSTREERD = "gefrustreerd"


@dataclass
class PostClassification:
    """Communicatie classificatie van een post."""
    post_id: str
    content_type: Optional[str] = None          # procedureel, wijziging, etc.
    tone_formality: Optional[float] = None      # 0=informeel, 1=formeel
    tone_service_oriented: Optional[bool] = None
    tone_empathetic: Optional[bool] = None
    tone_proactive: Optional[bool] = None
    days_advance: Optional[int] = None          # dagen vóór relevante datum
    timing_class: Optional[str] = None          # proactief, adequaat, etc.
    has_call_to_action: Optional[bool] = None
    has_link: Optional[bool] = None
    has_contact_info: Optional[bool] = None
    has_deadline: Optional[bool] = None
    completeness_score: Optional[float] = None  # 0-1 schaal
    language: Optional[str] = None              # nl, en, etc.
    uses_emoji: Optional[bool] = None
    uses_formal_pronouns: Optional[bool] = None  # u vs jij
    classified_at: Optional[datetime] = None
    classification_method: Optional[str] = None  # rule_based, llm


@dataclass
class PostComment:
    """Comment op een post."""
    id: str
    post_id: str
    comment_id: Optional[str] = None            # platform-specifieke ID
    author_handle: Optional[str] = None
    comment_text: Optional[str] = None
    is_from_account: bool = False               # is dit van het account zelf?
    parent_comment_id: Optional[str] = None     # voor threaded replies
    posted_at: Optional[datetime] = None
    likes: int = 0
    collected_at: Optional[datetime] = None


@dataclass
class CommentAnalysis:
    """Analyse van een comment op een post."""
    id: str
    post_id: str
    comment_id: Optional[str] = None
    comment_text: Optional[str] = None
    is_question: bool = False
    question_type: Optional[str] = None         # procedure, status, klacht, overig
    has_response: bool = False
    response_type: Optional[str] = None         # inhoudelijk, doorverwijzing, etc.
    response_time_hours: Optional[int] = None
    sentiment: Optional[str] = None             # positief, neutraal, negatief
    analyzed_at: Optional[datetime] = None


@dataclass
class AccountCommProfile:
    """Communicatie profiel van een account (geaggregeerd)."""
    account_id: str
    total_posts_analyzed: int = 0
    pct_procedural: Optional[float] = None
    pct_wijziging: Optional[float] = None
    pct_waarschuwing: Optional[float] = None
    pct_promotional: Optional[float] = None
    pct_interaction: Optional[float] = None
    avg_days_advance: Optional[float] = None
    pct_proactive: Optional[float] = None       # % posts proactief/adequaat
    response_rate: Optional[float] = None       # % vragen beantwoord
    avg_response_hours: Optional[float] = None
    dominant_tone: Optional[str] = None         # formeel/informeel
    avg_formality_score: Optional[float] = None
    pct_with_cta: Optional[float] = None        # % met call to action
    pct_with_link: Optional[float] = None
    avg_completeness: Optional[float] = None
    last_calculated: Optional[datetime] = None


# SQL Schema
SCHEMA_SQL = """
-- Accounts tabel
CREATE TABLE IF NOT EXISTS accounts (
    id VARCHAR PRIMARY KEY,
    country VARCHAR NOT NULL,
    platform VARCHAR NOT NULL,
    handle VARCHAR NOT NULL,
    display_name VARCHAR,
    status VARCHAR DEFAULT 'active',
    notes VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Follower snapshots (dagelijks)
CREATE TABLE IF NOT EXISTS follower_snapshots (
    id VARCHAR PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    date DATE NOT NULL,
    followers INTEGER,
    following INTEGER,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_id, date)
);

-- Posts (12 maanden history)
CREATE TABLE IF NOT EXISTS posts (
    id VARCHAR PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    platform_post_id VARCHAR,
    posted_at TIMESTAMP NOT NULL,
    content_type VARCHAR,
    likes INTEGER DEFAULT 0,
    comments INTEGER DEFAULT 0,
    shares INTEGER DEFAULT 0,
    views INTEGER,
    url VARCHAR,
    caption_snippet VARCHAR,
    hashtags VARCHAR,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP,
    UNIQUE(account_id, platform_post_id)
);

-- Maandelijkse berekende metrics
CREATE TABLE IF NOT EXISTS monthly_metrics (
    id VARCHAR PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    year_month VARCHAR NOT NULL,
    avg_followers INTEGER,
    follower_growth INTEGER,
    follower_growth_pct DECIMAL(10,4),
    total_posts INTEGER DEFAULT 0,
    total_likes INTEGER DEFAULT 0,
    total_comments INTEGER DEFAULT 0,
    total_shares INTEGER DEFAULT 0,
    avg_engagement_rate DECIMAL(10,6),
    top_post_id VARCHAR,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_id, year_month)
);

-- Collection logs
CREATE TABLE IF NOT EXISTS collection_logs (
    id VARCHAR PRIMARY KEY,
    account_id VARCHAR,
    platform VARCHAR,
    status VARCHAR,
    posts_collected INTEGER DEFAULT 0,
    error_message VARCHAR,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- Checkpoints voor hervatting
CREATE TABLE IF NOT EXISTS checkpoints (
    job_id VARCHAR PRIMARY KEY,
    state VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes voor snelle queries
CREATE INDEX IF NOT EXISTS idx_posts_account_date ON posts(account_id, posted_at);
CREATE INDEX IF NOT EXISTS idx_posts_posted_at ON posts(posted_at);
CREATE INDEX IF NOT EXISTS idx_follower_snapshots_account_date ON follower_snapshots(account_id, date);
CREATE INDEX IF NOT EXISTS idx_monthly_metrics_account ON monthly_metrics(account_id);
CREATE INDEX IF NOT EXISTS idx_monthly_metrics_yearmonth ON monthly_metrics(year_month);

-- ============================================================
-- COMMUNICATIE ANALYSE TABELLEN
-- ============================================================

-- Post classificatie (tone of voice, content type, timing)
CREATE TABLE IF NOT EXISTS post_classification (
    post_id VARCHAR PRIMARY KEY,
    content_type VARCHAR,                    -- procedureel, wijziging, waarschuwing, promotioneel
    tone_formality DECIMAL(3,2),             -- 0=informeel, 1=formeel
    tone_service_oriented BOOLEAN,
    tone_empathetic BOOLEAN,
    tone_proactive BOOLEAN,
    days_advance INTEGER,                    -- dagen vóór relevante datum (NULL als n.v.t.)
    timing_class VARCHAR,                    -- proactief, adequaat, reactief, last_minute, nvt
    has_call_to_action BOOLEAN,
    has_link BOOLEAN,
    has_contact_info BOOLEAN,
    has_deadline BOOLEAN,
    completeness_score DECIMAL(3,2),         -- 0-1 schaal
    language VARCHAR,                        -- nl, en, etc.
    uses_emoji BOOLEAN,
    uses_formal_pronouns BOOLEAN,            -- u vs jij
    classified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    classification_method VARCHAR            -- rule_based, llm
);

-- Comments op posts (voor interactie analyse)
CREATE TABLE IF NOT EXISTS post_comments (
    id VARCHAR PRIMARY KEY,
    post_id VARCHAR NOT NULL,
    comment_id VARCHAR,                      -- platform-specifieke ID
    author_handle VARCHAR,
    comment_text VARCHAR,
    is_from_account BOOLEAN DEFAULT FALSE,   -- is dit van het account zelf?
    parent_comment_id VARCHAR,               -- voor threaded replies
    posted_at TIMESTAMP,
    likes INTEGER DEFAULT 0,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comment analyse
CREATE TABLE IF NOT EXISTS comment_analysis (
    id VARCHAR PRIMARY KEY,
    post_id VARCHAR,
    comment_id VARCHAR,
    is_question BOOLEAN,
    question_type VARCHAR,                   -- procedure, status, klacht, overig
    has_response BOOLEAN,
    response_type VARCHAR,                   -- inhoudelijk, doorverwijzing, standaard, geen
    response_time_hours INTEGER,
    sentiment VARCHAR,                       -- positief, neutraal, negatief, gefrustreerd
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Account communicatie profiel (aggregatie)
CREATE TABLE IF NOT EXISTS account_comm_profile (
    account_id VARCHAR PRIMARY KEY,
    total_posts_analyzed INTEGER DEFAULT 0,
    pct_procedural DECIMAL(5,2),
    pct_wijziging DECIMAL(5,2),
    pct_waarschuwing DECIMAL(5,2),
    pct_promotional DECIMAL(5,2),
    pct_interaction DECIMAL(5,2),
    avg_days_advance DECIMAL(5,1),
    pct_proactive DECIMAL(5,2),              -- % posts proactief/adequaat timing
    response_rate DECIMAL(5,2),              -- % vragen beantwoord
    avg_response_hours DECIMAL(5,1),
    dominant_tone VARCHAR,                   -- formeel/informeel
    avg_formality_score DECIMAL(3,2),
    pct_with_cta DECIMAL(5,2),               -- % met call to action
    pct_with_link DECIMAL(5,2),
    avg_completeness DECIMAL(3,2),
    last_calculated TIMESTAMP
);

-- Indexes voor communicatie analyse
CREATE INDEX IF NOT EXISTS idx_post_classification_content ON post_classification(content_type);
CREATE INDEX IF NOT EXISTS idx_post_comments_post ON post_comments(post_id);
CREATE INDEX IF NOT EXISTS idx_comment_analysis_post ON comment_analysis(post_id);
"""


def create_schema():
    """Maak database schema aan."""
    db = get_connection()

    # Split SQL statements en voer ze uit
    statements = [s.strip() for s in SCHEMA_SQL.split(';') if s.strip()]

    for statement in statements:
        try:
            db.execute(statement)
        except Exception as e:
            logger.warning(f"Schema statement warning: {e}")

    logger.info("Database schema aangemaakt/geverifieerd")


def generate_uuid() -> str:
    """Generate a new UUID."""
    return str(uuid.uuid4())
