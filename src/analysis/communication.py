"""
Communicatie Analyse Module

Analyseert tone of voice, content type, en interactiepatronen
van overheidsaccounts op social media.
"""
import re
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
import logging

from ..database.connection import Database, get_connection
from ..database.models import (
    Post, PostClassification, CommentAnalysis, AccountCommProfile,
    PostContentCategory, TimingClass, ResponseType, QuestionType, Sentiment,
    generate_uuid
)

logger = logging.getLogger(__name__)


# ============================================================
# KEYWORD PATTERNS VOOR RULE-BASED CLASSIFICATIE
# ============================================================

# Content type keywords (Nederlands + Engels)
PROCEDUREEL_KEYWORDS = [
    # Nederlands
    r'\bvisum\b', r'\bvisa\b', r'\baanvra\w+\b', r'\bdocument\w*\b',
    r'\bpaspoort\b', r'\bidentiteit\w*\b', r'\bformulier\w*\b',
    r'\bprocedure\w*\b', r'\bstap\w*\b', r'\bvereist\w*\b', r'\bnodig\b',
    r'\blegalis\w+\b', r'\bapoστille\b', r'\bverlenging\b', r'\bgeldig\w*\b',
    r'\bkost\w*\b', r'\btarief\b', r'\bbetaling\b', r'\bafspraak\b',
    # Engels
    r'\bapplication\b', r'\bapply\b', r'\brequired\b', r'\bdocuments?\b',
    r'\bpassport\b', r'\bidentity\b', r'\bform\b', r'\bprocedure\b',
    r'\bsteps?\b', r'\brequirements?\b', r'\bfee\b', r'\bappointment\b',
]

WIJZIGING_KEYWORDS = [
    # Nederlands
    r'\bgewijzigd\b', r'\bwijziging\w*\b', r'\bnieuwe?\b', r'\bupdate\b',
    r'\bverandering\w*\b', r'\bper\s+\d', r'\bvanaf\b', r'\bvoortaan\b',
    r'\baangepast\b', r'\bhervorming\b', r'\bnieuwe\s+regel\w*\b',
    # Engels
    r'\bchanged?\b', r'\bupdate[ds]?\b', r'\bnew\b', r'\bstarting\b',
    r'\beffective\b', r'\bmodified\b', r'\brevised\b',
]

WAARSCHUWING_KEYWORDS = [
    # Nederlands
    r'\bgesloten\b', r'\bsluiting\b', r'\bvertraging\b', r'\bstoring\b',
    r'\blet op\b', r'\bwaarschuwing\b', r'\bdringend\b', r'\bspoed\b',
    r'\bonderbreking\b', r'\bniet beschikbaar\b', r'\buitgesteld\b',
    r'\bgeannuleerd\b', r'\bopgeschort\b',
    # Engels
    r'\bclosed\b', r'\bclosure\b', r'\bdelay\w*\b', r'\bdisruption\b',
    r'\bwarning\b', r'\burgent\b', r'\bsuspend\w*\b', r'\bcancel\w*\b',
    r'\bpostpone\w*\b', r'\bunavailable\b',
]

PROMOTIONEEL_KEYWORDS = [
    # Nederlands
    r'\bevenement\b', r'\bviering\b', r'\bfeest\w*\b', r'\bcultuur\w*\b',
    r'\bkunst\b', r'\bhandel\w*\b', r'\bmissie\b', r'\bsamenwerk\w*\b',
    r'\bpartnership\b', r'\binvesteer\w*\b', r'\bexport\b', r'\bimport\b',
    r'\bstartup\b', r'\binnovatie\b', r'\bnetwerk\w*\b',
    # Engels
    r'\bevent\b', r'\bcelebrat\w*\b', r'\bculture\b', r'\bart\b',
    r'\btrade\b', r'\bmission\b', r'\bpartnership\b', r'\binvest\w*\b',
]

# Tone indicators
FORMAL_INDICATORS = [
    r'\bu\b', r'\buw\b', r'\bdient\b', r'\bgelieve\b', r'\bverzoeken\b',
    r'\bhierbij\b', r'\bingevolge\b', r'\bconform\b', r'\bterzake\b',
    r'\bdesgewenst\b', r'\bindien\b', r'\bmits\b', r'\bteneinde\b',
]

INFORMAL_INDICATORS = [
    r'\bje\b', r'\bjij\b', r'\bjouw\b', r'\bhey\b', r'\bhoi\b',
    r'\bcheck\b', r'\bcool\b', r'\btop\b', r'\bsuper\b', r'\bleuk\b',
]

SERVICE_ORIENTED_INDICATORS = [
    r'\bhelp\w*\b', r'\bvragen?\b', r'\bcontact\b', r'\bbereik\w*\b',
    r'\bservice\b', r'\bklantenservice\b', r'\bsteunen\b', r'\bbijstaan\b',
    r'\bhow can we\b', r'\bhoe kunnen\b', r'\bwe are here\b',
]

CALL_TO_ACTION_PATTERNS = [
    r'\bklik\b', r'\bbezoek\b', r'\bcheck\b', r'\blees\b', r'\bbekijk\b',
    r'\bdownload\b', r'\bregistreer\b', r'\bmeld.*aan\b', r'\bmaak.*afspraak\b',
    r'\bclick\b', r'\bvisit\b', r'\bread\b', r'\bview\b', r'\bregister\b',
    r'\bbook\b', r'\bschedule\b', r'\bapply now\b', r'\bvraag aan\b',
]

# Question detection patterns
QUESTION_PATTERNS = [
    r'\?',
    r'\bhoe\b.*\?', r'\bwat\b.*\?', r'\bwanneer\b.*\?', r'\bwaar\b.*\?',
    r'\bkan ik\b', r'\bkunnen we\b', r'\bis het mogelijk\b',
    r'\bik wil weten\b', r'\bgraag weten\b', r'\binfo\w* over\b',
    r'\bhow\b.*\?', r'\bwhat\b.*\?', r'\bwhen\b.*\?', r'\bwhere\b.*\?',
    r'\bcan i\b', r'\bcould you\b', r'\bplease tell\b',
]

COMPLAINT_PATTERNS = [
    r'\bschandalig\b', r'\bslecht\w*\b', r'\bwachten\b', r'\bfrustrer\w*\b',
    r'\bonacceptabel\b', r'\bteleurgesteld\b', r'\bklacht\b',
    r'\bniet gereageerd\b', r'\bgeen antwoord\b', r'\bwachttijd\b',
    r'\bunacceptable\b', r'\bdisappoint\w*\b', r'\bfrustrat\w*\b',
    r'\bno response\b', r'\bstill waiting\b', r'\bcomplaint\b',
]


# ============================================================
# CLASSIFICATIE FUNCTIES
# ============================================================

def classify_content_type(text: str) -> str:
    """
    Classificeer post content type op basis van keywords.
    Returns: procedureel, wijziging, waarschuwing, promotioneel, overig
    """
    if not text:
        return "overig"

    text_lower = text.lower()

    scores = {
        'procedureel': 0,
        'wijziging': 0,
        'waarschuwing': 0,
        'promotioneel': 0,
    }

    # Tel matches per categorie
    for pattern in PROCEDUREEL_KEYWORDS:
        if re.search(pattern, text_lower):
            scores['procedureel'] += 1

    for pattern in WIJZIGING_KEYWORDS:
        if re.search(pattern, text_lower):
            scores['wijziging'] += 1

    for pattern in WAARSCHUWING_KEYWORDS:
        if re.search(pattern, text_lower):
            scores['waarschuwing'] += 1

    for pattern in PROMOTIONEEL_KEYWORDS:
        if re.search(pattern, text_lower):
            scores['promotioneel'] += 1

    # Bepaal hoogste score
    max_score = max(scores.values())
    if max_score == 0:
        return "overig"

    # Return categorie met hoogste score
    for category, score in scores.items():
        if score == max_score:
            return category

    return "overig"


def calculate_formality_score(text: str) -> float:
    """
    Bereken formaliteit score (0 = informeel, 1 = formeel).
    """
    if not text:
        return 0.5

    text_lower = text.lower()

    formal_count = sum(1 for p in FORMAL_INDICATORS if re.search(p, text_lower))
    informal_count = sum(1 for p in INFORMAL_INDICATORS if re.search(p, text_lower))

    # Basis score
    total = formal_count + informal_count
    if total == 0:
        return 0.5  # Neutraal

    # Score tussen 0 en 1
    score = formal_count / total

    # Aanpassingen op basis van andere indicatoren
    if re.search(r'[\U0001F600-\U0001F64F]', text):  # Emoji
        score -= 0.1
    if text.isupper():  # ALL CAPS
        score -= 0.2
    if re.search(r'!!+', text):  # Multiple exclamation marks
        score -= 0.1

    return max(0.0, min(1.0, score))


def is_service_oriented(text: str) -> bool:
    """Check of de tekst servicegericht is."""
    if not text:
        return False
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in SERVICE_ORIENTED_INDICATORS)


def has_call_to_action(text: str) -> bool:
    """Check of de tekst een call to action bevat."""
    if not text:
        return False
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in CALL_TO_ACTION_PATTERNS)


def has_link(text: str) -> bool:
    """Check of de tekst een link bevat."""
    if not text:
        return False
    return bool(re.search(r'https?://|www\.|\.nl|\.com|\.org', text.lower()))


def has_contact_info(text: str) -> bool:
    """Check of de tekst contactgegevens bevat."""
    if not text:
        return False
    patterns = [
        r'\b\d{2,4}[-\s]?\d{6,8}\b',  # Telefoonnummer
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',  # Email
        r'\bcontact\b', r'\bbel\b', r'\bmail\b', r'\bemail\b',
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def has_deadline(text: str) -> bool:
    """Check of de tekst een deadline bevat."""
    if not text:
        return False
    patterns = [
        r'\bvoor\s+\d{1,2}[\s/-]\w+\b',  # voor 15 januari
        r'\btot\s+\d{1,2}[\s/-]\w+\b',   # tot 15 januari
        r'\bdeadline\b', r'\buiterlijk\b', r'\blaatste dag\b',
        r'\bby\s+\w+\s+\d{1,2}\b', r'\buntil\b', r'\bbefore\b',
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def uses_emoji(text: str) -> bool:
    """Check of de tekst emoji's bevat."""
    if not text:
        return False
    # Emoji unicode ranges
    emoji_pattern = re.compile(
        "[\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"   # symbols & pictographs
        "\U0001F680-\U0001F6FF"   # transport & map
        "\U0001F1E0-\U0001F1FF"   # flags
        "\U00002702-\U000027B0"   # dingbats
        "\U000024C2-\U0001F251"   # enclosed characters
        "]+", flags=re.UNICODE
    )
    return bool(emoji_pattern.search(text))


def uses_formal_pronouns(text: str) -> bool:
    """Check of formele aanspreekvorm (u) wordt gebruikt."""
    if not text:
        return False
    text_lower = text.lower()
    # Zoek naar 'u' als apart woord (niet in andere woorden)
    formal = bool(re.search(r'\bu\b|\buw\b', text_lower))
    informal = bool(re.search(r'\bje\b|\bjij\b|\bjouw\b', text_lower))
    return formal and not informal


def detect_language(text: str) -> str:
    """Detecteer taal (simpele heuristiek)."""
    if not text:
        return "unknown"

    text_lower = text.lower()

    dutch_indicators = ['de', 'het', 'een', 'van', 'en', 'voor', 'met', 'is', 'dat', 'op']
    english_indicators = ['the', 'a', 'an', 'of', 'and', 'for', 'with', 'is', 'that', 'to']

    dutch_count = sum(1 for w in dutch_indicators if re.search(rf'\b{w}\b', text_lower))
    english_count = sum(1 for w in english_indicators if re.search(rf'\b{w}\b', text_lower))

    if dutch_count > english_count:
        return "nl"
    elif english_count > dutch_count:
        return "en"
    return "unknown"


def calculate_completeness(text: str) -> float:
    """
    Bereken volledigheid score (wie/wat/wanneer/hoe aanwezig).
    Returns: 0.0 - 1.0
    """
    if not text:
        return 0.0

    score = 0.0
    max_score = 4.0

    # WIE - is er een actor/organisatie genoemd?
    if re.search(r'\bambassade\b|\bconsulaat\b|\bministerie\b|\brijksoverheid\b|\bembassy\b',
                 text, re.IGNORECASE):
        score += 1.0

    # WAT - is er een actie/onderwerp?
    if has_call_to_action(text) or classify_content_type(text) != "overig":
        score += 1.0

    # WANNEER - is er een datum/tijd?
    if re.search(r'\d{1,2}[-/]\d{1,2}|\bmaandag\b|\bdinsdag\b|\bwoensdag\b|\bdonderdag\b|'
                 r'\bvrijdag\b|\bmaandag\b|\bjanuar\w*\b|\bfebruar\w*\b|\bmaart\b|\bapril\b|'
                 r'\bmei\b|\bjuni\b|\bjuli\b|\baugustus\b|\bseptember\b|\boktober\b|'
                 r'\bnovember\b|\bdecember\b', text, re.IGNORECASE):
        score += 1.0

    # HOE - is er een link of instructie?
    if has_link(text) or has_contact_info(text):
        score += 1.0

    return score / max_score


def is_question(text: str) -> bool:
    """Check of tekst een vraag bevat."""
    if not text:
        return False
    return any(re.search(p, text, re.IGNORECASE) for p in QUESTION_PATTERNS)


def classify_question_type(text: str) -> Optional[str]:
    """Classificeer type vraag."""
    if not text or not is_question(text):
        return None

    text_lower = text.lower()

    # Status vragen
    if re.search(r'\bstatus\b|\bwaar staat\b|\bhoever\b|\bwachttijd\b|'
                 r'\bhow long\b|\bwhere is my\b', text_lower):
        return QuestionType.STATUS.value

    # Klachten
    if any(re.search(p, text_lower) for p in COMPLAINT_PATTERNS):
        return QuestionType.KLACHT.value

    # Procedure vragen
    if re.search(r'\bhoe\b|\bwat\b|\bwelke\b|\bwanneer\b|\bhow\b|\bwhat\b|\bwhen\b',
                 text_lower):
        return QuestionType.PROCEDURE.value

    return QuestionType.OVERIG.value


def classify_sentiment(text: str) -> str:
    """Classificeer sentiment van tekst."""
    if not text:
        return Sentiment.NEUTRAAL.value

    text_lower = text.lower()

    # Gefrustreerd
    if any(re.search(p, text_lower) for p in COMPLAINT_PATTERNS):
        return Sentiment.GEFRUSTREERD.value

    # Negatief
    negative_patterns = [r'\bniet\b.*\bgoed\b', r'\bslecht\b', r'\bjammer\b',
                         r'\bhelaas\b', r'\bteleurgesteld\b', r'\bunfortunately\b']
    if any(re.search(p, text_lower) for p in negative_patterns):
        return Sentiment.NEGATIEF.value

    # Positief
    positive_patterns = [r'\bbedankt\b', r'\bthanks\b', r'\bperfect\b',
                         r'\bgoed\b', r'\bfijn\b', r'\bgreat\b', r'\bexcellent\b']
    if any(re.search(p, text_lower) for p in positive_patterns):
        return Sentiment.POSITIEF.value

    return Sentiment.NEUTRAAL.value


# ============================================================
# POST CLASSIFICATIE
# ============================================================

def classify_post(post: Post) -> PostClassification:
    """
    Classificeer een post op alle dimensies.
    """
    text = post.caption_snippet or ""

    return PostClassification(
        post_id=post.id,
        content_type=classify_content_type(text),
        tone_formality=calculate_formality_score(text),
        tone_service_oriented=is_service_oriented(text),
        tone_empathetic=bool(re.search(r'\bbegrijp\w*\b|\bsnap\w*\b|\bunderstand\b',
                                       text, re.IGNORECASE)),
        tone_proactive=has_call_to_action(text),
        days_advance=None,  # Vereist datum extractie - TODO met LLM
        timing_class=TimingClass.NVT.value,
        has_call_to_action=has_call_to_action(text),
        has_link=has_link(text),
        has_contact_info=has_contact_info(text),
        has_deadline=has_deadline(text),
        completeness_score=calculate_completeness(text),
        language=detect_language(text),
        uses_emoji=uses_emoji(text),
        uses_formal_pronouns=uses_formal_pronouns(text),
        classified_at=datetime.now(),
        classification_method="rule_based"
    )


def classify_posts_batch(posts: List[Post], db: Optional[Database] = None) -> List[PostClassification]:
    """
    Classificeer een batch posts.
    """
    db = db or get_connection()
    results = []

    for post in posts:
        classification = classify_post(post)
        results.append(classification)

        # Opslaan in database
        save_post_classification(classification, db)

    logger.info(f"Geclassificeerd: {len(results)} posts")
    return results


def save_post_classification(classification: PostClassification, db: Database):
    """Sla post classificatie op in database."""
    db.execute("""
        INSERT INTO post_classification (
            post_id, content_type, tone_formality, tone_service_oriented,
            tone_empathetic, tone_proactive, days_advance, timing_class,
            has_call_to_action, has_link, has_contact_info, has_deadline,
            completeness_score, language, uses_emoji, uses_formal_pronouns,
            classified_at, classification_method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (post_id) DO UPDATE SET
            content_type = EXCLUDED.content_type,
            tone_formality = EXCLUDED.tone_formality,
            tone_service_oriented = EXCLUDED.tone_service_oriented,
            tone_empathetic = EXCLUDED.tone_empathetic,
            tone_proactive = EXCLUDED.tone_proactive,
            days_advance = EXCLUDED.days_advance,
            timing_class = EXCLUDED.timing_class,
            has_call_to_action = EXCLUDED.has_call_to_action,
            has_link = EXCLUDED.has_link,
            has_contact_info = EXCLUDED.has_contact_info,
            has_deadline = EXCLUDED.has_deadline,
            completeness_score = EXCLUDED.completeness_score,
            language = EXCLUDED.language,
            uses_emoji = EXCLUDED.uses_emoji,
            uses_formal_pronouns = EXCLUDED.uses_formal_pronouns,
            classified_at = EXCLUDED.classified_at,
            classification_method = EXCLUDED.classification_method
    """, [
        classification.post_id,
        classification.content_type,
        classification.tone_formality,
        classification.tone_service_oriented,
        classification.tone_empathetic,
        classification.tone_proactive,
        classification.days_advance,
        classification.timing_class,
        classification.has_call_to_action,
        classification.has_link,
        classification.has_contact_info,
        classification.has_deadline,
        classification.completeness_score,
        classification.language,
        classification.uses_emoji,
        classification.uses_formal_pronouns,
        classification.classified_at,
        classification.classification_method
    ])


# ============================================================
# ACCOUNT PROFIEL BEREKENING
# ============================================================

def calculate_account_comm_profile(account_id: str, db: Optional[Database] = None) -> AccountCommProfile:
    """
    Bereken geaggregeerd communicatieprofiel voor een account.
    """
    db = db or get_connection()

    # Haal classificaties op
    rows = db.fetchall("""
        SELECT
            pc.content_type, pc.tone_formality, pc.tone_service_oriented,
            pc.has_call_to_action, pc.has_link, pc.completeness_score, pc.timing_class
        FROM post_classification pc
        JOIN posts p ON pc.post_id = p.id
        WHERE p.account_id = ?
    """, [account_id])

    if not rows:
        return AccountCommProfile(account_id=account_id, last_calculated=datetime.now())

    total = len(rows)

    # Tel content types
    content_counts = {'procedureel': 0, 'wijziging': 0, 'waarschuwing': 0,
                      'promotioneel': 0, 'overig': 0}
    formality_scores = []
    cta_count = 0
    link_count = 0
    completeness_scores = []
    proactive_count = 0

    for row in rows:
        content_type = row[0] or 'overig'
        if content_type in content_counts:
            content_counts[content_type] += 1

        if row[1] is not None:
            formality_scores.append(float(row[1]))

        if row[3]:  # has_call_to_action
            cta_count += 1
        if row[4]:  # has_link
            link_count += 1
        if row[5] is not None:
            completeness_scores.append(float(row[5]))

        timing = row[6]
        if timing in ['proactief', 'adequaat']:
            proactive_count += 1

    # Bereken percentages
    avg_formality = sum(formality_scores) / len(formality_scores) if formality_scores else 0.5
    dominant_tone = "formeel" if avg_formality >= 0.5 else "informeel"

    profile = AccountCommProfile(
        account_id=account_id,
        total_posts_analyzed=total,
        pct_procedural=round(content_counts['procedureel'] / total * 100, 2) if total > 0 else 0,
        pct_wijziging=round(content_counts['wijziging'] / total * 100, 2) if total > 0 else 0,
        pct_waarschuwing=round(content_counts['waarschuwing'] / total * 100, 2) if total > 0 else 0,
        pct_promotional=round(content_counts['promotioneel'] / total * 100, 2) if total > 0 else 0,
        pct_interaction=0.0,  # TODO: uit comment analyse
        avg_days_advance=None,  # TODO: uit timing analyse
        pct_proactive=round(proactive_count / total * 100, 2) if total > 0 else 0,
        response_rate=None,  # TODO: uit comment analyse
        avg_response_hours=None,  # TODO: uit comment analyse
        dominant_tone=dominant_tone,
        avg_formality_score=round(avg_formality, 2),
        pct_with_cta=round(cta_count / total * 100, 2) if total > 0 else 0,
        pct_with_link=round(link_count / total * 100, 2) if total > 0 else 0,
        avg_completeness=round(sum(completeness_scores) / len(completeness_scores), 2) if completeness_scores else 0,
        last_calculated=datetime.now()
    )

    # Opslaan in database
    save_account_comm_profile(profile, db)

    return profile


def save_account_comm_profile(profile: AccountCommProfile, db: Database):
    """Sla account communicatieprofiel op."""
    db.execute("""
        INSERT INTO account_comm_profile (
            account_id, total_posts_analyzed, pct_procedural, pct_wijziging,
            pct_waarschuwing, pct_promotional, pct_interaction, avg_days_advance,
            pct_proactive, response_rate, avg_response_hours, dominant_tone,
            avg_formality_score, pct_with_cta, pct_with_link, avg_completeness,
            last_calculated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (account_id) DO UPDATE SET
            total_posts_analyzed = EXCLUDED.total_posts_analyzed,
            pct_procedural = EXCLUDED.pct_procedural,
            pct_wijziging = EXCLUDED.pct_wijziging,
            pct_waarschuwing = EXCLUDED.pct_waarschuwing,
            pct_promotional = EXCLUDED.pct_promotional,
            pct_interaction = EXCLUDED.pct_interaction,
            avg_days_advance = EXCLUDED.avg_days_advance,
            pct_proactive = EXCLUDED.pct_proactive,
            response_rate = EXCLUDED.response_rate,
            avg_response_hours = EXCLUDED.avg_response_hours,
            dominant_tone = EXCLUDED.dominant_tone,
            avg_formality_score = EXCLUDED.avg_formality_score,
            pct_with_cta = EXCLUDED.pct_with_cta,
            pct_with_link = EXCLUDED.pct_with_link,
            avg_completeness = EXCLUDED.avg_completeness,
            last_calculated = EXCLUDED.last_calculated
    """, [
        profile.account_id,
        profile.total_posts_analyzed,
        profile.pct_procedural,
        profile.pct_wijziging,
        profile.pct_waarschuwing,
        profile.pct_promotional,
        profile.pct_interaction,
        profile.avg_days_advance,
        profile.pct_proactive,
        profile.response_rate,
        profile.avg_response_hours,
        profile.dominant_tone,
        profile.avg_formality_score,
        profile.pct_with_cta,
        profile.pct_with_link,
        profile.avg_completeness,
        profile.last_calculated
    ])


# ============================================================
# QUERIES
# ============================================================

def get_posts_for_classification(account_id: Optional[str] = None,
                                  limit: int = 100,
                                  db: Optional[Database] = None) -> List[Post]:
    """Haal posts op die nog niet geclassificeerd zijn."""
    db = db or get_connection()

    query = """
        SELECT p.id, p.account_id, p.platform_post_id, p.posted_at, p.content_type,
               p.likes, p.comments, p.shares, p.views, p.url, p.caption_snippet,
               p.hashtags, p.collected_at, p.last_updated
        FROM posts p
        LEFT JOIN post_classification pc ON p.id = pc.post_id
        WHERE pc.post_id IS NULL
    """
    params = []

    if account_id:
        query += " AND p.account_id = ?"
        params.append(account_id)

    query += " ORDER BY p.posted_at DESC LIMIT ?"
    params.append(limit)

    rows = db.fetchall(query, params)

    posts = []
    for row in rows:
        posts.append(Post(
            id=row[0],
            account_id=row[1],
            platform_post_id=row[2],
            posted_at=row[3],
            content_type=row[4],
            likes=row[5],
            comments=row[6],
            shares=row[7],
            views=row[8],
            url=row[9],
            caption_snippet=row[10],
            hashtags=row[11],
            collected_at=row[12],
            last_updated=row[13]
        ))

    return posts


def get_classification_summary(db: Optional[Database] = None) -> Dict[str, Any]:
    """Haal samenvatting van classificaties op."""
    db = db or get_connection()

    # Totaal geclassificeerd
    total = db.fetchone("SELECT COUNT(*) FROM post_classification")[0]

    # Per content type
    content_types = db.fetchall("""
        SELECT content_type, COUNT(*) as count
        FROM post_classification
        GROUP BY content_type
        ORDER BY count DESC
    """)

    # Gemiddelde scores
    averages = db.fetchone("""
        SELECT
            AVG(tone_formality),
            AVG(CASE WHEN has_call_to_action THEN 1 ELSE 0 END),
            AVG(CASE WHEN has_link THEN 1 ELSE 0 END),
            AVG(completeness_score)
        FROM post_classification
    """)

    return {
        "total_classified": total,
        "by_content_type": {row[0]: row[1] for row in content_types},
        "avg_formality": round(averages[0] or 0, 2),
        "pct_with_cta": round((averages[1] or 0) * 100, 1),
        "pct_with_link": round((averages[2] or 0) * 100, 1),
        "avg_completeness": round(averages[3] or 0, 2),
    }


# ============================================================
# COMMENT ANALYSE
# ============================================================

def analyze_comment(comment_text: str) -> Dict[str, Any]:
    """
    Analyseer een individuele comment.
    Returns dict met is_question, question_type, sentiment.
    """
    if not comment_text:
        return {
            "is_question": False,
            "question_type": None,
            "sentiment": Sentiment.NEUTRAAL.value,
        }

    return {
        "is_question": is_question(comment_text),
        "question_type": classify_question_type(comment_text),
        "sentiment": classify_sentiment(comment_text),
    }


def analyze_post_comments(post_id: str, account_handle: str,
                          db: Optional[Database] = None) -> Dict[str, Any]:
    """
    Analyseer alle comments op een post.
    Berekent response rate en response tijd.
    """
    db = db or get_connection()

    comments = db.fetchall("""
        SELECT id, comment_text, author_handle, is_from_account, posted_at
        FROM post_comments
        WHERE post_id = ?
        ORDER BY posted_at
    """, [post_id])

    if not comments:
        return {
            "total_comments": 0,
            "questions": 0,
            "answered": 0,
            "response_rate": None,
            "avg_response_hours": None,
        }

    questions = []
    account_responses = []
    account_handle_lower = account_handle.lower()

    for comment in comments:
        comment_id, text, author, is_from_account, posted_at = comment

        if is_from_account or (author and author.lower() == account_handle_lower):
            account_responses.append({
                "id": comment_id,
                "posted_at": posted_at,
            })
        elif text and is_question(text):
            questions.append({
                "id": comment_id,
                "text": text,
                "posted_at": posted_at,
                "question_type": classify_question_type(text),
            })

    # Bepaal hoeveel vragen beantwoord zijn
    answered_count = 0
    response_times = []

    for q in questions:
        # Zoek een antwoord na de vraag
        for r in account_responses:
            if r["posted_at"] and q["posted_at"] and r["posted_at"] > q["posted_at"]:
                answered_count += 1
                # Bereken response tijd
                delta = r["posted_at"] - q["posted_at"]
                response_times.append(delta.total_seconds() / 3600)  # uren
                break

    response_rate = (answered_count / len(questions) * 100) if questions else None
    avg_response = sum(response_times) / len(response_times) if response_times else None

    return {
        "total_comments": len(comments),
        "questions": len(questions),
        "answered": answered_count,
        "response_rate": round(response_rate, 1) if response_rate else None,
        "avg_response_hours": round(avg_response, 1) if avg_response else None,
    }


def calculate_account_interaction_stats(account_id: str,
                                         db: Optional[Database] = None) -> Dict[str, Any]:
    """
    Bereken interactie statistieken voor een account.
    """
    db = db or get_connection()

    # Haal account handle op
    account = db.fetchone("""
        SELECT handle FROM accounts WHERE id = ?
    """, [account_id])

    if not account:
        return {}

    handle = account[0]

    # Haal alle posts met comments op
    posts_with_comments = db.fetchall("""
        SELECT DISTINCT p.id
        FROM posts p
        JOIN post_comments c ON p.id = c.post_id
        WHERE p.account_id = ?
    """, [account_id])

    total_questions = 0
    total_answered = 0
    all_response_times = []

    for (post_id,) in posts_with_comments:
        stats = analyze_post_comments(post_id, handle, db)
        total_questions += stats["questions"]
        total_answered += stats["answered"]
        if stats["avg_response_hours"]:
            all_response_times.append(stats["avg_response_hours"])

    response_rate = (total_answered / total_questions * 100) if total_questions > 0 else None
    avg_response = sum(all_response_times) / len(all_response_times) if all_response_times else None

    return {
        "posts_with_comments": len(posts_with_comments),
        "total_questions": total_questions,
        "total_answered": total_answered,
        "response_rate": round(response_rate, 1) if response_rate else None,
        "avg_response_hours": round(avg_response, 1) if avg_response else None,
    }


def save_comment_analysis(analysis: CommentAnalysis, db: Database):
    """Sla comment analyse op."""
    db.execute("""
        INSERT INTO comment_analysis
        (id, post_id, comment_id, is_question, question_type, has_response,
         response_type, response_time_hours, sentiment, analyzed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            is_question = EXCLUDED.is_question,
            question_type = EXCLUDED.question_type,
            has_response = EXCLUDED.has_response,
            response_type = EXCLUDED.response_type,
            response_time_hours = EXCLUDED.response_time_hours,
            sentiment = EXCLUDED.sentiment,
            analyzed_at = EXCLUDED.analyzed_at
    """, [
        analysis.id,
        analysis.post_id,
        analysis.comment_id,
        analysis.is_question,
        analysis.question_type,
        analysis.has_response,
        analysis.response_type,
        analysis.response_time_hours,
        analysis.sentiment,
        analysis.analyzed_at
    ])
