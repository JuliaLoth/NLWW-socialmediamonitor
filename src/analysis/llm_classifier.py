"""
LLM Classificatie Module

Gebruikt Claude API voor geavanceerde classificatie van social media posts
wanneer rule-based classificatie niet voldoende is.
"""
import os
import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass

try:
    import anthropic
except ImportError:
    anthropic = None

from ..database.connection import Database, get_connection
from ..database.models import Post, PostClassification, generate_uuid

logger = logging.getLogger(__name__)


# ============================================================
# CLAUDE API CLIENT
# ============================================================

class ClaudeClassifier:
    """
    Classifier die Claude API gebruikt voor geavanceerde post analyse.
    """

    def __init__(self, api_key: Optional[str] = None):
        if anthropic is None:
            raise ImportError(
                "anthropic package niet geinstalleerd. "
                "Installeer met: pip install anthropic"
            )

        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY niet gevonden. "
                "Stel de environment variable in of geef api_key mee."
            )

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = "claude-sonnet-4-20250514"  # Sonnet voor betere classificatie

    def classify_post(self, post_text: str) -> Dict[str, Any]:
        """
        Classificeer een enkele post met Claude.
        """
        if not post_text or len(post_text.strip()) < 10:
            return self._empty_classification()

        prompt = self._build_classification_prompt(post_text)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )

            # Parse JSON response
            result_text = response.content[0].text
            return self._parse_response(result_text)

        except Exception as e:
            logger.error(f"Claude classificatie fout: {e}")
            return self._empty_classification()

    def classify_batch(self, posts: List[Dict[str, str]], batch_size: int = 5) -> List[Dict[str, Any]]:
        """
        Classificeer meerdere posts in batches.
        """
        results = []

        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            batch_results = self._classify_batch_internal(batch)
            results.extend(batch_results)

        return results

    def _classify_batch_internal(self, posts: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Interne batch classificatie.
        """
        if not posts:
            return []

        prompt = self._build_batch_prompt(posts)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4000,
                timeout=120.0,  # 2 minuten timeout
                messages=[{"role": "user", "content": prompt}]
            )

            result_text = response.content[0].text
            return self._parse_batch_response(result_text, len(posts))

        except Exception as e:
            logger.error(f"Claude batch classificatie fout: {e}")
            return [self._empty_classification() for _ in posts]

    def _build_classification_prompt(self, post_text: str, platform: str = "", account: str = "", date: str = "") -> str:
        """Bouw prompt voor enkele post classificatie."""
        return f"""Je bent een content-analist gespecialiseerd in overheidscommunicatie. Analyseer onderstaande social media post van een diplomatieke missie of overheidsinstantie.

## POST
Platform: {platform or "onbekend"}
Account: {account or "onbekend"}
Datum: {date or "onbekend"}
Tekst: \"\"\"
{post_text}
\"\"\"

## ANALYSE-INSTRUCTIES

Beoordeel de post op onderstaande dimensies. Geef je antwoord als JSON.

### 1. content_type (string)
Kies de primaire categorie:
- "procedureel" → procedures, documenten, aanvraagprocessen, vereisten
- "wijziging" → beleidsveranderingen, nieuwe regels, aangepaste openingstijden
- "waarschuwing" → sluitingen, vertragingen, veiligheidswaarschuwingen, urgente mededelingen
- "promotioneel" → evenementen, culturele uitwisseling, nationale feestdagen, positief nieuws
- "interactie" → vragen aan publiek, polls, felicitaties, condoleances
- "diplomatiek_nieuws" → bilaterale ontmoetingen, staatsbezoeken, persverklaringen, officiële standpunten, verdragen, ambassade-nieuws
- "overig" → past in geen van bovenstaande

### 2. tone_formality (float 0.0-1.0)
- 0.0-0.3: informeel (emoji's, "je/jij", spreektaal, uitroeptekens)
- 0.4-0.6: neutraal (mix van elementen)
- 0.7-1.0: formeel ("u", volledige zinnen, ambtelijke formuleringen, geen emoji's)

### 3. communication_orientation (string)
- "service" → helpend, vanuit burgerperspectief, praktische informatie
- "zender" → institutioneel, vanuit organisatieperspectief, representatief

### 4. has_call_to_action (boolean)
True als expliciet een actie wordt gevraagd (klik, bel, bezoek, reageer, deel).

### 5. information_completeness (object)
Geef per element aan of het aanwezig is (true/false):
- "wie": doelgroep duidelijk
- "wat": onderwerp/actie duidelijk
- "wanneer": timing/deadline genoemd
- "hoe": vervolgstappen uitgelegd
- "score": som van aanwezige elementen / 4

### 6. detected_deadline (string of null)
Exacte datum in ISO-formaat (YYYY-MM-DD) indien genoemd, anders null.

### 7. language (string)
ISO 639-1 taalcode van de post (nl, en, de, fr, ar, etc.)

## OUTPUT FORMAT
```json
{{
  "content_type": "",
  "tone_formality": 0.0,
  "communication_orientation": "",
  "has_call_to_action": false,
  "information_completeness": {{
    "wie": false,
    "wat": false,
    "wanneer": false,
    "hoe": false,
    "score": 0.0
  }},
  "detected_deadline": null,
  "language": "",
  "confidence": 0.0,
  "notes": ""
}}
```

Bij twijfel tussen categorieën: kies de meest specifieke. Voeg in "notes" korte toelichting toe bij edge cases."""

    def _build_batch_prompt(self, posts: List[Dict[str, str]]) -> str:
        """Bouw prompt voor batch classificatie."""
        posts_text = "\n\n".join([
            f"POST {i+1}:\n{p.get('text', '')}"
            for i, p in enumerate(posts)
        ])

        return f"""Je bent een content-analist gespecialiseerd in overheidscommunicatie. Analyseer deze {len(posts)} social media posts van diplomatieke missies of overheidsinstanties.

{posts_text}

Voor ELKE post, classificeer op:

1. content_type: Kies uit:
   - "procedureel" → procedures, documenten, aanvraagprocessen, vereisten
   - "wijziging" → beleidsveranderingen, nieuwe regels, aangepaste openingstijden
   - "waarschuwing" → sluitingen, vertragingen, veiligheidswaarschuwingen, urgente mededelingen
   - "promotioneel" → evenementen, culturele uitwisseling, nationale feestdagen, positief nieuws
   - "interactie" → vragen aan publiek, polls, felicitaties, condoleances
   - "diplomatiek_nieuws" → bilaterale ontmoetingen, staatsbezoeken, persverklaringen, officiële standpunten
   - "overig" → past in geen van bovenstaande

2. tone_formality: 0.0-1.0
   - 0.0-0.3: informeel (emoji's, spreektaal)
   - 0.4-0.6: neutraal
   - 0.7-1.0: formeel (ambtelijk, "u")

3. communication_orientation: "service" (helpend, burgerperspectief) of "zender" (institutioneel)

4. has_call_to_action: true/false

5. completeness_score: 0.0-1.0 (wie+wat+wanneer+hoe, 0.25 per element)

6. language: ISO 639-1 taalcode (nl, en, ar, etc.)

Antwoord ALLEEN in JSON array formaat:
[
  {{"post_index": 1, "content_type": "...", "tone_formality": 0.5, "communication_orientation": "...", "has_call_to_action": false, "completeness_score": 0.5, "language": "en"}},
  {{"post_index": 2, ...}},
  ...
]"""

    def _parse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse JSON response van Claude."""
        try:
            # Probeer JSON te extraheren
            start = response_text.find('{')
            end = response_text.rfind('}') + 1
            if start >= 0 and end > start:
                json_str = response_text[start:end]
                return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Kon JSON niet parsen: {e}")

        return self._empty_classification()

    def _parse_batch_response(self, response_text: str, expected_count: int) -> List[Dict[str, Any]]:
        """Parse batch JSON response."""
        try:
            start = response_text.find('[')
            end = response_text.rfind(']') + 1
            if start >= 0 and end > start:
                json_str = response_text[start:end]
                results = json.loads(json_str)
                if len(results) == expected_count:
                    return results
        except json.JSONDecodeError as e:
            logger.warning(f"Kon batch JSON niet parsen: {e}")

        return [self._empty_classification() for _ in range(expected_count)]

    def _empty_classification(self) -> Dict[str, Any]:
        """Return lege classificatie bij fouten."""
        return {
            "content_type": "overig",
            "tone_formality": 0.5,
            "communication_orientation": "zender",
            "has_call_to_action": False,
            "completeness_score": 0.0,
            "detected_deadline": None,
            "language": "en",
        }


# ============================================================
# INTEGRATIE FUNCTIES
# ============================================================

def classify_post_with_llm(post: Post, classifier: Optional[ClaudeClassifier] = None) -> PostClassification:
    """
    Classificeer een post met Claude LLM.
    """
    if classifier is None:
        classifier = ClaudeClassifier()

    text = post.caption_snippet or ""
    result = classifier.classify_post(text)

    return PostClassification(
        post_id=post.id,
        content_type=result.get("content_type", "overig"),
        tone_formality=result.get("tone_formality", 0.5),
        tone_service_oriented=result.get("is_service_oriented", False),
        tone_empathetic=None,
        tone_proactive=result.get("has_call_to_action", False),
        days_advance=None,  # Zou berekend kunnen worden uit detected_deadline
        timing_class="nvt",
        has_call_to_action=result.get("has_call_to_action", False),
        has_link=bool(result.get("detected_deadline")),
        has_contact_info=None,
        has_deadline=bool(result.get("detected_deadline")),
        completeness_score=result.get("completeness_score", 0.0),
        language=None,
        uses_emoji=None,
        uses_formal_pronouns=None,
        classified_at=datetime.now(),
        classification_method="llm_claude"
    )


def classify_posts_with_llm(posts: List[Post],
                            db: Optional[Database] = None,
                            api_key: Optional[str] = None) -> List[PostClassification]:
    """
    Classificeer meerdere posts met Claude LLM.
    Bespaart API calls door batching.
    """
    from .communication import save_post_classification

    db = db or get_connection()
    classifier = ClaudeClassifier(api_key=api_key)

    # Bereid posts voor
    post_data = [{"id": p.id, "text": p.caption_snippet or ""} for p in posts]

    # Batch classificeer
    results = classifier.classify_batch(post_data)

    classifications = []
    for post, result in zip(posts, results):
        classification = PostClassification(
            post_id=post.id,
            content_type=result.get("content_type", "overig"),
            tone_formality=result.get("tone_formality", 0.5),
            tone_service_oriented=result.get("is_service_oriented", False),
            tone_empathetic=None,
            tone_proactive=result.get("has_call_to_action", False),
            days_advance=None,
            timing_class="nvt",
            has_call_to_action=result.get("has_call_to_action", False),
            has_link=None,
            has_contact_info=None,
            has_deadline=bool(result.get("detected_deadline")),
            completeness_score=result.get("completeness_score", 0.0),
            language=None,
            uses_emoji=None,
            uses_formal_pronouns=None,
            classified_at=datetime.now(),
            classification_method="llm_claude"
        )
        classifications.append(classification)
        save_post_classification(classification, db)

    return classifications


def is_llm_available() -> bool:
    """Check of LLM classificatie beschikbaar is."""
    if anthropic is None:
        return False
    return bool(os.getenv("ANTHROPIC_API_KEY"))
