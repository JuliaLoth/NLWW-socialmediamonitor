"""
Streamlit Dashboard - MFA Social Media Monitor
Monitoring van Ministeries van Buitenlandse Zaken op social media

Start met: streamlit run src/outputs/dashboard/app.py
"""
import streamlit as st
from pathlib import Path
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import re
from collections import Counter

# Wordcloud import (optioneel)
try:
    from wordcloud import WordCloud
    import matplotlib.pyplot as plt
    WORDCLOUD_AVAILABLE = True
except ImportError:
    WORDCLOUD_AVAILABLE = False

# TextBlob voor sentiment (optioneel)
try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False

# Deep Translator voor vertaling (optioneel)
try:
    from deep_translator import GoogleTranslator
    TRANSLATOR_AVAILABLE = True
except ImportError:
    TRANSLATOR_AVAILABLE = False

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.database.connection import get_readonly_connection
from src.database.queries import AccountQueries, MetricsQueries, PostQueries
from src.config.settings import COUNTRY_NAMES_NL, PLATFORM_NAMES_NL

# Page config
st.set_page_config(
    page_title="MFA Social Media Monitor",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for clean design
st.markdown("""
<style>
    .main-header {
        font-size: 2rem;
        font-weight: 700;
        color: #1e3a5f;
        margin-bottom: 0.25rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #666;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #667eea;
    }
    .definition-box {
        background: #e8f4f8;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .example-box {
        background: #f0f7f0;
        padding: 0.75rem;
        border-radius: 0.25rem;
        border-left: 3px solid #4caf50;
        margin: 0.5rem 0;
        font-style: italic;
    }
    .warning-box {
        background: #fff3e0;
        padding: 0.75rem;
        border-radius: 0.25rem;
        border-left: 3px solid #ff9800;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
    }
</style>
""", unsafe_allow_html=True)


def main():
    """Main dashboard."""
    db = get_readonly_connection()

    # Header
    st.markdown('<p class="main-header">MFA Social Media Monitor</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Analyse van communicatiestijl van Ministeries van Buitenlandse Zaken</p>', unsafe_allow_html=True)

    # Sidebar navigation - Logical structure based on best practices
    st.sidebar.title("Navigatie")
    page = st.sidebar.radio(
        "Ga naar",
        [
            "📊 Samenvatting",
            "🇳🇱 Nederland Wereldwijd",
            "📈 Kwantitatief",
            "💬 Kwalitatief",
            "🌍 Per Land",
            "📖 Onderzoeksopzet",
            "💾 Export"
        ],
        label_visibility="collapsed"
    )

    # Quick stats in sidebar
    st.sidebar.markdown("---")
    st.sidebar.caption("**Data Overzicht**")

    total_posts = db.fetchone("SELECT COUNT(*) FROM posts")[0]
    total_accounts = db.fetchone("SELECT COUNT(*) FROM accounts WHERE status='active'")[0]
    total_classified = db.fetchone("SELECT COUNT(*) FROM post_classification")[0]

    st.sidebar.metric("Posts verzameld", total_posts)
    st.sidebar.metric("Accounts", total_accounts)
    st.sidebar.metric("Geclassificeerd", total_classified)

    # Route to pages
    if page == "📊 Samenvatting":
        show_executive_summary(db)
    elif page == "🇳🇱 Nederland Wereldwijd":
        show_nederland_overview(db)
    elif page == "📈 Kwantitatief":
        show_quantitative(db)
    elif page == "💬 Kwalitatief":
        show_qualitative(db)
    elif page == "🌍 Per Land":
        show_country_detail(db)
    elif page == "📖 Onderzoeksopzet":
        show_methodology(db)
    elif page == "💾 Export":
        show_export(db)


def show_nederland_overview(db):
    """Nederland Wereldwijd overzicht - consistente stijl met rest dashboard."""
    st.header("🇳🇱 Nederland")
    st.caption("Ministerie van Buitenlandse Zaken - Social Media Analyse")

    # Get all Dutch Instagram accounts with stats (alleen actieve)
    nl_data = db.fetchall("""
        SELECT a.platform, a.handle, a.display_name,
               COUNT(p.id) as posts,
               SUM(p.likes) as likes,
               SUM(p.comments) as comments,
               SUM(p.shares) as shares
        FROM accounts a
        LEFT JOIN posts p ON a.id = p.account_id
        WHERE a.country = 'nederland' AND a.platform = 'instagram' AND a.status = 'active'
        GROUP BY a.platform, a.handle, a.display_name
        ORDER BY SUM(p.likes) DESC
    """)

    # Totaal metrics
    total_posts = sum(d[3] or 0 for d in nl_data)
    total_likes = sum(d[4] or 0 for d in nl_data)
    total_comments = sum(d[5] or 0 for d in nl_data)
    total_engagement = total_likes + total_comments

    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Posts", f"{total_posts:,}")
    with col2:
        st.metric("❤️ Likes", f"{total_likes:,}")
    with col3:
        st.metric("💬 Comments", f"{total_comments:,}")
    with col4:
        st.metric("📈 Engagement", f"{total_engagement:,}")

    st.markdown("---")

    # Per Account
    st.subheader("📸 Instagram Accounts")

    if nl_data:
        cols = st.columns(len(nl_data))
        for i, acc in enumerate(nl_data):
            handle, posts, likes, comments = acc[1], acc[3] or 0, acc[4] or 0, acc[5] or 0
            with cols[i]:
                st.metric(f"@{handle}", f"{posts} posts", f"❤️ {likes:,} | 💬 {comments:,}")
    else:
        st.info("Geen Instagram data")

    st.markdown("---")

    # Communicatieprofiel
    st.subheader("📊 Communicatieprofiel")

    # Get communication profile for @nederlandwereldwijd
    nl_profile = db.fetchone("""
        SELECT cp.pct_procedural, cp.pct_promotional, cp.pct_wijziging, cp.pct_waarschuwing,
               cp.avg_formality_score, cp.pct_with_cta, cp.avg_completeness,
               cp.total_posts_analyzed
        FROM account_comm_profile cp
        JOIN accounts a ON cp.account_id = a.id
        WHERE a.handle = 'nederlandwereldwijd' AND a.country = 'nederland'
    """)

    if nl_profile:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Content Verdeling @nederlandwereldwijd**")

            categories = ['Procedures', 'Promoties', 'Wijzigingen', 'Waarschuwingen']
            values = [
                nl_profile[0] or 0,
                nl_profile[1] or 0,
                nl_profile[2] or 0,
                nl_profile[3] or 0
            ]

            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=values + [values[0]],
                theta=categories + [categories[0]],
                fill='toself',
                name='@nederlandwereldwijd',
                line_color='#667eea',
                fillcolor='rgba(102, 126, 234, 0.3)'
            ))
            fig.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                showlegend=False,
                height=300,
                margin=dict(t=30, b=30, l=50, r=50)
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Statistieken**")

            formality = nl_profile[4] or 0.5
            cta_pct = nl_profile[5] or 0
            completeness = nl_profile[6] or 0
            total_analyzed = nl_profile[7] or 0

            if formality >= 0.7:
                tone_label = "Formeel"
            elif formality >= 0.4:
                tone_label = "Neutraal"
            else:
                tone_label = "Informeel"

            st.metric("Formaliteit", f"{formality:.2f}", tone_label)
            st.metric("Call-to-Action", f"{cta_pct:.0f}%")
            st.metric("Volledigheid", f"{completeness:.0%}")
            st.caption(f"Gebaseerd op {total_analyzed} geanalyseerde posts")

    else:
        st.info("Nog geen communicatieprofiel beschikbaar. Voer eerst de LLM classificatie uit.")

    st.markdown("---")

    # Recente posts
    st.subheader("📝 Recente Posts")

    recent_posts = db.fetchall("""
        SELECT p.caption_snippet, p.likes, p.comments, p.posted_at, a.handle, a.platform
        FROM posts p
        JOIN accounts a ON p.account_id = a.id
        WHERE a.country = 'nederland' AND a.platform = 'instagram' AND a.status = 'active'
        ORDER BY p.posted_at DESC
        LIMIT 10
    """)

    if recent_posts:
        for post in recent_posts:
            caption = post[0][:100] + "..." if post[0] and len(post[0]) > 100 else post[0]
            platform_icon = "📸" if post[5] == "instagram" else "📘"

            with st.expander(f"{platform_icon} @{post[4]} - ❤️ {post[1] or 0} | 💬 {post[2] or 0}"):
                st.write(caption or "Geen tekst beschikbaar")
                if post[3]:
                    st.caption(f"Geplaatst: {str(post[3])[:10]}")
    else:
        st.info("Geen posts beschikbaar")


def show_executive_summary(db):
    """Executive summary - key insights at a glance."""
    st.header("Management Samenvatting")
    st.caption("De belangrijkste inzichten voor besluitvorming")

    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)

    # Get data (excl. Nederland voor vergelijking)
    total_posts = db.fetchone("""
        SELECT COUNT(*) FROM posts p
        JOIN accounts a ON p.account_id = a.id
        WHERE a.status = 'active'
    """)[0]
    total_likes = db.fetchone("""
        SELECT SUM(p.likes) FROM posts p
        JOIN accounts a ON p.account_id = a.id
        WHERE a.status = 'active'
    """)[0] or 0
    total_comments = db.fetchone("""
        SELECT SUM(p.comments) FROM posts p
        JOIN accounts a ON p.account_id = a.id
        WHERE a.status = 'active'
    """)[0] or 0
    total_countries = db.fetchone("SELECT COUNT(DISTINCT country) FROM accounts WHERE country != 'nederland'")[0]

    with col1:
        st.metric("📊 Posts geanalyseerd", f"{total_posts:,}")
    with col2:
        st.metric("❤️ Totaal Likes", f"{total_likes:,}")
    with col3:
        st.metric("💬 Totaal Comments", f"{total_comments:,}")
    with col4:
        st.metric("🌍 Landen vergeleken", total_countries)

    st.markdown("---")

    # TOP PERFORMERS - meest relevante inzichten
    st.subheader("🏆 Top Performers per Metric")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Hoogste Engagement (gem. likes per post)**")
        top_engagement = db.fetchall("""
            SELECT a.country,
                   ROUND(AVG(p.likes), 0) as avg_likes,
                   COUNT(p.id) as posts
            FROM posts p
            JOIN accounts a ON p.account_id = a.id
            WHERE a.status = 'active' AND a.platform = 'instagram' AND p.likes > 0
            GROUP BY a.country
            HAVING COUNT(p.id) >= 10
            ORDER BY AVG(p.likes) DESC
            LIMIT 5
        """)

        if top_engagement:
            for i, row in enumerate(top_engagement, 1):
                country = COUNTRY_NAMES_NL.get(row[0], row[0])
                st.write(f"{i}. **{country}** - {int(row[1]):,} gem. likes ({row[2]} posts)")
        else:
            st.info("Onvoldoende data")

    with col2:
        st.markdown("**Meest Actief (aantal posts)**")
        top_active = db.fetchall("""
            SELECT a.country, COUNT(p.id) as posts
            FROM posts p
            JOIN accounts a ON p.account_id = a.id
            WHERE a.status = 'active' AND a.platform = 'instagram'
            GROUP BY a.country
            ORDER BY COUNT(p.id) DESC
            LIMIT 5
        """)

        if top_active:
            for i, row in enumerate(top_active, 1):
                country = COUNTRY_NAMES_NL.get(row[0], row[0])
                st.write(f"{i}. **{country}** - {row[1]} posts")
        else:
            st.info("Onvoldoende data")

    st.markdown("---")

    # ENGAGEMENT VERGELIJKING
    st.subheader("📊 Engagement Vergelijking per Land")

    engagement_data = db.fetchall("""
        SELECT a.country,
               COUNT(p.id) as posts,
               ROUND(AVG(p.likes), 0) as avg_likes,
               ROUND(AVG(p.comments), 0) as avg_comments
        FROM posts p
        JOIN accounts a ON p.account_id = a.id
        WHERE a.status = 'active'
        GROUP BY a.country
        HAVING COUNT(p.id) >= 5
        ORDER BY AVG(p.likes) DESC
    """)

    if engagement_data:
        df = pd.DataFrame(engagement_data, columns=["Land", "Posts", "Gem. Likes", "Gem. Comments"])
        df["Land"] = df["Land"].apply(lambda x: COUNTRY_NAMES_NL.get(x, x))

        fig = px.bar(df, x="Land", y="Gem. Likes",
                    color="Gem. Likes",
                    color_continuous_scale="Blues",
                    text="Gem. Likes")
        fig.update_traces(texttemplate='%{text:.0f}', textposition='outside')
        fig.update_layout(
            xaxis_tickangle=-45,
            yaxis_title="Gemiddelde Likes per Post",
            showlegend=False,
            margin=dict(b=100)
        )
        st.plotly_chart(fig, use_container_width=True)



def show_quantitative(db):
    """Quantitative metrics - engagement, followers, posts."""
    st.header("Kwantitatieve Analyse")
    st.caption("Meetbare statistieken: interactie, bereik en activiteit")

    # Explanation
    with st.expander("ℹ️ Wat zijn kwantitatieve metrics?", expanded=False):
        st.markdown("""
        **Kwantitatieve metrics** zijn meetbare getallen die de prestaties van social media accounts weergeven:

        | Metric | Definitie | Waarom belangrijk |
        |--------|-----------|-------------------|
        | **Likes** | Aantal keer dat gebruikers op 'vind ik leuk' klikken | Meet waardering voor content |
        | **Comments** | Aantal reacties op posts | Meet betrokkenheid en interactie |
        | **Engagement Rate** | (Likes + Comments) / Volgers × 100 | Vergelijkbare interactie ongeacht accountgrootte |
        | **Posts** | Aantal gepubliceerde berichten | Meet activiteit en consistentie |
        """)

    st.markdown("---")

    # Overall stats
    st.subheader("Totaaloverzicht")

    # Get aggregated data per account - Instagram
    account_stats = db.fetchall("""
        SELECT
            a.country, a.handle, a.platform,
            COUNT(p.id) as posts,
            SUM(p.likes) as total_likes,
            SUM(p.comments) as total_comments,
            AVG(p.likes) as avg_likes,
            AVG(p.comments) as avg_comments
        FROM accounts a
        LEFT JOIN posts p ON a.id = p.account_id
        WHERE a.platform = 'instagram' AND a.status = 'active'
        GROUP BY a.id, a.country, a.handle, a.platform
        HAVING COUNT(p.id) > 0
        ORDER BY total_likes DESC
    """)

    if account_stats:
        df = pd.DataFrame(account_stats, columns=[
            "Land", "Handle", "Platform", "Posts", "Totaal Likes", "Totaal Comments",
            "Gem. Likes", "Gem. Comments"
        ])
        df["Land"] = df["Land"].apply(lambda x: COUNTRY_NAMES_NL.get(x, x))
        df["Platform"] = df["Platform"].apply(lambda x: "📸" if x == "instagram" else "📘")
        df["Engagement"] = (df["Totaal Likes"].fillna(0) + df["Totaal Comments"].fillna(0)).astype(int)
        df["Totaal Likes"] = df["Totaal Likes"].fillna(0).astype(int)
        df["Totaal Comments"] = df["Totaal Comments"].fillna(0).astype(int)
        df["Gem. Likes"] = df["Gem. Likes"].fillna(0).round(0).astype(int)
        df["Gem. Comments"] = df["Gem. Comments"].fillna(0).round(0).astype(int)

        # Top metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            top_likes = df.loc[df["Totaal Likes"].idxmax()]
            st.metric(
                "Meeste Likes",
                f"{int(top_likes['Totaal Likes']):,}",
                f"{top_likes['Land']}"
            )
        with col2:
            top_comments = df.loc[df["Totaal Comments"].idxmax()]
            st.metric(
                "Meeste Comments",
                f"{int(top_comments['Totaal Comments']):,}",
                f"{top_comments['Land']}"
            )
        with col3:
            top_engagement = df.loc[df["Engagement"].idxmax()]
            st.metric(
                "Hoogste Engagement",
                f"{int(top_engagement['Engagement']):,}",
                f"{top_engagement['Land']}"
            )

        st.markdown("---")

        # Bar chart - Engagement per country
        st.subheader("Engagement per Land")

        fig = px.bar(df.sort_values("Engagement", ascending=True),
                    x="Engagement", y="Land",
                    orientation='h',
                    color="Engagement",
                    color_continuous_scale="Blues",
                    labels={"Engagement": "Totaal (Likes + Comments)"})
        fig.update_layout(height=500, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        # Detailed table
        st.subheader("Gedetailleerde Statistieken")

        display_df = df[["Land", "Platform", "Handle", "Posts", "Totaal Likes", "Totaal Comments",
                        "Gem. Likes", "Gem. Comments"]].copy()
        display_df.columns = ["Land", "📱", "Account", "Posts", "Likes", "Comments",
                             "Gem. Likes/Post", "Gem. Comments/Post"]

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Engagement ratio analyse
        st.subheader("Engagement Ratio Analyse")
        st.caption("Verhouding tussen likes en comments per land")

        # Bereken engagement ratio
        df_ratio = df.copy()
        df_ratio["Engagement Ratio"] = (df_ratio["Totaal Comments"] / df_ratio["Totaal Likes"] * 100).round(1)
        df_ratio = df_ratio.sort_values("Engagement Ratio", ascending=True)

        col1, col2 = st.columns([2, 1])

        with col1:
            fig = px.bar(df_ratio,
                        x="Engagement Ratio", y="Land",
                        orientation='h',
                        color="Engagement Ratio",
                        color_continuous_scale="Viridis",
                        labels={"Engagement Ratio": "Comments per 100 Likes"})
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Interpretatie:**")
            st.markdown("""
            - **Hoge ratio** (>5%): Veel interactie, publiek reageert actief
            - **Lage ratio** (<2%): Passief publiek, vooral likes
            - **Gemiddeld** (2-5%): Normale verhouding
            """)

    else:
        st.warning("Geen data beschikbaar. Verzamel eerst posts met `python collect_all.py`")


def show_qualitative(db):
    """Qualitative analysis - tone of voice, content types."""
    st.header("Kwalitatieve Analyse")
    st.caption("Communicatiestijl, tone of voice en content categorisatie")

    # Explanation
    with st.expander("ℹ️ Wat is kwalitatieve analyse?", expanded=False):
        st.markdown("""
        **Kwalitatieve analyse** gaat over de *manier* waarop gecommuniceerd wordt, niet alleen hoeveel.

        We analyseren:
        - **Tone of Voice**: Is de communicatie formeel of informeel?
        - **Content Types**: Wat voor soort berichten worden geplaatst?
        - **Volledigheid**: Bevat het bericht alle relevante informatie?
        - **Call-to-Action**: Wordt de lezer aangezet tot actie?

        Deze analyse is uitgevoerd met **Claude AI** die elke post heeft beoordeeld.
        """)

    st.markdown("---")

    # SECTION 1: Content Types
    st.subheader("1. Content Type Analyse")
    st.markdown("Wat voor soort berichten plaatsen de verschillende MFA's?")

    col1, col2 = st.columns([1, 1])

    with col1:
        # Pie chart of content types (excl. Nederland, Instagram only)
        content_data = db.fetchall("""
            SELECT pc.content_type, COUNT(*) as count
            FROM post_classification pc
            JOIN posts p ON pc.post_id = p.id
            JOIN accounts a ON p.account_id = a.id
            WHERE a.status = 'active' AND a.platform = 'instagram'
            GROUP BY pc.content_type
            ORDER BY count DESC
        """)

        if content_data:
            df_content = pd.DataFrame(content_data, columns=["Type", "Aantal"])

            # Translate content types to Dutch
            type_labels = {
                "procedureel": "Procedures",
                "promotioneel": "Promoties",
                "wijziging": "Wijzigingen",
                "waarschuwing": "Waarschuwingen",
                "service": "Service",
                "overig": "Overig"
            }
            df_content["Type"] = df_content["Type"].map(lambda x: type_labels.get(x, x))

            # Color mapping
            colors = {
                "Procedures": "#2196F3",
                "Promoties": "#4CAF50",
                "Wijzigingen": "#FF9800",
                "Waarschuwingen": "#f44336",
                "Service": "#9C27B0",
                "Overig": "#9E9E9E"
            }

            fig = px.pie(df_content, values="Aantal", names="Type",
                        color="Type",
                        color_discrete_map=colors,
                        hole=0.4)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Uitleg Content Types:**")

        st.markdown("""
        <div class="definition-box">
        <b>📋 Procedures</b><br>
        Uitleg over processen: visumaanvragen, paspoorten, benodigde documenten<br>
        <i>"Voor een visumaanvraag heeft u nodig: paspoort, foto, formulier..."</i>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="definition-box">
        <b>🤝 Service</b><br>
        Helpende, dienstverlenende berichten gericht op burgers<br>
        <i>"Heeft u vragen? Neem contact op via ons contactformulier..."</i>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="definition-box">
        <b>🎉 Promoties</b><br>
        Evenementen, cultuur, handel, positieve diplomatieke berichten<br>
        <i>"Bezoek onze culturele week! Ontdek de kunst en muziek..."</i>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="definition-box">
        <b>🔄 Wijzigingen</b><br>
        Aankondigingen van veranderingen, nieuwe regels, updates<br>
        <i>"Vanaf 1 februari gelden nieuwe visumtarieven..."</i>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="definition-box">
        <b>⚠️ Waarschuwingen</b><br>
        Urgente mededelingen, sluitingen, reisadviezen<br>
        <i>"Let op: consulaat gesloten wegens feestdag..."</i>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # SECTION 2: Tone of Voice
    st.subheader("2. Tone of Voice Analyse")
    st.markdown("Hoe formeel of informeel communiceren de verschillende landen?")

    # Explanation of formality
    with st.expander("🎯 Wat betekent 'formeel' precies?"):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Formele communicatie (0.7-1.0)")
            st.markdown("""
            **Kenmerken:**
            - Gebruik van "u" in plaats van "je"
            - Lange, complexe zinnen
            - Passieve schrijfstijl ("wordt verzocht")
            - Geen emoji's of uitroeptekens
            - Ambtelijke/juridische taal
            - Afstandelijke toon
            """)
            st.markdown("""
            <div class="example-box">
            "Belanghebbenden worden verzocht kennis te nemen van de gewijzigde
            procedures inzake de aanvraag van reisdocumenten."
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown("### Informele communicatie (0.0-0.4)")
            st.markdown("""
            **Kenmerken:**
            - Gebruik van "je/jij"
            - Korte, directe zinnen
            - Actieve schrijfstijl
            - Emoji's en uitroeptekens toegestaan
            - Spreektaal en persoonlijke toon
            - Laagdrempelig en toegankelijk
            """)
            st.markdown("""
            <div class="example-box">
            "Hey! 👋 Check onze nieuwe visa-app! Makkelijker aanvragen
            was nog nooit zo simpel. Probeer het nu!"
            </div>
            """, unsafe_allow_html=True)

    # Formality scores per country (excl. Nederland, Instagram only)
    formality_data = db.fetchall("""
        SELECT a.country, cp.avg_formality_score, cp.pct_procedural, cp.pct_with_cta
        FROM account_comm_profile cp
        JOIN accounts a ON cp.account_id = a.id
        WHERE cp.avg_formality_score IS NOT NULL AND a.status = 'active' AND a.platform = 'instagram'
        ORDER BY cp.avg_formality_score DESC
    """)

    if formality_data:
        df_form = pd.DataFrame(formality_data, columns=["Land", "Formaliteit", "% Procedures", "% CTA"])
        df_form["Land"] = df_form["Land"].apply(lambda x: COUNTRY_NAMES_NL.get(x, x))

        # Categorize
        def categorize_formality(score):
            if score >= 0.7:
                return "Formeel"
            elif score >= 0.4:
                return "Neutraal"
            else:
                return "Informeel"

        df_form["Categorie"] = df_form["Formaliteit"].apply(categorize_formality)

        # Horizontal bar chart
        fig = px.bar(df_form.sort_values("Formaliteit"),
                    x="Formaliteit", y="Land",
                    orientation='h',
                    color="Categorie",
                    color_discrete_map={
                        "Formeel": "#f44336",
                        "Neutraal": "#FFC107",
                        "Informeel": "#4CAF50"
                    })

        fig.add_vline(x=0.5, line_dash="dash", line_color="gray",
                     annotation_text="Grens")
        fig.add_vline(x=0.7, line_dash="dot", line_color="red")

        fig.update_layout(
            height=500,
            xaxis_range=[0, 1],
            xaxis_title="Formaliteitsscore (0=informeel, 1=formeel)"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Summary stats
        col1, col2, col3 = st.columns(3)
        with col1:
            formal_count = len(df_form[df_form["Categorie"] == "Formeel"])
            st.metric("Formeel", f"{formal_count} landen")
        with col2:
            neutral_count = len(df_form[df_form["Categorie"] == "Neutraal"])
            st.metric("Neutraal", f"{neutral_count} landen")
        with col3:
            informal_count = len(df_form[df_form["Categorie"] == "Informeel"])
            st.metric("Informeel", f"{informal_count} landen")

    st.markdown("---")

    # SECTION 3: Completeness & CTA
    st.subheader("3. Informatiekwaliteit")
    st.markdown("Hoe volledig zijn de berichten en bevatten ze een call-to-action?")

    quality_data = db.fetchall("""
        SELECT a.country, cp.avg_completeness, cp.pct_with_cta, cp.pct_procedural
        FROM account_comm_profile cp
        JOIN accounts a ON cp.account_id = a.id
        WHERE cp.avg_completeness IS NOT NULL AND a.status = 'active' AND a.platform = 'instagram'
        ORDER BY cp.avg_completeness DESC
    """)

    if quality_data:
        df_quality = pd.DataFrame(quality_data, columns=["Land", "Volledigheid", "% CTA", "% Procedures"])
        df_quality["Land"] = df_quality["Land"].apply(lambda x: COUNTRY_NAMES_NL.get(x, x))

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Volledigheid per land**")
            st.caption("Meet of berichten WIE, WAT, WANNEER en HOE bevatten")

            fig = px.bar(df_quality.sort_values("Volledigheid", ascending=True),
                        x="Volledigheid", y="Land",
                        orientation='h',
                        color="Volledigheid",
                        color_continuous_scale="Greens")
            fig.update_layout(height=400, xaxis_range=[0, 1])
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Call-to-Action gebruik**")
            st.caption("% berichten met concrete actie-oproep")

            df_cta = df_quality[df_quality["% CTA"] > 0].sort_values("% CTA", ascending=True)
            if not df_cta.empty:
                fig = px.bar(df_cta,
                            x="% CTA", y="Land",
                            orientation='h',
                            color="% CTA",
                            color_continuous_scale="Oranges")
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Geen CTA data beschikbaar")

    # Profile comparison table
    st.markdown("---")
    st.subheader("4. Volledig Communicatieprofiel")

    profiles = db.fetchall("""
        SELECT
            a.country, a.handle,
            cp.pct_procedural, cp.pct_promotional, cp.pct_wijziging, cp.pct_waarschuwing,
            cp.avg_formality_score, cp.pct_with_cta, cp.avg_completeness, cp.dominant_tone
        FROM account_comm_profile cp
        JOIN accounts a ON cp.account_id = a.id
        WHERE a.status = 'active' AND a.platform = 'instagram'
        ORDER BY cp.pct_procedural DESC
    """)

    if profiles:
        df_profiles = pd.DataFrame(profiles, columns=[
            "Land", "Handle", "% Procedures", "% Promoties", "% Wijzigingen",
            "% Waarschuwingen", "Formaliteit", "% CTA", "Volledigheid", "Dominante Toon"
        ])
        df_profiles["Land"] = df_profiles["Land"].apply(lambda x: COUNTRY_NAMES_NL.get(x, x))

        # Format percentages
        for col in ["% Procedures", "% Promoties", "% Wijzigingen", "% Waarschuwingen", "% CTA"]:
            df_profiles[col] = df_profiles[col].apply(lambda x: f"{x:.0f}%" if pd.notna(x) and x > 0 else "-")

        df_profiles["Formaliteit"] = df_profiles["Formaliteit"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-")
        df_profiles["Volledigheid"] = df_profiles["Volledigheid"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-")

        st.dataframe(df_profiles, use_container_width=True, hide_index=True)

    # SECTION 5: Woordwolk
    st.markdown("---")
    st.subheader("5. Woordwolk")
    st.markdown("Meest gebruikte woorden in posts - vertaald naar Nederlands")

    if WORDCLOUD_AVAILABLE:
        # Haal captions op - max 30 per land voor evenwichtige verdeling
        captions = db.fetchall("""
            WITH ranked_posts AS (
                SELECT p.caption_snippet, a.country,
                       ROW_NUMBER() OVER (PARTITION BY a.country ORDER BY p.posted_at DESC) as rn
                FROM posts p
                JOIN accounts a ON p.account_id = a.id
                WHERE a.platform = 'instagram' AND a.status = 'active'
                  AND p.caption_snippet IS NOT NULL
            )
            SELECT caption_snippet FROM ranked_posts WHERE rn <= 30
        """)

        if captions:
            # Combineer alle tekst
            all_text = " ".join([c[0] for c in captions if c[0]])

            # Verwijder URLs, mentions, hashtags
            all_text = re.sub(r'http\S+', '', all_text)
            all_text = re.sub(r'@\w+', '', all_text)
            all_text = re.sub(r'#\w+', '', all_text)
            all_text = re.sub(r'[^\w\s]', ' ', all_text)

            # Stopwoorden
            stopwords = set([
                'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
                'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
                'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
                'should', 'may', 'might', 'must', 'shall', 'can', 'this', 'that',
                'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
                'de', 'het', 'een', 'en', 'van', 'in', 'is', 'op', 'te', 'dat',
                'die', 'voor', 'met', 'zijn', 'niet', 'aan', 'er', 'maar', 'om',
                'ook', 'als', 'bij', 'nog', 'wel', 'naar', 'kan', 'tot', 'dan',
                'al', 'was', 'nu', 'meer', 'zo', 'hier', 'our', 'your', 'their',
                'its', 'my', 'his', 'her', 'us', 'them', 'who', 'what', 'which'
            ])

            # Tel woord frequenties
            words = all_text.split()
            words = [w for w in words if len(w) > 2 and w.lower() not in stopwords]
            word_counts = Counter(words)
            top_words = word_counts.most_common(150)

            if top_words:
                try:
                    # Haal vertalingen uit database
                    words_list = [w for w, _ in top_words]
                    existing = db.fetchall("SELECT original_word, dutch_word FROM word_translations")
                    translations = {row[0]: row[1] for row in existing}

                    # Bouw frequentie dict met vertaalde woorden
                    translated_freq = {}
                    latin_check = re.compile(r'^[a-zA-ZÀ-ÿ\s\-]+$')

                    for word, count in top_words:
                        # Gebruik vertaling als beschikbaar, anders origineel
                        nl_word = translations.get(word, word)

                        # Alleen toevoegen als het Latijnse tekens bevat
                        if nl_word and latin_check.match(nl_word):
                            nl_word = nl_word.lower()
                            if nl_word in translated_freq:
                                translated_freq[nl_word] += count
                            else:
                                translated_freq[nl_word] = count

                    if len(translated_freq) < 10:
                        st.warning(f"""
                        **Onvoldoende vertaalde woorden ({len(translated_freq)})**

                        Voer eerst het vertaalscript uit:
                        ```bash
                        python translate_words.py
                        ```
                        """)
                    else:
                        # Windows font
                        import os
                        font_path = 'C:/Windows/Fonts/arial.ttf' if os.path.exists('C:/Windows/Fonts/arial.ttf') else None

                        wordcloud = WordCloud(
                            width=800, height=400,
                            background_color='white',
                            max_words=100,
                            colormap='viridis',
                            font_path=font_path
                        ).generate_from_frequencies(translated_freq)

                        fig, ax = plt.subplots(figsize=(10, 5))
                        ax.imshow(wordcloud, interpolation='bilinear')
                        ax.axis('off')
                        st.pyplot(fig)
                        plt.close()

                except Exception as e:
                    st.warning(f"Kon woordwolk niet genereren: {e}")
            else:
                st.info("Niet genoeg woorden voor woordwolk")
        else:
            st.info("Geen posts beschikbaar voor woordwolk")
    else:
        st.warning("WordCloud package niet geïnstalleerd. Run: pip install wordcloud")



def show_country_detail(db):
    """Country detail view."""
    st.header("Analyse per Land")

    # Country selector
    accounts = AccountQueries.get_all(db)
    countries = sorted(set(a.country for a in accounts))

    selected_country = st.selectbox(
        "Selecteer land",
        countries,
        format_func=lambda x: COUNTRY_NAMES_NL.get(x, x)
    )

    if not selected_country:
        return

    country_name = COUNTRY_NAMES_NL.get(selected_country, selected_country)
    st.subheader(f"📍 {country_name}")

    # Get account for this country
    country_accounts = [a for a in accounts if a.country == selected_country]

    # Filter op Instagram only
    instagram_accounts = [a for a in country_accounts if a.platform == "instagram"]

    if not instagram_accounts:
        st.info("Geen Instagram accounts gevonden voor dit land")
        return

    # Helper function om account details te tonen
    def show_account_details(account, platform_color):
        st.markdown(f"**@{account.handle}**")

        # Get posts
        posts = PostQueries.get_by_account(account.id, limit=30, db=db)

        if posts:
            # Basic stats
            total_likes = sum(p.likes or 0 for p in posts)
            total_comments = sum(p.comments or 0 for p in posts)
            avg_likes = total_likes / len(posts)
            avg_comments = total_comments / len(posts)

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Posts", len(posts))
            with col2:
                st.metric("Totaal Likes", f"{total_likes:,}")
            with col3:
                st.metric("Totaal Comments", f"{total_comments:,}")
            with col4:
                st.metric("Gem. Likes/Post", f"{avg_likes:.0f}")

            st.markdown("---")

            # Communication profile
            profile = db.fetchone("""
                SELECT pct_procedural, pct_promotional, pct_wijziging, pct_waarschuwing,
                       avg_formality_score, pct_with_cta, avg_completeness, dominant_tone
                FROM account_comm_profile
                WHERE account_id = ?
            """, [account.id])

            if profile:
                st.subheader("Communicatieprofiel")

                col1, col2 = st.columns(2)

                with col1:
                    # Radar chart
                    categories = ["Procedures", "Promoties", "Wijzigingen", "Waarschuwingen"]
                    values = [
                        (profile[0] or 0) / 100,
                        (profile[1] or 0) / 100,
                        (profile[2] or 0) / 100,
                        (profile[3] or 0) / 100
                    ]

                    fig = go.Figure()
                    fig.add_trace(go.Scatterpolar(
                        r=values + [values[0]],
                        theta=categories + [categories[0]],
                        fill='toself',
                        name=country_name,
                        line_color=platform_color
                    ))
                    fig.update_layout(
                        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
                        showlegend=False,
                        margin=dict(t=40, b=40)
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.markdown("**Content Verdeling:**")
                    st.write(f"- Procedures: {profile[0]:.0f}%" if profile[0] else "- Procedures: -")
                    st.write(f"- Promoties: {profile[1]:.0f}%" if profile[1] else "- Promoties: -")
                    st.write(f"- Wijzigingen: {profile[2]:.0f}%" if profile[2] else "- Wijzigingen: -")
                    st.write(f"- Waarschuwingen: {profile[3]:.0f}%" if profile[3] else "- Waarschuwingen: -")

                    st.markdown("---")
                    st.markdown("**Tone of Voice:**")
                    st.write(f"- Formaliteit: {profile[4]:.2f}" if profile[4] else "- Formaliteit: -")
                    st.write(f"- Dominante toon: {profile[7]}" if profile[7] else "- Dominante toon: -")
                    st.write(f"- Call-to-Action: {profile[5]:.0f}%" if profile[5] else "- Call-to-Action: -")

            # Recent posts
            st.markdown("---")
            st.subheader("Recente Posts")

            # Get classified posts
            classified_posts = db.fetchall("""
                SELECT p.caption_snippet, p.likes, p.comments, p.posted_at,
                       pc.content_type, pc.tone_formality
                FROM posts p
                LEFT JOIN post_classification pc ON p.id = pc.post_id
                WHERE p.account_id = ?
                ORDER BY p.posted_at DESC
                LIMIT 10
            """, [account.id])

            # Content type translation
            content_type_labels = {
                "procedureel": "Procedures",
                "promotioneel": "Promoties",
                "wijziging": "Wijzigingen",
                "waarschuwing": "Waarschuwingen",
                "service": "Service",
                "overig": "Overig"
            }

            for post in classified_posts:
                caption = post[0][:150] + "..." if post[0] and len(post[0]) > 150 else post[0]
                content_type = post[4] or "onbekend"
                content_type_label = content_type_labels.get(content_type, content_type.title())

                type_emoji = {
                    "procedureel": "📋",
                    "promotioneel": "🎉",
                    "wijziging": "🔄",
                    "waarschuwing": "⚠️",
                    "service": "🤝",
                    "overig": "📝"
                }.get(content_type, "📝")

                with st.expander(f"{type_emoji} {content_type_label} - ❤️ {post[1] or 0} | 💬 {post[2] or 0}"):
                    st.write(caption or "Geen tekst beschikbaar")
                    if post[3]:
                        st.caption(f"Geplaatst: {str(post[3])[:10]}")
        else:
            st.info("Geen posts verzameld voor dit account")

    # Toon Instagram accounts
    for account in instagram_accounts:
        show_account_details(account, "#E1306C")  # Instagram pink
        st.markdown("---")


def show_methodology(db):
    """Research methodology and definitions."""
    st.header("📖 Onderzoeksopzet & Definities")
    st.caption("Hoe is dit onderzoek uitgevoerd en wat betekenen de begrippen?")

    # Tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs(["Onderzoeksopzet", "Definities", "Meetmethode", "Beperkingen"])

    with tab1:
        st.subheader("1. Onderzoeksopzet")

        st.markdown("""
        ### Doel van het onderzoek
        Dit dashboard analyseert de **communicatiestijl** van Ministeries van Buitenlandse Zaken
        op **Instagram**. We onderzoeken:

        1. **Kwantitatief**: Hoeveel interactie (likes, comments) krijgen de accounts?
        2. **Kwalitatief**: Hoe communiceren ze? Formeel of informeel? Service-gericht of promotioneel?

        ### Onderzoeksperiode
        **Juli 2025 - Januari 2026** (6 maanden)

        Dit onderzoek bestrijkt een halfjaar om trends en patronen in communicatie te analyseren.

        ### Dataverzameling
        """)

        # Show actual data stats
        stats = db.fetchone("""
            SELECT
                COUNT(DISTINCT a.id) as accounts,
                COUNT(p.id) as posts,
                MIN(p.posted_at) as earliest,
                MAX(p.posted_at) as latest
            FROM accounts a
            LEFT JOIN posts p ON a.id = p.account_id
        """)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Accounts", stats[0] if stats else 0)
        with col2:
            st.metric("Posts verzameld", stats[1] if stats else 0)
        with col3:
            st.metric("Vroegste post", str(stats[2])[:10] if stats and stats[2] else "-")
        with col4:
            st.metric("Laatste post", str(stats[3])[:10] if stats and stats[3] else "-")

        st.markdown("""
        ### Platforms in dit onderzoek
        """)

        platform_counts = db.fetchall("""
            SELECT platform, COUNT(*) as cnt FROM accounts
            WHERE status = 'active' AND platform = 'instagram'
            GROUP BY platform
        """)

        if platform_counts:
            platform_text = ", ".join([f"**{p[0].title()}** ({p[1]} accounts)" for p in platform_counts])
            st.markdown(platform_text)

        st.markdown("""
        ### Landen in dit onderzoek
        """)

        countries = db.fetchall("""
            SELECT country, platform, handle FROM accounts
            WHERE status = 'active' AND platform = 'instagram'
            ORDER BY country, platform
        """)

        if countries:
            country_list = [f"- **{COUNTRY_NAMES_NL.get(c[0], c[0])}** 📸 @{c[2]}" for c in countries]
            cols = st.columns(3)
            third = len(country_list) // 3
            for i, col in enumerate(cols):
                with col:
                    start = i * third
                    end = start + third if i < 2 else len(country_list)
                    st.markdown("\n".join(country_list[start:end]))

    with tab2:
        st.subheader("2. Definities")

        st.markdown("### Tone of Voice: Formaliteit")

        st.markdown("""
        De **formaliteitsscore** (0.0 - 1.0) meet hoe formeel of informeel een bericht is geschreven.
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""
            #### 🟢 Informeel (0.0 - 0.4)

            **Kenmerken:**
            - Persoonlijke aanspreekvorm ("je", "jij")
            - Korte, directe zinnen
            - Actieve schrijfstijl
            - Emoji's en uitroeptekens
            - Spreektaal
            - Enthousiaste, toegankelijke toon

            **Voorbeelden:**
            """)
            st.info('"Hey! 👋 Nieuw paspoort nodig? Check onze website voor de snelste route!"')
            st.info('"Super nieuws! 🎉 Onze nieuwe visa-app is live. Download hem nu!"')

        with col2:
            st.markdown("""
            #### 🔴 Formeel (0.7 - 1.0)

            **Kenmerken:**
            - Beleefdsheidsvorm ("u")
            - Lange, complexe zinnen
            - Passieve schrijfstijl
            - Geen emoji's
            - Ambtelijke/juridische taal
            - Afstandelijke, zakelijke toon

            **Voorbeelden:**
            """)
            st.info('"Belanghebbenden worden verzocht kennis te nemen van de gewijzigde regelgeving inzake reisdocumenten."')
            st.info('"Het Ministerie informeert u hierbij over de aanpassingen in de consulaire dienstverlening."')

        st.markdown("---")

        st.markdown("### Content Types")

        st.markdown("""
        Elk bericht wordt gecategoriseerd in één van de volgende types:
        """)

        content_types = [
            ("📋 Procedures", "Uitleg over processen en aanvraagprocedures",
             "Visumaanvragen, paspoort vernieuwing, benodigde documenten, openingstijden, stappen in een proces",
             '"Voor een visumaanvraag heeft u nodig: geldig paspoort, pasfoto, ingevuld formulier. Lever in bij loket 3."'),

            ("🤝 Service", "Helpende, dienstverlenende berichten gericht op burgers",
             "Contactinformatie, hulp aanbieden, vragen beantwoorden, doorverwijzingen, klantenservice",
             '"Heeft u vragen over uw aanvraag? Neem contact op via ons formulier of bel +31 247 247 247."'),

            ("🎉 Promoties", "Positieve berichten over evenementen, cultuur en diplomatie",
             "Culturele events, handelsmissies, nationale feestdagen, staatsbezoeken, successen",
             '"Vier met ons de Nationale Dag! Ontdek onze rijke cultuur en tradities op het festival dit weekend."'),

            ("🔄 Wijzigingen", "Aankondigingen van veranderingen en updates",
             "Nieuwe regels, gewijzigde tarieven, aangepaste procedures, beleidswijzigingen",
             '"Belangrijk: vanaf 1 maart gelden nieuwe visumtarieven. Bekijk de actuele prijslijst op onze website."'),

            ("⚠️ Waarschuwingen", "Urgente mededelingen en alerts",
             "Sluitingen, vertragingen, reisadviezen, noodsituaties, storingen",
             '"Let op: het consulaat is 25-26 december gesloten wegens feestdagen. Spoedeisende zaken: noodlijn."'),

            ("📝 Overig", "Berichten die niet in bovenstaande categorieën passen",
             "Condoleances, felicitaties, algemene statements, persoonlijke berichten",
             '"We wensen iedereen een voorspoedig nieuwjaar."')
        ]

        for title, desc, examples, quote in content_types:
            with st.expander(title):
                st.markdown(f"**Definitie:** {desc}")
                st.markdown(f"**Voorbeelden van onderwerpen:** {examples}")
                st.info(f"**Voorbeeldbericht:** {quote}")

        st.markdown("---")

        st.markdown("### Andere Metrics")

        metrics_def = [
            ("Service-gericht",
             "Meet of een bericht helpend en dienstverlenend is naar de burger",
             "Een service-gericht bericht biedt hulp, beantwoordt vragen, of verwijst naar contactmogelijkheden. Dit staat los van het content type - ook een procedure-uitleg kan service-gericht zijn."),

            ("Volledigheid (0.0 - 1.0)",
             "Meet of een bericht alle relevante informatie bevat",
             "Score van 0.25 per aanwezig element: WIE (organisatie), WAT (onderwerp), WANNEER (datum/tijd), HOE (link/instructie)"),

            ("Call-to-Action (CTA)",
             "Of het bericht de lezer aanzet tot een concrete actie",
             "Voorbeelden: 'Klik hier', 'Registreer nu', 'Bezoek onze website', 'Neem contact op'"),

            ("Engagement",
             "De totale interactie op een post",
             "Berekend als: Likes + Comments. Geeft aan hoe actief het publiek reageert."),

            ("Reactiesnelheid (toekomstig)",
             "Hoe snel en vaak reageert het account op vragen in comments",
             "Meet of het account zelf actief reageert op vragen van volgers. Indicatie van service-niveau.")
        ]

        for title, desc, detail in metrics_def:
            st.markdown(f"**{title}**")
            st.markdown(f"- {desc}")
            st.caption(detail)

    with tab3:
        st.subheader("3. Meetmethode")

        st.markdown("""
        ### Hoe worden posts geclassificeerd?

        De classificatie gebeurt in twee stappen:

        #### Stap 1: Data Verzameling
        Posts worden verzameld via de Instagram API/scraping. Per post slaan we op:
        - Tekst (caption)
        - Aantal likes en comments
        - Datum van plaatsing
        - URL en hashtags

        #### Stap 2: AI Classificatie
        Elke post wordt geanalyseerd door **Claude AI** (Anthropic). De AI krijgt de volgende prompt:
        """)

        with st.expander("Bekijk de classificatie-prompt"):
            st.code("""
Analyseer deze social media post van een overheidsaccount (ambassade/ministerie).

POST:
[tekst van de post]

Classificeer de post op de volgende dimensies:

1. content_type: Kies uit:
   - "procedureel": informatie over procedures, documenten, aanvragen
   - "wijziging": aankondiging van veranderingen, nieuwe regels
   - "waarschuwing": sluitingen, vertragingen, urgente mededelingen
   - "promotioneel": evenementen, cultuur, positieve berichten
   - "overig": past niet in bovenstaande categorieën

2. tone_formality: Getal van 0.0 (zeer informeel) tot 1.0 (zeer formeel)
   - 0.0-0.3: informeel (emoji's, spreektaal, "je/jij")
   - 0.4-0.6: neutraal
   - 0.7-1.0: formeel ("u", ambtelijke taal)

3. is_service_oriented: true/false - Is de post helpend/servicegericht?

4. has_call_to_action: true/false - Wordt een actie gevraagd?

5. completeness_score: 0.0-1.0
   - 0.25 per element: WIE, WAT, WANNEER, HOE

6. detected_deadline: Datum indien genoemd, anders null
            """, language="text")

        st.markdown("""
        #### Waarom AI classificatie?

        | Methode | Voordeel | Nadeel |
        |---------|----------|--------|
        | **Handmatig** | Zeer nauwkeurig | Tijdrovend, niet schaalbaar |
        | **Keywords** | Snel, goedkoop | Mist context en nuance |
        | **AI (Claude)** | Begrijpt context, schaalbaar | Kost API credits |

        We gebruiken Claude AI omdat het:
        - Context en nuance begrijpt
        - Meerdere talen kan analyseren
        - Consistent classificeert
        - Schaalbaar is voor grote datasets
        """)

    with tab4:
        st.subheader("4. Beperkingen")

        st.markdown("""
        ### Beperkingen van dit onderzoek

        ⚠️ **Houd rekening met de volgende beperkingen:**

        #### Data beperkingen
        - **Instagram rate-limiting**: Instagram beperkt automatische dataverzameling,
          waardoor niet alle historische posts beschikbaar zijn
        - **Geen volgers data**: Follower counts zijn niet meegenomen in deze versie
        - **Alleen Instagram**: Facebook is niet meegenomen vanwege beperkingen in
          het verzamelen van engagement data (likes/comments)

        #### Platform dekking
        - **Instagram**: ~30 posts per account (beperkt door rate-limiting)
        - Posts van de afgelopen 6 maanden zijn geanalyseerd

        #### Classificatie beperkingen
        - **AI is niet perfect**: Claude kan fouten maken in classificatie
        - **Taal variatie**: Posts in lokale talen kunnen anders geïnterpreteerd worden
        - **Context ontbreekt**: AI ziet geen afbeeldingen/video's, alleen tekst
        - **Subjectiviteit**: "Formeel" is deels subjectief

        #### Vergelijkbaarheid
        - **Verschillende doelgroepen**: MFA's bedienen verschillende publieken
        - **Cultuurverschillen**: Wat "formeel" is verschilt per cultuur
        - **Account doel**: Sommige accounts zijn meer promotioneel van aard

        ### Aanbevelingen voor interpretatie

        ✅ **Gebruik deze data voor:**
        - Algemene trends en patronen identificeren
        - Vergelijking tussen landen op hoofdlijnen
        - Inspiratie voor communicatiestrategie

        ❌ **Gebruik deze data NIET voor:**
        - Harde conclusies over "goed" of "slecht"
        - Individuele post-beoordeling
        - Definitieve rankings
        """)


def show_export(db):
    """Export functionality."""
    st.header("💾 Data Export")

    st.subheader("Export Opties")

    export_type = st.selectbox(
        "Selecteer export type",
        ["Communicatieprofielen", "Alle posts met classificatie", "Ruwe post data"]
    )

    if st.button("Genereer Export", type="primary"):
        with st.spinner("Data voorbereiden..."):

            if export_type == "Communicatieprofielen":
                data = db.fetchall("""
                    SELECT
                        a.country, a.handle,
                        cp.total_posts_analyzed,
                        cp.pct_procedural, cp.pct_promotional,
                        cp.pct_wijziging, cp.pct_waarschuwing,
                        cp.avg_formality_score, cp.dominant_tone,
                        cp.pct_with_cta, cp.avg_completeness
                    FROM account_comm_profile cp
                    JOIN accounts a ON cp.account_id = a.id
                    ORDER BY a.country
                """)

                if data:
                    df = pd.DataFrame(data, columns=[
                        "Land", "Handle", "Posts Geanalyseerd",
                        "% Procedureel", "% Promotioneel", "% Wijziging", "% Waarschuwing",
                        "Formaliteit Score", "Dominante Toon", "% CTA", "Volledigheid"
                    ])
                    df["Land"] = df["Land"].apply(lambda x: COUNTRY_NAMES_NL.get(x, x))

                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "⬇️ Download CSV",
                        csv,
                        "mfa_communicatieprofielen.csv",
                        "text/csv"
                    )
                    st.dataframe(df, use_container_width=True, hide_index=True)

            elif export_type == "Alle posts met classificatie":
                data = db.fetchall("""
                    SELECT
                        a.country, a.handle, p.posted_at,
                        p.likes, p.comments,
                        pc.content_type, pc.tone_formality,
                        pc.has_call_to_action, pc.completeness_score,
                        p.caption_snippet
                    FROM posts p
                    JOIN accounts a ON p.account_id = a.id
                    LEFT JOIN post_classification pc ON p.id = pc.post_id
                    ORDER BY a.country, p.posted_at DESC
                """)

                if data:
                    df = pd.DataFrame(data, columns=[
                        "Land", "Handle", "Datum", "Likes", "Comments",
                        "Content Type", "Formaliteit", "Heeft CTA", "Volledigheid", "Tekst"
                    ])
                    df["Land"] = df["Land"].apply(lambda x: COUNTRY_NAMES_NL.get(x, x))

                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "⬇️ Download CSV",
                        csv,
                        "mfa_posts_classificatie.csv",
                        "text/csv"
                    )
                    st.dataframe(df.head(100), use_container_width=True, hide_index=True)
                    st.caption(f"Preview: eerste 100 van {len(df)} posts")

            else:  # Ruwe post data
                data = db.fetchall("""
                    SELECT
                        a.country, a.handle, p.posted_at,
                        p.likes, p.comments, p.url, p.caption_snippet
                    FROM posts p
                    JOIN accounts a ON p.account_id = a.id
                    ORDER BY a.country, p.posted_at DESC
                """)

                if data:
                    df = pd.DataFrame(data, columns=[
                        "Land", "Handle", "Datum", "Likes", "Comments", "URL", "Tekst"
                    ])
                    df["Land"] = df["Land"].apply(lambda x: COUNTRY_NAMES_NL.get(x, x))

                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "⬇️ Download CSV",
                        csv,
                        "mfa_ruwe_posts.csv",
                        "text/csv"
                    )
                    st.dataframe(df.head(50), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
