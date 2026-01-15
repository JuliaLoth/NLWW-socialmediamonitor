"""
PDF rapport generator met WeasyPrint.
"""
from pathlib import Path
from datetime import datetime
from typing import Optional
import logging

try:
    from jinja2 import Template
    from weasyprint import HTML, CSS
except ImportError:
    Template = None
    HTML = None

from ...database.connection import Database, get_connection
from ...database.queries import AccountQueries, MetricsQueries
from ...analysis.benchmarks import get_platform_comparison, get_regional_comparison, get_top_performers
from ...config.settings import COUNTRY_NAMES_NL, PLATFORM_NAMES_NL, EXPORTS_DIR

logger = logging.getLogger(__name__)

# HTML template voor rapport
REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="nl">
<head>
    <meta charset="UTF-8">
    <title>NL Ambassade Social Media Rapport - {{ year_month }}</title>
    <style>
        @page {
            size: A4;
            margin: 2cm;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            font-size: 11pt;
            line-height: 1.5;
            color: #333;
        }

        .header {
            background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
            color: white;
            padding: 20px;
            margin: -2cm -2cm 20px -2cm;
            text-align: center;
        }

        .header h1 {
            margin: 0;
            font-size: 24pt;
        }

        .header .subtitle {
            margin-top: 10px;
            font-size: 12pt;
            opacity: 0.9;
        }

        h2 {
            color: #1e3a5f;
            border-bottom: 2px solid #1e3a5f;
            padding-bottom: 5px;
            margin-top: 30px;
        }

        h3 {
            color: #2d5a87;
            margin-top: 20px;
        }

        .summary-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            margin: 20px 0;
        }

        .summary-card {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
            border-left: 4px solid #1e3a5f;
        }

        .summary-card .value {
            font-size: 24pt;
            font-weight: bold;
            color: #1e3a5f;
        }

        .summary-card .label {
            font-size: 10pt;
            color: #666;
            margin-top: 5px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 10pt;
        }

        th {
            background: #1e3a5f;
            color: white;
            padding: 10px;
            text-align: left;
        }

        td {
            padding: 8px 10px;
            border-bottom: 1px solid #ddd;
        }

        tr:nth-child(even) {
            background: #f8f9fa;
        }

        .positive {
            color: #28a745;
        }

        .negative {
            color: #dc3545;
        }

        .footer {
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            font-size: 9pt;
            color: #666;
            text-align: center;
        }

        .page-break {
            page-break-before: always;
        }

        .two-column {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Social Media Rapport</h1>
        <div class="subtitle">Nederlandse Ambassades - {{ year_month }}</div>
    </div>

    <h2>Samenvatting</h2>

    <div class="summary-grid">
        <div class="summary-card">
            <div class="value">{{ summary.total_accounts }}</div>
            <div class="label">Actieve Accounts</div>
        </div>
        <div class="summary-card">
            <div class="value">{{ "{:,}".format(summary.total_followers) }}</div>
            <div class="label">Totaal Volgers</div>
        </div>
        <div class="summary-card">
            <div class="value">{{ summary.total_posts }}</div>
            <div class="label">Posts Deze Maand</div>
        </div>
        <div class="summary-card">
            <div class="value">{{ "%.3f"|format(summary.avg_engagement) }}%</div>
            <div class="label">Gem. Engagement</div>
        </div>
    </div>

    <div class="two-column">
        <div>
            <h3>Top 5 Performers</h3>
            <table>
                <tr>
                    <th>#</th>
                    <th>Land</th>
                    <th>Platform</th>
                    <th>Engagement</th>
                </tr>
                {% for item in top_performers %}
                <tr>
                    <td>{{ item.rank }}</td>
                    <td>{{ item.country }}</td>
                    <td>{{ item.platform }}</td>
                    <td>{{ "%.3f"|format(item.value) }}%</td>
                </tr>
                {% endfor %}
            </table>
        </div>
        <div>
            <h3>Aandachtspunten</h3>
            <table>
                <tr>
                    <th>#</th>
                    <th>Land</th>
                    <th>Platform</th>
                    <th>Engagement</th>
                </tr>
                {% for item in bottom_performers %}
                <tr>
                    <td>{{ item.rank }}</td>
                    <td>{{ item.country }}</td>
                    <td>{{ item.platform }}</td>
                    <td>{{ "%.3f"|format(item.value) }}%</td>
                </tr>
                {% endfor %}
            </table>
        </div>
    </div>

    <div class="page-break"></div>

    <h2>Prestaties per Platform</h2>

    <table>
        <tr>
            <th>Platform</th>
            <th>Accounts</th>
            <th>Totaal Volgers</th>
            <th>Posts</th>
            <th>Gem. Engagement</th>
        </tr>
        {% for platform in platforms %}
        <tr>
            <td>{{ platform.name }}</td>
            <td>{{ platform.accounts }}</td>
            <td>{{ "{:,}".format(platform.followers) }}</td>
            <td>{{ platform.posts }}</td>
            <td>{{ "%.4f"|format(platform.engagement) }}%</td>
        </tr>
        {% endfor %}
    </table>

    <h2>Prestaties per Land</h2>

    <table>
        <tr>
            <th>Land</th>
            <th>Platforms</th>
            <th>Volgers</th>
            <th>Posts</th>
            <th>Engagement</th>
        </tr>
        {% for country in countries %}
        <tr>
            <td>{{ country.name }}</td>
            <td>{{ country.platforms }}</td>
            <td>{{ "{:,}".format(country.followers) }}</td>
            <td>{{ country.posts }}</td>
            <td>{{ "%.4f"|format(country.engagement) }}%</td>
        </tr>
        {% endfor %}
    </table>

    <div class="page-break"></div>

    <h2>Gedetailleerd Overzicht per Account</h2>

    <table>
        <tr>
            <th>Land</th>
            <th>Platform</th>
            <th>Handle</th>
            <th>Volgers</th>
            <th>Groei</th>
            <th>Posts</th>
            <th>Engagement</th>
        </tr>
        {% for account in accounts %}
        <tr>
            <td>{{ account.country }}</td>
            <td>{{ account.platform }}</td>
            <td>@{{ account.handle }}</td>
            <td>{{ "{:,}".format(account.followers) if account.followers else "-" }}</td>
            <td class="{{ 'positive' if account.growth > 0 else 'negative' if account.growth < 0 else '' }}">
                {{ "{:+,}".format(account.growth) if account.growth else "-" }}
            </td>
            <td>{{ account.posts }}</td>
            <td>{{ "%.3f"|format(account.engagement) if account.engagement else "-" }}%</td>
        </tr>
        {% endfor %}
    </table>

    <div class="footer">
        <p>Dit rapport is automatisch gegenereerd door de NL Ambassade Social Media Monitor</p>
        <p>Gegenereerd op: {{ generated_at }}</p>
    </div>
</body>
</html>
"""


def generate_monthly_pdf(
    year_month: str,
    output_path: Optional[Path] = None,
    db: Optional[Database] = None
) -> Path:
    """
    Genereer PDF rapport voor een maand.
    """
    if Template is None or HTML is None:
        raise ImportError(
            "jinja2 en weasyprint zijn vereist voor PDF generatie. "
            "Installeer met: pip install jinja2 weasyprint"
        )

    db = db or get_connection()

    if output_path is None:
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = EXPORTS_DIR / f"rapport_{year_month}.pdf"

    # Verzamel data
    metrics = MetricsQueries.get_all_for_month(year_month, db)
    all_accounts = {a.id: a for a in AccountQueries.get_all(db)}

    # Summary
    summary = {
        "total_accounts": len(metrics),
        "total_followers": sum(m.avg_followers or 0 for m in metrics),
        "total_posts": sum(m.total_posts for m in metrics),
        "avg_engagement": (
            sum(m.avg_engagement_rate or 0 for m in metrics) / len(metrics)
            if metrics else 0
        ),
    }

    # Top/Bottom performers
    from ...analysis.benchmarks import get_top_performers, get_bottom_performers
    top_performers = get_top_performers(year_month, n=5, db=db)
    bottom_performers = get_bottom_performers(year_month, n=5, db=db)

    # Platform data
    platform_comparison = get_platform_comparison(year_month, db)
    platforms = [
        {
            "name": PLATFORM_NAMES_NL.get(p, p),
            "accounts": d["accounts"],
            "followers": d["total_followers"],
            "posts": d["total_posts"],
            "engagement": d["avg_engagement_rate"],
        }
        for p, d in platform_comparison.items()
    ]

    # Country data
    regional_comparison = get_regional_comparison(year_month, db)
    countries = [
        {
            "name": d["display_name"],
            "platforms": ", ".join(d["platforms"]),
            "followers": d["total_followers"],
            "posts": d["total_posts"],
            "engagement": d["avg_engagement_rate"],
        }
        for c, d in regional_comparison.items()
    ]

    # Account details
    sorted_metrics = sorted(
        metrics,
        key=lambda m: m.avg_engagement_rate or 0,
        reverse=True
    )

    accounts = []
    for m in sorted_metrics:
        account = all_accounts.get(m.account_id)
        if account:
            accounts.append({
                "country": COUNTRY_NAMES_NL.get(account.country, account.country),
                "platform": PLATFORM_NAMES_NL.get(account.platform, account.platform),
                "handle": account.handle,
                "followers": m.avg_followers,
                "growth": m.follower_growth,
                "posts": m.total_posts,
                "engagement": m.avg_engagement_rate,
            })

    # Render template
    template = Template(REPORT_TEMPLATE)
    html_content = template.render(
        year_month=year_month,
        summary=summary,
        top_performers=top_performers,
        bottom_performers=bottom_performers,
        platforms=platforms,
        countries=countries,
        accounts=accounts,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )

    # Generate PDF
    html = HTML(string=html_content)
    html.write_pdf(output_path)

    logger.info(f"PDF rapport gegenereerd: {output_path}")
    return output_path


def generate_yearly_pdf(
    year: int,
    output_path: Optional[Path] = None,
    db: Optional[Database] = None
) -> Path:
    """
    Genereer jaarlijks PDF rapport.
    """
    if Template is None or HTML is None:
        raise ImportError("jinja2 en weasyprint zijn vereist voor PDF generatie")

    db = db or get_connection()

    if output_path is None:
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = EXPORTS_DIR / f"jaarrapport_{year}.pdf"

    # Verzamel data per maand
    monthly_data = []
    for month in range(1, 13):
        year_month = f"{year:04d}-{month:02d}"
        metrics = MetricsQueries.get_all_for_month(year_month, db)

        if metrics:
            monthly_data.append({
                "month": year_month,
                "accounts": len(metrics),
                "followers": sum(m.avg_followers or 0 for m in metrics),
                "posts": sum(m.total_posts for m in metrics),
                "engagement": sum(m.avg_engagement_rate or 0 for m in metrics) / len(metrics),
            })

    # Simple yearly template
    yearly_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body { font-family: sans-serif; margin: 40px; }
            h1 { color: #1e3a5f; }
            table { width: 100%; border-collapse: collapse; }
            th { background: #1e3a5f; color: white; padding: 10px; }
            td { padding: 8px; border-bottom: 1px solid #ddd; }
        </style>
    </head>
    <body>
        <h1>Jaarrapport {{ year }}</h1>
        <table>
            <tr>
                <th>Maand</th>
                <th>Accounts</th>
                <th>Volgers</th>
                <th>Posts</th>
                <th>Engagement</th>
            </tr>
            {% for m in months %}
            <tr>
                <td>{{ m.month }}</td>
                <td>{{ m.accounts }}</td>
                <td>{{ "{:,}".format(m.followers) }}</td>
                <td>{{ m.posts }}</td>
                <td>{{ "%.3f"|format(m.engagement) }}%</td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """

    template = Template(yearly_template)
    html_content = template.render(year=year, months=monthly_data)

    html = HTML(string=html_content)
    html.write_pdf(output_path)

    logger.info(f"Jaarlijks PDF rapport gegenereerd: {output_path}")
    return output_path
