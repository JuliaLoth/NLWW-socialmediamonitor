# NL Embassy Social Media Monitor

Multi-agent systeem voor monitoring van Nederlandse ambassade social media accounts wereldwijd.

## Overzicht

Dit systeem monitort 20+ Nederlandse ambassades/consulaten op:
- Instagram
- Facebook
- X (Twitter)

Het verzamelt data over een rolling window van 12 maanden en genereert:
- Real-time dashboard met metrics en trends
- Maandelijkse en jaarlijkse rapporten (PDF/Excel)
- Benchmark vergelijkingen tussen ambassades

## Architectuur

```
┌─────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR                              │
│         (coordineert agents, beheert workflow)                   │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│  DATA AGENT   │    │ ANALYSE AGENT │    │ RAPPORT AGENT │
│               │    │               │    │               │
│ - Scraping    │───▶│ - Metrics     │───▶│ - Dashboard   │
│ - Normalisatie│    │ - Trends      │    │ - PDF export  │
│ - Opslag      │    │ - Benchmarks  │    │ - Excel       │
└───────────────┘    └───────────────┘    └───────────────┘
```

## Installatie

```bash
# Clone repository
cd C:\Users\julia.DPIMEDIA\projects\nl-embassy-monitor

# Maak virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Installeer dependencies
pip install -r requirements.txt

# Installeer Playwright browsers (voor Facebook scraping)
playwright install chromium

# Initialiseer database
python main.py init
```

## Gebruik

### CLI Commands

```bash
# Toon help
python main.py --help

# Initialiseer database en laad accounts
python main.py init

# Verzamel nieuwe data voor alle accounts
python main.py collect

# Verzamel data voor specifiek land
python main.py collect --country turkije

# Verzamel 12 maanden historische data
python main.py backfill

# Bereken metrics en benchmarks
python main.py analyze

# Genereer rapporten
python main.py report                    # Maandelijks
python main.py report --type yearly      # Jaarlijks

# Start dashboard
python main.py dashboard

# Toon systeem status
python main.py status

# Toon alle accounts
python main.py accounts
```

### Dashboard

Start het Streamlit dashboard:

```bash
python main.py dashboard
# Of direct:
streamlit run src/outputs/dashboard/app.py
```

Open http://localhost:8501 in je browser.

## Project Structuur

```
nl-embassy-monitor/
├── main.py                 # CLI entry point
├── requirements.txt        # Python dependencies
├── src/
│   ├── agents/             # Multi-agent systeem
│   │   ├── base.py         # Base Agent class
│   │   ├── job_queue.py    # Job queue (SQLite)
│   │   ├── orchestrator.py # Workflow coordinatie
│   │   ├── data_agent.py   # Data verzameling
│   │   ├── analyse_agent.py# Metrics berekening
│   │   └── rapport_agent.py# Output generatie
│   ├── collectors/         # Platform-specifieke scrapers
│   │   ├── base.py         # Base Collector
│   │   ├── instagram.py    # Instagram (instaloader)
│   │   ├── twitter.py      # X/Twitter (Nitter)
│   │   └── facebook.py     # Facebook (Playwright)
│   ├── analysis/           # Analyse modules
│   │   ├── metrics.py      # Engagement berekening
│   │   ├── trends.py       # Trend analyse
│   │   └── benchmarks.py   # Vergelijkingen
│   ├── outputs/
│   │   ├── dashboard/      # Streamlit dashboard
│   │   │   └── app.py
│   │   └── reports/        # Rapport generatie
│   │       ├── pdf_generator.py
│   │       └── excel_export.py
│   ├── database/           # DuckDB database
│   │   ├── connection.py
│   │   ├── models.py
│   │   └── queries.py
│   └── config/
│       ├── settings.py     # Configuratie
│       └── accounts.yaml   # Account definities
└── data/
    ├── embassy_monitor.duckdb  # Database
    ├── job_queue.sqlite        # Job queue
    └── exports/                # Gegenereerde rapporten
```

## Geconfigureerde Accounts

Het systeem monitort accounts in de volgende landen:
- Turkije
- India
- China
- Indonesie
- Filipijnen
- Marokko
- Zuid-Afrika
- Verenigde Arabische Emiraten
- Saoedi-Arabie
- Verenigd Koninkrijk
- Suriname
- Thailand
- Rusland
- Ghana
- Iran
- Koeweit
- Egypte
- Pakistan
- Jordanie
- Oman

Zie `src/config/accounts.yaml` voor de volledige configuratie.

## Technologie Stack

| Component | Technologie |
|-----------|-------------|
| Database | DuckDB |
| Job Queue | SQLite |
| Instagram | instaloader |
| Twitter | Nitter scraping |
| Facebook | Playwright |
| Dashboard | Streamlit |
| PDF | WeasyPrint |
| Excel | openpyxl |
| CLI | Click + Rich |

## Rate Limiting

Het systeem respecteert rate limits per platform:

| Platform | Requests/min | Max/dag |
|----------|--------------|---------|
| Instagram | 10 | 200 |
| Facebook | 5 | 100 |
| Twitter | 20 | 500 |

## Troubleshooting

### Instagram rate limit
Als je geblokkeerd wordt door Instagram, wacht dan 24 uur en probeer opnieuw.

### Nitter instances down
Het systeem roteert automatisch tussen Nitter instances. Bij problemen, update de lijst in `src/config/settings.py`.

### Facebook popup issues
Playwright probeert automatisch popups te sluiten. Bij problemen, controleer of Chromium correct geinstalleerd is.

## Licentie

Intern gebruik - Nederlandse overheid
