# Preventive Health Research Pipeline

An automated microservices pipeline that discovers, extracts, classifies, and summarises preventive health research articles from 44+ curated medical and academic sources. Processed articles are delivered as quality-filtered summaries to project-specific Slack channels on a recurring schedule.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Services](#services)
- [Pipeline Flow](#pipeline-flow)
- [Quality Guardrails](#quality-guardrails)
- [Configured Sources](#configured-sources)
- [Project Areas](#project-areas)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Monitoring](#monitoring)
- [Development](#development)
- [Project Structure](#project-structure)
- [License](#license)

---

## Overview

The pipeline runs every 30 minutes during business hours (9 AM -- 7 PM IST) and performs the following:

1. Crawls 44+ sources (RSS feeds, PubMed API, HTML scraping) for new research articles.
2. Deduplicates articles using URL hashes, DOI matching, PMID matching, and title similarity.
3. Extracts full text via PubMed Central API, PDF parsing, HTML extraction, or Unpaywall open-access lookup.
4. Classifies articles into project areas and sub-topics using an LLM.
5. Generates structured summaries with evidence-level ratings.
6. Delivers results to Slack channels (max 40 articles per run, sorted by evidence level).

All articles are persisted in ScyllaDB, cached in Redis, streamed to Kafka, and backed up to S3.

---

## Architecture

```
                          +------------------+
                          |   API Gateway    |  :8000
                          +--------+---------+
                                   |
                          +--------+---------+
                          |   Orchestrator   |  :8006
                          |  (Cron + Coord)  |
                          +--------+---------+
                                   |
         +------------+------------+------------+------------+
         |            |            |            |            |
   +-----+----+ +-----+----+ +----+-----+ +----+-----+ +----+------+
   |  Crawler  | |   Dedup  | |Extraction| |   LLM    | |Notification|
   |  :8001    | |  :8002   | |  :8003   | |  :8004   | |   :8005    |
   +-----+----+ +-----+----+ +----+-----+ +----+-----+ +----+------+
         |            |            |            |            |
   +-----+------------+------------+------------+------------+------+
   |                        Infrastructure                          |
   |   ScyllaDB :9042      Redis :6379      Kafka      S3           |
   +----------------------------------------------------------------+
```

---

## Services

| Service        | Port | Description                                              |
|----------------|------|----------------------------------------------------------|
| API Gateway    | 8000 | Unified REST API, proxies to internal services            |
| Crawler        | 8001 | Adaptive multi-method crawler (RSS, PubMed API, HTML)     |
| Dedup          | 8002 | Deduplication via URL hash, DOI, PMID, title similarity   |
| Extraction     | 8003 | Full-text extraction (PMC, PDF, HTML, Unpaywall)          |
| LLM            | 8004 | Classification, evidence assessment, summarisation        |
| Notification   | 8005 | Slack message formatting and delivery                     |
| Orchestrator   | 8006 | Pipeline scheduling and step coordination                 |
| ScyllaDB       | 9042 | Primary article storage (time-series optimised)           |
| Redis          | 6379 | Caching, rate limiting, dedup index                       |
| Prometheus     | 9090 | Metrics collection (optional)                             |
| Grafana        | 3000 | Dashboards and alerting (optional)                        |

---

## Pipeline Flow

```
Crawl  -->  Dedup  -->  Extract Full Text  -->  LLM Process  -->  Store  -->  Notify
 |            |              |                     |              |            |
 |  RSS       |  URL hash    |  PMC API            |  Classify    |  ScyllaDB  |  Slack
 |  PubMed    |  DOI match   |  PDF parse           |  Summarise   |  S3        |  (9AM-7PM IST)
 |  HTML      |  Title sim   |  HTML extract        |  Evidence    |  Kafka     |  Max 40/run
 |            |  PMID match  |  Unpaywall OA        |  scoring     |            |
```

**Incomplete article recovery**: Articles that fail mid-pipeline are automatically retried in subsequent runs. The orchestrator loads articles from the last 3 days that have not reached the final "notified" status and resumes processing from the appropriate stage.

---

## Quality Guardrails

### High Priority (Auto-Accept)

- **Sources**: Nature, Lancet, Cell, BMJ, JAHA, PubMed, IHME, CDC, NIH
- **Study Types**: Meta-analyses, systematic reviews, randomised controlled trials
- **Rigor**: n > 150 (observational), n > 50 per arm (RCT), effect sizes, confidence intervals, p-values

### Medium Priority (Review)

- **Sources**: Harvard, Yale, Stanford, MIT, expert commentary
- **Study Types**: Observational studies, mechanistic studies
- **Rigor**: Moderate sample sizes, cautious conclusions

### Low Priority (Filtered Out)

- Non-peer-reviewed content
- Sample size < 20
- Missing statistical indicators
- Sensational language or exaggerated claims
- Industry-funded studies with overstated findings

Evidence levels are capped at 3/5 for abstract-only articles (where full text could not be extracted).

---

## Configured Sources

### Peer-Reviewed Journals (19 sources)

Nature, Nature Communications, Nature Food, Cell - Neuron, Lancet (Diabetes & Endocrinology, Respiratory, Rheumatology, South East Asia), BMJ (Research, Clinical Review, Practice), JAHA, European Journal of Medical Research, IHME, Obesity & Energetics.

### PubMed Targeted Searches (16 topics)

Atherosclerosis, insulin resistance, liver disease, kidney disease, osteoporosis, bone density, mental health, immune system, anxiety, vitamins, dietary fats, gastrointestinal infections, air pollution, water pollution, sleep.

### Government and Institutional (6 sources)

NIH News, CDC MMWR, ECDC, American Heart Association, PIB India.

### Academic and Expert Sources (9 sources)

Harvard Medicine, Harvard Public Health, Yale Medicine, MIT News (Health), Stanford Medicine, Peter Attia MD.

---

## Project Areas

Articles are classified into five project areas, each mapped to a dedicated Slack channel:

| Area                      | Slack Channel              | Focus                                                  |
|---------------------------|----------------------------|--------------------------------------------------------|
| Disease Prevention        | #disease-prevention        | Heart disease, diabetes, cancer, neurodegenerative, musculoskeletal, mental health |
| Behavioral Protocols      | #behavioral-protocols      | Exercise, sleep, meditation, breathwork, risky behaviours |
| Nutritional Protocols     | #nutritional-protocols     | Protein, carbohydrates, fats, hydration, diets, supplements |
| Government Interventions  | #government-interventions  | Food safety, air/water pollution, toxins, public health communications, funding |
| Youth Health              | #youth-health              | School/college programs, regional initiatives            |

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- OpenAI or Anthropic API key (for LLM processing)
- Slack Bot Token (for notifications)
- PubMed API key (optional, increases rate limits)

### Environment Variables

Create a `.env` file in the project root:

```env
# LLM
LLM_PROVIDER=openai
LLM_API_KEY=sk-...
LLM_MODEL=gpt-4-turbo

# Slack
SLACK_BOT_TOKEN=xoxb-...
SLACK_SIGNING_SECRET=...

# PubMed (optional, improves rate limits)
PUBMED_API_KEY=...

# AWS S3 (for backups)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-south-1
S3_BUCKET=pulse-narrative

# Kafka
KAFKA_BROKER_PROD=...
KAFKA_BROKER_BETA=...

# ScyllaDB (production/beta, if not using local Docker)
SCYLLA_HOST_PROD_LST=...
SCYLLA_USERNAME_PROD=...
SCYLLA_PASSWORD_PROD=...
```

### Deployment

```bash
# Clone the repository
git clone <repository-url>
cd preventive-health-pipeline

# Start infrastructure (wait for ScyllaDB to initialise)
docker-compose up -d scylladb redis
sleep 60

# Start all services
docker-compose up -d

# Trigger a manual pipeline run
curl -X POST http://localhost:8000/api/pipeline/run

# View pipeline logs
docker-compose logs -f orchestrator
```

---

## Configuration

### Adding a New Source

Create or edit a YAML file under `config/sources/`:

```yaml
sources:
  - source_id: example_journal
    name: "Example Journal"
    url: "https://example.com/articles"
    source_type: journal        # journal, database, government, academic, expert_blog
    quality_tier: high           # high, medium, low
    crawl_method: rss            # rss, pubmed_api, html_scrape, adaptive
    rate_limit: 5
    enabled: true
    rss_url: "https://example.com/feed.xml"
```

### Slack Channel Mapping

Edit `config/slack.yaml`:

```yaml
channels:
  disease_prevention: "#disease-prevention"
  behavioral_protocols: "#behavioral-protocols"
  nutritional_protocols: "#nutritional-protocols"
  government_interventions: "#government-interventions"
  youth_health: "#youth-health"
```

### Pipeline Schedule

Edit `config/pipeline.yaml`:

```yaml
schedule: "*/30 3-13 * * *"     # Every 30 min, 9 AM - 7 PM IST
max_articles_per_run: 500
max_articles_per_source: 50
relevance_threshold: 60.0
dedup_similarity_threshold: 0.80
```

---

## API Reference

All endpoints are accessible through the API Gateway at `http://localhost:8000`.

### Pipeline

| Method | Endpoint                       | Description                      |
|--------|--------------------------------|----------------------------------|
| POST   | `/api/pipeline/run`            | Trigger a pipeline run           |
| GET    | `/api/pipeline/status`         | Get current pipeline status      |
| GET    | `/api/pipeline/status/{run_id}`| Get status of a specific run     |

### Sources

| Method | Endpoint                        | Description                     |
|--------|---------------------------------|---------------------------------|
| GET    | `/api/sources`                  | List all configured sources     |
| POST   | `/api/sources/{source_id}/crawl`| Crawl a single source           |

### Health

| Method | Endpoint  | Description                                |
|--------|-----------|--------------------------------------------|
| GET    | `/health` | Aggregated health check across all services|

---

## Monitoring

Prometheus and Grafana are available as optional services:

```bash
docker-compose --profile monitoring up -d
```

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (default password: `admin`)
- ScyllaDB metrics: `http://localhost:9180/metrics`

---

## Development

### Running a Single Service Locally

```bash
cd services/crawler
pip install -r requirements.txt
uvicorn main:app --reload --port 8001
```

### Running Tests

```bash
cd tests
python -m pytest
```

---

## Project Structure

```
preventive-health-pipeline/
|-- config/
|   |-- pipeline.yaml              # Schedule, limits, thresholds
|   |-- slack.yaml                 # Channel mappings
|   |-- sources/
|       |-- journals.yaml          # Journal RSS sources
|       |-- pubmed.yaml            # PubMed API search topics
|       |-- government.yaml        # Government and institutional sources
|       |-- academic.yaml          # Academic and expert sources
|-- services/
|   |-- api-gateway/               # Unified REST API (port 8000)
|   |-- crawler/                   # Adaptive multi-method crawler
|   |-- dedup/                     # Deduplication engine
|   |-- extraction/                # Full-text extraction (PMC, PDF, HTML, Unpaywall)
|   |-- llm/                       # LLM classification and summarisation
|   |-- notification/              # Slack message delivery
|   |-- orchestrator/              # Pipeline scheduling and coordination
|-- shared/
|   |-- models/                    # Pydantic data models (Article, CrawlJob, etc.)
|   |-- utils/                     # Database managers, helpers, hashing
|   |-- config/                    # Settings and configuration loader
|-- tests/                         # Integration and unit tests
|-- docker-compose.yml
|-- .env                           # Environment variables (not committed)
|-- README.md
```

---

## License

MIT License
