# Preventive Health Research Pipeline

A production-ready microservices pipeline that automatically discovers, processes, and summarizes preventive health research articles from 50+ curated medical sources, delivering quality-filtered summaries to Slack channels.

## 🎯 Features

- **50+ Curated Sources**: Nature, Lancet, BMJ, PubMed, CDC, NIH, and more
- **Evidence-Based Filtering**: Guardrails based on study type, sample size, and statistical rigor
- **5 Project Areas**: Disease Prevention, Behavioral Protocols, Nutritional Protocols, Government Interventions, Youth Health
- **ScyllaDB Storage**: High-performance NoSQL optimized for time-series article data
- **LLM-Powered Processing**: Classification, quality assessment, and summarization
- **Slack Integration**: Daily digests with quality badges and evidence levels

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR                             │
│              (Daily Scheduler + Pipeline Coordinator)            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   CRAWLER    │    │    DEDUP     │    │     LLM      │
│   SERVICE    │───▶│   SERVICE    │───▶│   SERVICE    │
│              │    │              │    │              │
│  - RSS/Atom  │    │  - URL Hash  │    │  - Filter    │
│  - PubMed    │    │  - DOI Match │    │  - Classify  │
│  - HTML      │    │  - Title Sim │    │  - Summarize │
└──────────────┘    └──────────────┘    └──────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ NOTIFICATION │
                    │   SERVICE    │
                    │              │
                    │  - Slack     │
                    │  - Digests   │
                    └──────────────┘
```

## 📊 Quality Guardrails

### HIGH Priority (Auto-Accept)
- **Sources**: Nature, Lancet, Cell, BMJ, JAHA, PubMed, IHME, CDC, NIH
- **Study Types**: Meta-analyses, Systematic Reviews, RCTs
- **Rigor**: n > 150 (observational), n > 50 per arm (RCT), effect sizes, CIs, p-values

### MEDIUM Priority (Review)
- **Sources**: Harvard, Yale, Stanford, MIT, Peter Attia
- **Study Types**: Observational studies, Mechanistic studies
- **Rigor**: Moderate sample sizes, cautious conclusions

### LOW Priority (Filter Out)
- Non-peer-reviewed content
- Sample size < 20
- Missing statistics
- Sensational language
- Industry-funded with exaggerated claims

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- OpenAI or Anthropic API key
- Slack Bot Token

### Setup

```bash
# Clone repository
git clone <repo>
cd preventive-health-pipeline

# Configure environment
cp .env.example .env
# Edit .env with your API keys

# Start infrastructure first
docker-compose up -d scylladb redis
sleep 60  # Wait for ScyllaDB to initialize

# Start all services
docker-compose up -d

# Trigger manual pipeline run
curl -X POST http://localhost:8000/api/pipeline/run

# View logs
docker-compose logs -f orchestrator
```

## 📁 Project Structure

```
preventive-health-pipeline/
├── services/
│   ├── crawler/          # Multi-source article crawler
│   ├── dedup/            # Deduplication engine
│   ├── llm/              # Quality filter + AI processing
│   ├── notification/     # Slack integration
│   ├── orchestrator/     # Pipeline coordination
│   └── api-gateway/      # Unified API
├── shared/
│   ├── models/           # Data models
│   ├── utils/            # Helpers + DB utilities
│   └── config/           # Configuration management
├── config/
│   └── sources/          # Source configurations
│       ├── journals.yaml
│       ├── pubmed.yaml
│       ├── government.yaml
│       └── academic.yaml
├── docker-compose.yml
└── README.md
```

## 📡 Configured Sources (50+)

### Peer-Reviewed Journals
- Nature, Nature Communications, Nature Food
- Lancet (Diabetes, Respiratory, Rheumatology, SE Asia)
- Cell - Neuron
- BMJ (Research, Clinical Review, Practice)
- JAHA
- European Journal of Medical Research

### PubMed Searches (16 topics)
- Atherosclerosis, Insulin, Liver, Kidney
- Osteoporosis, Bone Density, Mental Health
- Immune System, Anxiety, Vitamins
- Fats, Diarrhea, Air/Water Pollution, Sleep

### Government & Institutional
- NIH, CDC, ECDC
- PIB India
- American Heart Association

### Academic
- Harvard Medicine, Yale Medicine
- Stanford Medicine, MIT News
- Peter Attia MD (Expert)

## ⚙️ Configuration

### Adding New Sources

Edit or create YAML files in `config/sources/`:

```yaml
sources:
  - source_id: new_journal
    name: "New Journal Name"
    url: "https://example.com/articles"
    source_type: journal
    quality_tier: high  # high, medium, low
    crawl_method: rss   # rss, pubmed_api, html_scrape
    rate_limit: 5
    enabled: true
    rss_url: "https://example.com/feed.xml"
```

### Slack Channels

Configure in `config/slack.yaml`:

```yaml
channels:
  disease_prevention: "#disease-prevention"
  behavioral_protocols: "#behavioral-protocols"
  nutritional_protocols: "#nutritional-protocols"
  government_interventions: "#government-interventions"
  youth_health: "#youth-health"
```

## 🔌 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/pipeline/run` | POST | Trigger pipeline |
| `/api/pipeline/status` | GET | Current status |
| `/api/sources` | GET | List sources |
| `/api/sources/{id}/crawl` | POST | Crawl single source |
| `/health` | GET | Service health |

## 📈 Monitoring

```bash
# Enable monitoring stack
docker-compose --profile monitoring up -d

# Access dashboards
# Grafana: http://localhost:3000
# ScyllaDB metrics: http://localhost:9180/metrics
```

## 🧪 Development

```bash
# Run single service locally
cd services/crawler
pip install -r requirements.txt
uvicorn src.main:app --reload --port 8001
```

## 📝 License

MIT License