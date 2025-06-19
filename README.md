# FACO ETL 🚀

**Simple Python ETL for Gestión de Cobranza Analytics**

Transforms BigQuery raw data into Looker Studio ready aggregated tables with:
- ✅ Business dimensions aggregation
- ✅ Working days calculations
- ✅ First-time tracking per client
- ✅ Period-over-period comparisons
- ✅ Actions vs unique clients metrics

## 🏃‍♂️ Quick Start

```bash
# Clone and run
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl

# Run with Docker (recommended)
docker-compose up etl

# Or run locally
pip install -r requirements.txt
python main.py --mes 2025-06 --estado abierto
```

## 🐳 Docker Usage

```bash
# Development mode with auto-reload
docker-compose up dev

# Production run
docker-compose up etl

# Interactive shell
docker-compose run --rm shell
```

## ⚙️ Configuration

Set environment variables in `.env` file:

```bash
# BigQuery
GOOGLE_CLOUD_PROJECT=mibot-222814
BIGQUERY_DATASET=BI_USA
GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/key.json

# ETL Settings
COUNTRY_CODE=PE
INCLUDE_SATURDAYS=false
OUTPUT_TABLE_PREFIX=dash_cobranza

# Logging
LOG_LEVEL=INFO
```

## 📊 Output Tables

- `dash_cobranza_agregada`: Main aggregated table for Looker Studio
- `dash_cobranza_comparativas`: Period-over-period comparisons
- `dash_primera_vez_tracking`: First-time interaction tracking

## 🏗️ Architecture

```
📊 BigQuery → 🐍 Python ETL → 📊 BigQuery → 📈 Looker Studio
   (Raw)        (Transform)     (Aggregated)    (Dashboards)
```

## 📋 Requirements

- Python 3.9+
- Google Cloud credentials with BigQuery access
- Docker (optional but recommended)

## 🛠️ Development

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Format code
black src/
flake8 src/
```

## 📄 License

MIT License - see LICENSE file