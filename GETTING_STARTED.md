# 🚀 FACO ETL - Getting Started

## Quick Setup

1. **Clone repository**:
```bash
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl
```

2. **Setup credentials**:
```bash
# Create credentials directory
mkdir credentials

# Copy your Google Cloud service account key
cp path/to/your/service-account-key.json credentials/key.json
```

3. **Configure environment**:
```bash
# Copy and edit environment file
cp .env.example .env
# Edit .env with your settings
```

4. **Run with Docker** (recommended):
```bash
# Production run
docker-compose up etl

# Development mode
docker-compose up dev

# Interactive shell for debugging
docker-compose run --rm shell
```

## Usage Examples

```bash
# Basic run for current month
python main.py --mes 2025-06 --estado abierto

# Process closed period
python main.py --mes 2025-05 --estado finalizado

# Dry run (no writes to BigQuery)
python main.py --mes 2025-06 --estado abierto --dry-run

# Debug mode with detailed logging
python main.py --mes 2025-06 --estado abierto --debug
```

## Output Tables

The ETL generates these tables in BigQuery:

- **`dash_cobranza_agregada`**: Main aggregated table for Looker Studio
- **`dash_cobranza_comparativas`**: Period-over-period comparisons  
- **`dash_primera_vez_tracking`**: First-time interaction tracking
- **`dash_cobranza_base_cartera`**: Base portfolio metrics

## Development

```bash
# Local development setup
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Format code
black src/
isort src/

# Type checking
mypy src/
```

## Troubleshooting

- **Credentials error**: Ensure `credentials/key.json` exists and has BigQuery permissions
- **Date format error**: Use YYYY-MM format for `--mes` parameter
- **Memory issues**: Reduce `BATCH_SIZE` in `.env` file

For more help, check the logs in `logs/etl.log` or run with `--debug` flag.