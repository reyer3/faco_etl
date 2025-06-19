# 🚀 FACO ETL - Getting Started

## Quick Setup

### Option 1: Using UV (Recommended for Local Development)

```bash
# 1. Clone repository
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl

# 2. Setup Python environment with UV
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies
uv sync

# 4. Install dev dependencies (optional)
uv sync --extra dev

# 5. Setup credentials
mkdir credentials
cp path/to/your/service-account-key.json credentials/key.json

# 6. Configure environment
cp .env.example .env
# Edit .env with your settings

# 7. Run ETL
python main.py --mes 2025-06 --estado abierto
```

### Option 2: Using Docker (Recommended for Production)

```bash
# 1. Clone repository
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl

# 2. Setup credentials
mkdir credentials
cp path/to/your/service-account-key.json credentials/key.json

# 3. Configure environment
cp .env.example .env
# Edit .env with your settings

# 4. Run with Docker
docker-compose up etl
```

## Development Commands

### Using UV

```bash
# Run ETL with different parameters
python main.py --mes 2025-06 --estado abierto
python main.py --mes 2025-05 --estado finalizado --dry-run
python main.py --mes 2025-06 --estado abierto --debug

# Development tools
uv run black src/          # Format code
uv run isort src/          # Sort imports
uv run flake8 src/         # Lint code
uv run mypy src/           # Type checking
uv run pytest tests/       # Run tests

# Install new dependencies
uv add pandas              # Add production dependency
uv add --dev pytest       # Add development dependency
```

### Using Docker

```bash
# Development mode with auto-reload
docker-compose up dev

# Interactive shell for debugging
docker-compose run --rm shell

# Run specific commands
docker-compose run --rm etl python main.py --dry-run
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

## Configuration

### Environment Variables (.env file)

```bash
# BigQuery Configuration
GOOGLE_CLOUD_PROJECT=mibot-222814
BIGQUERY_DATASET=BI_USA
GOOGLE_APPLICATION_CREDENTIALS=./credentials/key.json

# ETL Parameters
MES_VIGENCIA=2025-06
ESTADO_VIGENCIA=abierto
COUNTRY_CODE=PE
INCLUDE_SATURDAYS=false

# Output Configuration
OUTPUT_TABLE_PREFIX=dash_cobranza
OVERWRITE_TABLES=true

# Performance
BATCH_SIZE=10000
MAX_WORKERS=4

# Logging
LOG_LEVEL=INFO
LOG_FILE=./logs/etl.log
```

## Troubleshooting

### Python 3.13+ Compatibility Issues

If you encounter `distutils` or version conflicts:

```bash
# Clear UV cache and reinstall
uv cache clean
rm -rf .venv
uv venv
uv sync
```

### Common Issues

- **Credentials error**: Ensure `credentials/key.json` exists and has BigQuery permissions
- **Date format error**: Use YYYY-MM format for `--mes` parameter  
- **Memory issues**: Reduce `BATCH_SIZE` in `.env` file
- **Import errors**: Make sure you're in the virtual environment: `source .venv/bin/activate`

### Debug Mode

For detailed troubleshooting:

```bash
# Enable debug logging
python main.py --mes 2025-06 --estado abierto --debug

# Check logs
tail -f logs/etl.log

# Interactive debugging with Docker
docker-compose run --rm shell
```

## Development Workflow

1. **Setup**: `uv venv && uv sync --extra dev`
2. **Code**: Edit files in `src/`
3. **Test**: `uv run pytest tests/`
4. **Format**: `uv run black src/ && uv run isort src/`
5. **Lint**: `uv run flake8 src/ && uv run mypy src/`
6. **Run**: `python main.py --dry-run`
7. **Commit**: Standard git workflow

## Performance Tips

- Use `--dry-run` for testing logic without BigQuery writes
- Set appropriate `BATCH_SIZE` based on available memory
- Use `MAX_WORKERS` for parallel processing
- Monitor logs with `tail -f logs/etl.log`