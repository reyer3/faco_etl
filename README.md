# FACO ETL 🚀

**Simple Python ETL for Gestión de Cobranza Analytics**

Transforms BigQuery raw data into Looker Studio ready aggregated tables with:
- ✅ Business dimensions aggregation
- ✅ Working days calculations
- ✅ First-time tracking per client
- ✅ Period-over-period comparisons
- ✅ Actions vs unique clients metrics

## 🏃‍♂️ Quick Start

### Option 1: Local Development
```bash
# Clone and setup
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl

# Quick setup (creates directories, .env file)
chmod +x setup_local.sh
./setup_local.sh

# Install dependencies
pip install -r requirements.txt

# Test run
python main.py --dry-run
```

### Option 2: Docker (Recommended)
```bash
# Clone and run
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl

# Setup directories first
chmod +x setup_local.sh
./setup_local.sh

# Run with Docker
docker-compose up etl
```

## ⚙️ Configuration

### Local Environment
The setup script creates a `.env` file automatically. Edit it with your settings:

```bash
# Edit configuration
nano .env

# Key settings to update:
GOOGLE_CLOUD_PROJECT=your-project-id
BIGQUERY_DATASET=your-dataset
# Add your service account key to credentials/key.json
```

### Docker Environment
```bash
# Development mode with auto-reload
docker-compose up dev

# Production run
docker-compose up etl

# Interactive shell for debugging
docker-compose run --rm shell
```

## 🔧 Local Setup Details

The setup script (`./setup_local.sh`) creates:
- `logs/` directory for log files
- `credentials/` directory for Google Cloud keys
- `.env` file from template
- Placeholder `credentials/key.json` (replace with real key)

## 📊 Output Tables

- `dash_cobranza_agregada`: Main aggregated table for Looker Studio
- `dash_cobranza_comparativas`: Period-over-period comparisons
- `dash_primera_vez_tracking`: First-time interaction tracking

## 🏗️ Architecture

```
📊 BigQuery → 🐍 Python ETL → 📊 BigQuery → 📈 Looker Studio
   (Raw)        (Transform)     (Aggregated)    (Dashboards)
```

## 🧪 Testing & Validation

```bash
# Run validation tests
./validate.sh

# Python test suite
python tests/test_validation.py

# Dry run (no BigQuery writes)
python main.py --dry-run --debug
```

## 📋 Usage Examples

```bash
# Basic run for current month
python main.py --mes 2025-06 --estado abierto

# Process closed period
python main.py --mes 2025-05 --estado finalizado

# Dry run with debug logging
python main.py --mes 2025-06 --estado abierto --dry-run --debug

# Docker run with custom parameters
MES_VIGENCIA=2025-05 ESTADO_VIGENCIA=finalizado docker-compose up etl
```

## 🛠️ Development

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Format code
black src/
isort src/

# Type checking
mypy src/
```

## 🔍 Troubleshooting

### Common Issues:

**Permission denied on `/app` directory:**
- You're running locally with Docker paths
- Run `./setup_local.sh` to create proper local structure

**Credentials error:**
- Ensure `credentials/key.json` exists and has BigQuery permissions
- Check `GOOGLE_CLOUD_PROJECT` in `.env` file

**Import errors:**
- Install dependencies: `pip install -r requirements.txt`
- Activate virtual environment if using one

**Date format error:**
- Use YYYY-MM format for `--mes` parameter (e.g., `2025-06`)

### Debug Mode:
```bash
# Enable detailed logging
python main.py --debug --dry-run

# Check configuration
python -c "from src.core.config import get_config; print(get_config().__dict__)"
```

## 📄 License

MIT License - see LICENSE file