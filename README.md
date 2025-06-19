# FACO ETL 🚀

**Production-Ready Python ETL for Gestión de Cobranza Analytics**

Transforms BigQuery raw data into Looker Studio optimized tables with:
- ✅ Business dimensions aggregation  
- ✅ Working days calculations (Peru calendar)
- ✅ First-time tracking per client
- ✅ Period-over-period comparisons
- ✅ Actions vs unique clients metrics
- ✅ Temporal validation (gestiones within valid periods)

## 🚀 **Quick Start (2 minutes)**

### **Option 1: Automatic Setup**
```bash
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl
chmod +x setup.sh
./setup.sh
```

### **Option 2: Manual Setup**
```bash
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl

# Install dependencies
pip install -r requirements.txt

# Configure environment 
cp .env.example .env
# Edit .env with your settings

# Setup Google Cloud credentials (choose one):
# A) Application Default Credentials (recommended)
gcloud auth application-default login

# B) Service Account Key
mkdir credentials
# Download your service account key to credentials/key.json

# Test everything works
python main.py --test-connectivity
```

## 🔧 **Fix Common Issues**

### **Credentials Error?**
```bash
# Quick fix for credentials
gcloud auth application-default login

# Or check what's missing
python main.py --setup-help

# Or run without BigQuery for testing
python main.py --dry-run
```

### **Import Errors?**
```bash
# Make sure you're in the project root
cd faco_etl
python main.py --test-connectivity
```

## 🎯 **Usage Examples**

```bash
# Test connectivity first
python main.py --test-connectivity

# Get credentials help
python main.py --setup-help

# Run ETL for current month (dry-run)
python main.py --mes 2025-06 --estado abierto --dry-run

# Run real ETL with BigQuery
python main.py --mes 2025-06 --estado abierto

# Process closed period
python main.py --mes 2025-05 --estado finalizado

# Debug mode with detailed logging
python main.py --mes 2025-06 --estado abierto --debug
```

## 📊 **What It Produces**

The ETL generates optimized tables for Looker Studio:

| Table | Description | Use Case |
|-------|-------------|----------|
| `dash_cobranza_agregada` | Main aggregated metrics by business dimensions | Primary dashboard source |
| `dash_cobranza_comparativas` | Period-over-period comparisons with same business day | Trend analysis |
| `dash_primera_vez_tracking` | First-time interaction tracking per client | Customer journey analysis |
| `dash_cobranza_base_cartera` | Portfolio base metrics and financial KPIs | Executive reporting |

### **Key Features:**
- **Diferentiated Metrics**: Actions vs unique clients
- **Temporal Validation**: Only gestiones within valid periods
- **Business Days**: Peru calendar with working day calculations
- **File Date Extraction**: Smart date parsing from filenames
- **BigQuery Optimized**: Partitioned and clustered for performance

## 🏗️ **Architecture**

```
📊 BigQuery Raw Data → 🐍 Python ETL → 📊 BigQuery Aggregated → 📈 Looker Studio
   ├─ calendario                    ├─ Extract         ├─ dash_cobranza_*        └─ Dashboards
   ├─ asignacion                    ├─ Transform       ├─ Partitioned by date    
   ├─ voicebot                      ├─ Business Days   ├─ Clustered by dimensions
   ├─ mibotair                      ├─ First-time      └─ Ready for BI tools
   ├─ trandeuda                     └─ Load            
   └─ pagos                                            
```

## 🐳 **Docker Usage**

```bash
# Production run
docker-compose up etl

# Development with hot-reload  
docker-compose up dev

# Interactive shell for debugging
docker-compose run --rm shell
```

## ⚙️ **Configuration**

All configuration via environment variables in `.env`:

```env
# BigQuery Connection
GOOGLE_CLOUD_PROJECT=mibot-222814
BIGQUERY_DATASET=BI_USA

# ETL Parameters  
MES_VIGENCIA=2025-06
ESTADO_VIGENCIA=abierto
COUNTRY_CODE=PE

# Performance
BATCH_SIZE=10000
MAX_WORKERS=4

# Output
OUTPUT_TABLE_PREFIX=dash_cobranza
OVERWRITE_TABLES=true
```

## 📈 **Business Logic**

### **Temporal Validation**
- Gestiones must be between `FECHA_ASIGNACION` and `FECHA_CIERRE` from calendario
- File dates extracted from nombres (not `creado_el`)
- Pagos use `fecha_pago` column for filtering

### **Relationship Model**
```
CALENDARIO (1) ──┐
                 ├── ARCHIVO ──→ ASIGNACION (*)
                 └── fecha_inicio/fin
                 
ASIGNACION (1) ──→ cod_luna ──→ GESTIONES (*)
                               ├── voicebot  
                               └── mibotair
```

### **Key Metrics**
- **Actions**: Total interactions (each call counts)
- **Unique Clients**: Distinct clients contacted per dimension
- **First Time**: Tracking primera vez por cliente + dimensión
- **Business Days**: Working day of month for comparisons

## 🛠️ **Development**

```bash
# Local development
python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Code quality
black src/
flake8 src/
mypy src/
```

## 📋 **Troubleshooting**

| Issue | Solution |
|-------|----------|
| **Credentials Error** | `gcloud auth application-default login` |
| **Import Error** | Run from project root: `cd faco_etl` |
| **No Data Found** | Check `mes_vigencia` and `estado_vigencia` |
| **BigQuery Permission** | Ensure service account has BigQuery read/write |
| **Memory Issues** | Reduce `BATCH_SIZE` in `.env` |

## 🎉 **Ready for Production**

This ETL is production-ready with:
- ✅ **Error handling** and retry logic  
- ✅ **Logging** with structured output
- ✅ **Monitoring** through detailed metrics
- ✅ **Performance** optimized for BigQuery
- ✅ **Scalability** with configurable batch sizes
- ✅ **Flexibility** for different periods and states

---

**Need help?** Check logs in `logs/etl.log` or run with `--debug` flag.

**For presentations:** Use `--dry-run` to test without BigQuery writes.