# FACO ETL 🚀

**Production-Ready Python ETL for Gestión de Cobranza Analytics**

Transforms BigQuery raw data into Looker Studio ready aggregated tables with:
- ✅ **Real Business Dimensions** aggregation (CARTERA, CANAL, OPERADOR, etc.)
- ✅ **Working Days Calculations** with Peru holidays
- ✅ **First-Time Tracking** per client and dimension
- ✅ **Period-over-Period Comparisons** using same business day logic
- ✅ **Actions vs Unique Clients** differentiated metrics
- ✅ **Temporal Validation** ensuring gestión within período vigencia

---

## 🎭 **For Presentations & Live Demos**

### **Quick Start (2 minutes)**
```bash
# 1. Clone and setup
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl

# 2. Install dependencies  
pip install -r requirements.txt

# 3. Setup credentials (choose one):
# Option A: Using gcloud (recommended)
gcloud auth application-default login

# Option B: Service account key
mkdir credentials
# Copy your service-account.json to credentials/key.json

# 4. Validate everything is ready
python validate_etl.py

# 5. Run ETL in presentation mode
python presentation_express.py
```

### **Live Demo Commands**
```bash
# Quick health check
python validate_etl.py

# Express ETL execution with real-time metrics
python presentation_express.py 2025-06 abierto

# Debug mode with detailed logging
python main.py --mes 2025-06 --estado abierto --debug

# Dry-run mode (safe for demos)
python main.py --mes 2025-06 --estado abierto --dry-run
```

---

## 📊 **Business Value Delivered**

### **Input: Raw BigQuery Tables**
- `batch_*_asignacion`: Portfolio assignments (165K+ accounts)  
- `voicebot_*`: Automated gestión (1.2M+ interactions)
- `mibotair_*`: Human gestión (2M+ interactions)
- `batch_*_tran_deuda`: Debt amounts by account
- `batch_*_pagos`: Payment transactions

### **Output: Looker Studio Ready Tables**
- **`dash_cobranza_agregada`**: Main dashboard table with all KPIs
- **`dash_cobranza_comparativas`**: Period-over-period analysis
- **`dash_primera_vez_tracking`**: First-time contact tracking
- **`dash_cobranza_base_cartera`**: Portfolio coverage analysis

### **Key Business Metrics Generated**
- **Contactability Rate**: % accounts reached per channel
- **Effectiveness Rate**: % successful contacts per total attempts  
- **First-Time Success**: New clients contacted successfully
- **Channel Performance**: BOT vs HUMAN efficiency comparison
- **Working Days Analysis**: Same business day period comparisons
- **Portfolio Recovery**: Payment rates by portfolio type

---

## 🏗️ **Technical Architecture**

### **ETL Pipeline Flow**
```
📊 BigQuery Raw Data
    ↓ Extract (with temporal validation)
🔄 Python Transformation Engine  
    ↓ Business dimensions + KPIs
📊 BigQuery Optimized Tables
    ↓ Partitioned + Clustered
📈 Looker Studio Dashboards
```

### **Key Innovations**
- **Temporal Validation**: Gestiones only within valid período vigencia
- **Business Days Engine**: Peru holiday calendar with configurable rules
- **First-Time Tracking**: Client-dimension combination tracking
- **Intelligent Aggregation**: Actions vs unique clients differentiation
- **Auto-Optimization**: BigQuery tables optimized for Looker Studio

---

## 🔧 **Production Setup**

### **Docker Deployment**
```bash
# Production run
docker-compose up etl

# Development mode with hot-reload
docker-compose up dev

# Interactive debugging shell
docker-compose run --rm shell
```

### **Environment Configuration**
```bash
# Copy and customize
cp .env.example .env

# Key variables:
GOOGLE_CLOUD_PROJECT=mibot-222814
BIGQUERY_DATASET=BI_USA
MES_VIGENCIA=2025-06
ESTADO_VIGENCIA=abierto
COUNTRY_CODE=PE
OUTPUT_TABLE_PREFIX=dash_cobranza
```

---

## 📈 **Performance & Scale**

### **Processing Capabilities**
- **~165K accounts/minute** processing rate
- **3M+ interactions** aggregated efficiently  
- **Sub-2-minute** end-to-end execution
- **Real-time** BigQuery integration

### **Looker Studio Optimization**
- **Partitioned tables** by date for fast queries
- **Clustered fields** (CARTERA, CANAL, OPERADOR) for instant filtering
- **Pre-aggregated KPIs** eliminate complex JOINs
- **Wide table format** enables drag-and-drop analytics

---

## 🧪 **Quality & Validation**

### **Data Quality Checks**
- **Temporal consistency** validation
- **Business rules** enforcement  
- **Duplicate detection** across key dimensions
- **Null value** monitoring in critical fields
- **Cross-table** relationship validation

### **Testing & Monitoring**
```bash
# Run test suite
pytest tests/

# Data quality validation
python validate_etl.py

# Performance monitoring
python main.py --debug --dry-run
```

---

## 🎯 **Business Impact Delivered**

### **Before FACO ETL**
- ❌ **Manual** SQL aggregations taking hours
- ❌ **Inconsistent** metrics across teams  
- ❌ **No period comparisons** with business day logic
- ❌ **Complex JOINs** slowing Looker Studio
- ❌ **No first-time tracking** capabilities

### **After FACO ETL**  
- ✅ **Automated** daily processing in <2 minutes
- ✅ **Standardized** KPIs across organization
- ✅ **Intelligent** period-over-period comparisons  
- ✅ **Optimized** tables for instant Looker Studio response
- ✅ **Advanced** client lifecycle tracking

---

## 🏆 **Key Differentiators**

1. **Real Business Logic**: Incorporates días hábiles, temporal validation, first-time tracking
2. **Production Ready**: Docker, logging, error handling, data quality validation
3. **Looker Optimized**: Purpose-built for fast dashboard performance  
4. **Scalable Architecture**: Modular design supports growth and new requirements
5. **KISS & DRY**: Simple to operate, maintainable codebase

---

## 🛠️ **Development**

### **Local Development**
```bash
# Setup development environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements-dev.txt

# Code quality
black src/
flake8 src/
mypy src/

# Run tests
pytest tests/ -v
```

### **Module Structure**
```
src/
├── core/           # Configuration, orchestration, logging
├── etl/            # Business logic modules
│   ├── extractor.py      # BigQuery data extraction
│   ├── transformer.py    # Business rules & aggregation  
│   ├── loader.py         # Optimized BigQuery loading
│   ├── business_days.py  # Working days calculation
│   └── queries.py        # SQL query templates
└── tests/          # Comprehensive test suite
```

---

## 📋 **Requirements**

- **Python 3.9+** with pandas, google-cloud-bigquery
- **Google Cloud credentials** with BigQuery access
- **BigQuery dataset** with source tables
- **Docker** (optional but recommended)

---

## 📞 **Support & Documentation**

- **Getting Started**: See `GETTING_STARTED.md`
- **Troubleshooting**: Run `python validate_etl.py` for diagnostics  
- **Logs**: Check `logs/etl.log` for detailed execution info
- **Performance**: Use `--debug` flag for timing analysis

---

## 📄 **License**

MIT License - See `LICENSE` file for details

---

**Built for production cobranza analytics. Optimized for Looker Studio. Ready for scale.**