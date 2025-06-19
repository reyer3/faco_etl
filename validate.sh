#!/bin/bash

# FACO ETL Quick Validation Script
# Usage: ./validate.sh

echo "🚀 FACO ETL - Quick Validation"
echo "================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Python is available
echo "🐍 Checking Python..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✅ $PYTHON_VERSION found${NC}"
else
    echo -e "${RED}❌ Python 3 not found${NC}"
    exit 1
fi

# Check if Docker is available
echo ""
echo "🐳 Checking Docker..."
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version)
    echo -e "${GREEN}✅ $DOCKER_VERSION found${NC}"
else
    echo -e "${YELLOW}⚠️  Docker not found (optional)${NC}"
fi

# Check if docker-compose is available
echo ""
echo "🔧 Checking Docker Compose..."
if command -v docker-compose &> /dev/null; then
    COMPOSE_VERSION=$(docker-compose --version)
    echo -e "${GREEN}✅ $COMPOSE_VERSION found${NC}"
else
    echo -e "${YELLOW}⚠️  Docker Compose not found (optional)${NC}"
fi

# Check project structure
echo ""
echo "📁 Checking project structure..."

required_files=(
    "main.py"
    "requirements.txt"
    "Dockerfile"
    "docker-compose.yml"
    ".env.example"
    "src/core/config.py"
    "src/core/logger.py"
    "src/core/orchestrator.py"
)

for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✅ $file${NC}"
    else
        echo -e "${RED}❌ $file missing${NC}"
    fi
done

# Test Python imports (if dependencies are installed)
echo ""
echo "📦 Testing Python imports..."
python3 -c "
import sys
try:
    import click
    print('✅ click')
except ImportError:
    print('❌ click (pip install click)')

try:
    import pandas
    print('✅ pandas')
except ImportError:
    print('❌ pandas (pip install pandas)')

try:
    from loguru import logger
    print('✅ loguru')
except ImportError:
    print('❌ loguru (pip install loguru)')

try:
    from dotenv import load_dotenv
    print('✅ python-dotenv')
except ImportError:
    print('❌ python-dotenv (pip install python-dotenv)')
"

# Run validation test if possible
echo ""
echo "🧪 Running validation tests..."
if [ -f "tests/test_validation.py" ]; then
    python3 tests/test_validation.py
else
    echo -e "${RED}❌ test_validation.py not found${NC}"
fi

echo ""
echo "================================"
echo "🎯 Next Steps:"
echo ""
echo "1. Install dependencies:"
echo "   pip install -r requirements.txt"
echo ""
echo "2. Setup credentials:"
echo "   mkdir credentials"
echo "   cp your-service-account.json credentials/key.json"
echo ""
echo "3. Configure environment:"
echo "   cp .env.example .env"
echo "   # Edit .env with your settings"
echo ""
echo "4. Test run:"
echo "   python main.py --mes 2025-06 --estado abierto --dry-run"
echo ""
echo "5. Docker run:"
echo "   docker-compose up etl"
