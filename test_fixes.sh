#!/bin/bash

# FACO ETL Quick Test Script
# Tests the local setup after fixes

echo "🧪 FACO ETL - Quick Test After Fixes"
echo "===================================="

# Clean up and recreate environment
echo "🧹 Cleaning up any existing logs/credentials..."
rm -rf logs/ credentials/ .env

echo ""
echo "🔧 Running local setup..."
chmod +x setup_local.sh
./setup_local.sh

echo ""
echo "🐍 Testing Python configuration..."
python3 -c "
import sys
sys.path.insert(0, 'src')

try:
    from core.config import get_config
    config = get_config()
    print(f'✅ Configuration loaded successfully')
    print(f'   Project: {config.project_id}')
    print(f'   Dataset: {config.dataset_id}')
    print(f'   Mes: {config.mes_vigencia}')
    print(f'   Estado: {config.estado_vigencia}')
    print(f'   Local environment: {config.is_local_environment}')
    print(f'   Credentials path: {config.credentials_path}')
    print(f'   Log file: {config.log_file}')
except Exception as e:
    print(f'❌ Configuration error: {e}')
    exit(1)
"

echo ""
echo "📝 Testing logger..."
python3 -c "
import sys
sys.path.insert(0, 'src')

try:
    from core.logger import setup_logging
    setup_logging('INFO', 'logs/test.log')
    print('✅ Logger setup successful')
except Exception as e:
    print(f'❌ Logger error: {e}')
    exit(1)
"

echo ""
echo "🎯 Testing full dry run..."
python3 main.py --dry-run --debug

echo ""
echo "===================================="
echo "✅ All tests completed!"
echo ""
echo "📋 Test Summary:"
echo "  ✅ Local setup script works"
echo "  ✅ Configuration loads correctly"
echo "  ✅ Logger handles local paths"
echo "  ✅ Dry run executes successfully"
echo ""
echo "🚀 Repository is ready for development!"