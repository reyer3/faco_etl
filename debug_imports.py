#!/usr/bin/env python3
"""
Debug script to test ETL module imports

Identifica exactamente qué está fallando en los imports.
"""

import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

def test_import(module_name, class_name):
    """Test importing a specific module and class"""
    try:
        print(f"🔍 Testing {module_name}.{class_name}...")
        module = __import__(f"etl.{module_name}", fromlist=[class_name])
        cls = getattr(module, class_name)
        print(f"✅ {module_name}.{class_name} - OK")
        return True
    except Exception as e:
        print(f"❌ {module_name}.{class_name} - ERROR: {e}")
        print(f"   Tipo de error: {type(e).__name__}")
        return False

def test_basic_imports():
    """Test basic Python imports"""
    try:
        import pandas as pd
        print("✅ pandas - OK")
    except Exception as e:
        print(f"❌ pandas - ERROR: {e}")
        
    try:
        from google.cloud import bigquery
        print("✅ google.cloud.bigquery - OK")
    except Exception as e:
        print(f"❌ google.cloud.bigquery - ERROR: {e}")
        
    try:
        from loguru import logger
        print("✅ loguru - OK")
    except Exception as e:
        print(f"❌ loguru - ERROR: {e}")

def main():
    print("🔬 FACO ETL - Debug de Imports")
    print("=" * 40)
    
    print("📦 Testing basic dependencies...")
    test_basic_imports()
    print()
    
    print("🧩 Testing ETL modules...")
    
    # Test each ETL module individually
    modules_to_test = [
        ("extractor", "BigQueryExtractor"),
        ("business_days", "BusinessDaysProcessor"), 
        ("transformer", "CobranzaTransformer"),
        ("loader", "BigQueryLoader")
    ]
    
    failed_modules = []
    for module_name, class_name in modules_to_test:
        if not test_import(module_name, class_name):
            failed_modules.append(f"{module_name}.{class_name}")
    
    print()
    print("=" * 40)
    if failed_modules:
        print(f"❌ MÓDULOS CON PROBLEMAS: {', '.join(failed_modules)}")
        print()
        print("🔧 SOLUCIONES:")
        print("1. Verificar dependencias: pip install -r requirements.txt")
        print("2. Revisar errores específicos arriba")
        print("3. Ejecutar desde directorio correcto")
    else:
        print("✅ TODOS LOS MÓDULOS OK")
        print("🤔 El problema podría ser en el orchestrator...")
        
        # Test orchestrator import
        try:
            print("🔍 Testing orchestrator import...")
            from core.config import ETLConfig
            config = ETLConfig()
            print("✅ Config OK")
            
            # Try the actual import that's failing
            from etl.extractor import BigQueryExtractor
            from etl.business_days import BusinessDaysProcessor
            from etl.transformer import CobranzaTransformer
            from etl.loader import BigQueryLoader
            
            print("✅ ALL ETL IMPORTS OK - El problema está en otro lado")
            
        except Exception as e:
            print(f"❌ Orchestrator import ERROR: {e}")

if __name__ == "__main__":
    main()