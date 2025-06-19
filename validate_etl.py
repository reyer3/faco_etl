#!/usr/bin/env python3
"""
FACO ETL - Quick Validation & Pre-Flight Check

Validates that all components are working before presentation.
Designed for quick troubleshooting and status verification.
"""

import sys
import os
from pathlib import Path
from loguru import logger
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_basic_imports():
    """Test that all basic dependencies can be imported"""
    logger.info("📦 Testing basic dependencies...")
    
    imports_status = {}
    
    # Test core dependencies
    try:
        import pandas as pd
        imports_status['pandas'] = '✅ OK'
    except ImportError as e:
        imports_status['pandas'] = f'❌ ERROR: {e}'
    
    try:
        from google.cloud import bigquery
        imports_status['google.cloud.bigquery'] = '✅ OK'
    except ImportError as e:
        imports_status['google.cloud.bigquery'] = f'❌ ERROR: {e}'
    
    try:
        from loguru import logger
        imports_status['loguru'] = '✅ OK'
    except ImportError as e:
        imports_status['loguru'] = f'❌ ERROR: {e}'
    
    try:
        import holidays
        imports_status['holidays'] = '✅ OK'
    except ImportError as e:
        imports_status['holidays'] = f'❌ ERROR: {e}'
    
    # Test ETL modules
    logger.info("🧩 Testing ETL modules...")
    
    etl_modules = [
        ('extractor.BigQueryExtractor', 'etl.extractor', 'BigQueryExtractor'),
        ('business_days.BusinessDaysProcessor', 'etl.business_days', 'BusinessDaysProcessor'),
        ('transformer.CobranzaTransformer', 'etl.transformer', 'CobranzaTransformer'),
        ('loader.BigQueryLoader', 'etl.loader', 'BigQueryLoader'),
        ('core.config.ETLConfig', 'core.config', 'ETLConfig'),
        ('core.orchestrator.ETLOrchestrator', 'core.orchestrator', 'ETLOrchestrator')
    ]
    
    for display_name, module_name, class_name in etl_modules:
        try:
            logger.info(f"🔍 Testing {display_name}...")
            module = __import__(module_name, fromlist=[class_name])
            getattr(module, class_name)
            imports_status[display_name] = '✅ OK'
        except ImportError as e:
            imports_status[display_name] = f'❌ IMPORT ERROR: {e}'
        except AttributeError as e:
            imports_status[display_name] = f'❌ ATTRIBUTE ERROR: {e}'
        except Exception as e:
            imports_status[display_name] = f'❌ ERROR: {type(e).__name__}: {e}'
    
    return imports_status

def test_configuration():
    """Test configuration setup"""
    logger.info("⚙️ Testing configuration...")
    
    try:
        from core.config import get_config
        config = get_config()
        
        config_status = {
            'project_id': config.project_id,
            'dataset_id': config.dataset_id,
            'mes_vigencia': config.mes_vigencia,
            'estado_vigencia': config.estado_vigencia,
            'credentials_available': config.has_credentials,
            'is_local_environment': config.is_local_environment,
            'dry_run': config.dry_run,
            'output_tables': list(config.output_tables.values())
        }
        
        if not config.has_credentials:
            logger.warning("⚠️  No se encontraron credenciales de Google Cloud")
            logger.info(config.get_credentials_help())
        
        return config_status, config
        
    except Exception as e:
        logger.error(f"❌ Error en configuración: {e}")
        return {'error': str(e)}, None

def test_bigquery_connectivity(config):
    """Test BigQuery connectivity if possible"""
    logger.info("📡 Testing BigQuery connectivity...")
    
    if not config or not config.has_credentials:
        return {'status': 'SKIPPED', 'reason': 'No credentials available'}
    
    try:
        from etl.extractor import BigQueryExtractor
        extractor = BigQueryExtractor(config)
        
        # Test basic connectivity
        if extractor.test_connectivity():
            logger.success("✅ BigQuery connectivity successful")
            
            # Get some basic stats
            stats = extractor.get_data_availability_summary()
            return {'status': 'SUCCESS', 'stats': stats}
        else:
            return {'status': 'FAILED', 'reason': 'Connectivity test failed'}
            
    except Exception as e:
        logger.warning(f"⚠️  BigQuery connectivity failed: {e}")
        return {'status': 'ERROR', 'error': str(e)}

def test_sample_processing(config):
    """Test a small sample of the ETL process"""
    logger.info("🔬 Testing sample ETL processing...")
    
    if not config:
        return {'status': 'SKIPPED', 'reason': 'No valid configuration'}
    
    try:
        from core.orchestrator import ETLOrchestrator
        
        # Create orchestrator
        orchestrator = ETLOrchestrator(config)
        
        # Get processing summary
        summary = orchestrator.get_processing_summary()
        
        # Validate connectivity
        connectivity = orchestrator.validate_connectivity()
        
        return {
            'status': 'SUCCESS',
            'processing_summary': summary,
            'connectivity': connectivity
        }
        
    except Exception as e:
        logger.error(f"❌ Error en procesamiento de muestra: {e}")
        return {'status': 'ERROR', 'error': str(e)}

def generate_preflight_report(results):
    """Generate a comprehensive pre-flight report"""
    logger.info("📋 Generating pre-flight report...")
    
    report = {
        'timestamp': str(pd.Timestamp.now()),
        'environment': 'LOCAL' if results.get('config', {}).get('is_local_environment', True) else 'DOCKER',
        'overall_status': 'UNKNOWN',
        'ready_for_presentation': False,
        'issues': [],
        'recommendations': [],
        'test_results': results
    }
    
    # Analyze import status
    import_errors = [k for k, v in results.get('imports', {}).items() if 'ERROR' in v]
    if import_errors:
        report['issues'].append(f"Import errors: {', '.join(import_errors)}")
        report['recommendations'].append("Run: pip install -r requirements.txt")
    
    # Analyze configuration
    config_status = results.get('configuration')
    if isinstance(config_status, dict) and 'error' in config_status:
        report['issues'].append(f"Configuration error: {config_status['error']}")
    elif config_status and not config_status.get('credentials_available'):
        report['issues'].append("Google Cloud credentials not configured")
        report['recommendations'].append("Setup credentials using: gcloud auth application-default login")
    
    # Analyze BigQuery connectivity
    bq_status = results.get('bigquery_connectivity', {})
    if bq_status.get('status') == 'ERROR':
        report['issues'].append(f"BigQuery error: {bq_status.get('error')}")
    elif bq_status.get('status') == 'FAILED':
        report['issues'].append("BigQuery connectivity failed")
    
    # Determine overall status
    if not import_errors and config_status and bq_status.get('status') in ['SUCCESS', 'SKIPPED']:
        report['overall_status'] = 'READY'
        report['ready_for_presentation'] = True
    elif not import_errors and config_status:
        report['overall_status'] = 'PARTIAL'
        report['ready_for_presentation'] = True  # Can run in mock mode
        report['recommendations'].append("ETL can run in mock mode for demonstration")
    else:
        report['overall_status'] = 'NOT_READY'
        report['ready_for_presentation'] = False
    
    return report

def print_summary_table(results):
    """Print a nice summary table"""
    
    print("\n" + "="*80)
    print("🚀 FACO ETL - Pre-Flight Check Summary")
    print("="*80)
    
    # Import status
    print("\n📦 DEPENDENCIES:")
    for name, status in results.get('imports', {}).items():
        print(f"   {name:<40} {status}")
    
    # Configuration
    print(f"\n⚙️  CONFIGURATION:")
    config = results.get('configuration', {})
    if isinstance(config, dict) and 'error' not in config:
        print(f"   Project ID:                          {config.get('project_id', 'N/A')}")
        print(f"   Dataset:                             {config.get('dataset_id', 'N/A')}")
        print(f"   Período:                             {config.get('mes_vigencia', 'N/A')} ({config.get('estado_vigencia', 'N/A')})")
        print(f"   Credentials:                         {'✅ Available' if config.get('credentials_available') else '❌ Missing'}")
        print(f"   Environment:                         {'🏠 Local' if config.get('is_local_environment') else '🐳 Docker'}")
    else:
        print(f"   Status:                              ❌ Error in configuration")
    
    # BigQuery connectivity
    print(f"\n📡 BIGQUERY CONNECTIVITY:")
    bq_status = results.get('bigquery_connectivity', {})
    status_emoji = {
        'SUCCESS': '✅',
        'FAILED': '❌', 
        'ERROR': '💥',
        'SKIPPED': '⏭️'
    }
    emoji = status_emoji.get(bq_status.get('status'), '❓')
    print(f"   Status:                              {emoji} {bq_status.get('status', 'Unknown')}")
    
    if bq_status.get('stats'):
        stats = bq_status['stats']
        print(f"   Available tables:                    {stats.get('available_tables', 'N/A')}")
        print(f"   Date range:                          {stats.get('date_range', 'N/A')}")
    
    # Processing capability
    print(f"\n🔬 PROCESSING CAPABILITY:")
    processing = results.get('sample_processing', {})
    if processing.get('status') == 'SUCCESS':
        summary = processing.get('processing_summary', {})
        connectivity = processing.get('connectivity', {})
        
        mode = '🎯 REAL DATA' if connectivity.get('real_components_available') else '🎭 MOCK MODE'
        print(f"   Mode:                                {mode}")
        print(f"   Estimated records:                   {summary.get('estimated_records', 'N/A')}")
        
        real_available = connectivity.get('real_components_available', False)
        bq_conn = connectivity.get('bigquery', False)
        print(f"   Components available:                {'✅' if real_available else '❌'} Real ETL modules")
        print(f"   BigQuery connection:                 {'✅' if bq_conn else '❌'} {bq_conn}")
    
    print(f"\n" + "="*80)

def main():
    """Run complete validation"""
    # Configure simple logging for this script
    logger.remove()
    logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}", level="INFO")
    
    logger.info("🔬 FACO ETL - Pre-Flight Check")
    logger.info("="*40)
    
    results = {}
    
    # Test 1: Basic imports
    results['imports'] = test_basic_imports()
    
    # Test 2: Configuration
    config_result, config = test_configuration()
    results['configuration'] = config_result
    
    # Test 3: BigQuery connectivity
    results['bigquery_connectivity'] = test_bigquery_connectivity(config)
    
    # Test 4: Sample processing
    results['sample_processing'] = test_sample_processing(config)
    
    # Generate report
    report = generate_preflight_report(results)
    
    # Print summary
    print_summary_table(results)
    
    # Final status
    if report['ready_for_presentation']:
        logger.success(f"🎉 READY FOR PRESENTATION! Status: {report['overall_status']}")
        if report['overall_status'] == 'PARTIAL':
            logger.info("ℹ️  ETL puede ejecutarse en modo MOCK para la demostración")
    else:
        logger.error(f"❌ NOT READY - Status: {report['overall_status']}")
        
    if report['recommendations']:
        logger.info("💡 RECOMMENDATIONS:")
        for rec in report['recommendations']:
            logger.info(f"   • {rec}")
    
    # Save detailed report
    report_file = Path('logs') / 'preflight_report.json'
    report_file.parent.mkdir(exist_ok=True)
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info(f"📄 Detailed report saved: {report_file}")
    
    # Exit code for CI/automation
    sys.exit(0 if report['ready_for_presentation'] else 1)

if __name__ == "__main__":
    main()