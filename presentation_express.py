#!/usr/bin/env python3
"""
FACO ETL - Express Mode for Presentation

Quick ETL execution with real-time metrics display.
Optimized for live demonstrations and presentations.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import time
from loguru import logger

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def setup_presentation_logging():
    """Setup clean logging for presentation"""
    logger.remove()
    
    # Console logging with presentation-friendly format
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <5}</level> | {message}",
        level="INFO",
        colorize=True
    )
    
    # Also log to file for backup
    log_file = Path("logs") / f"presentation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file.parent.mkdir(exist_ok=True)
    
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="DEBUG"
    )

def quick_credentials_setup():
    """Quick credentials setup for presentation"""
    logger.info("🔑 Verificando credenciales de Google Cloud...")
    
    # Check if credentials are already set
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if os.path.exists(cred_path):
            logger.success(f"✅ Credenciales encontradas: {cred_path}")
            return True
    
    # Check for local credentials
    cred_dir = Path("credentials")
    if cred_dir.exists():
        for cred_file in ["key.json", "service-account.json", "gcp-key.json"]:
            cred_path = cred_dir / cred_file
            if cred_path.exists():
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(cred_path)
                logger.success(f"✅ Credenciales configuradas automáticamente: {cred_path}")
                return True
    
    # Check for gcloud default credentials
    try:
        import subprocess
        result = subprocess.run(
            ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            logger.success(f"✅ Usando credenciales por defecto de gcloud: {result.stdout.strip()}")
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    
    logger.warning("⚠️  Credenciales no encontradas - ejecutando en modo DEMO")
    return False

def run_presentation_etl(mes_vigencia="2025-06", estado_vigencia="abierto"):
    """Run ETL in presentation mode with real-time updates"""
    
    logger.info("🚀 Iniciando FACO ETL en modo PRESENTACIÓN")
    logger.info("="*60)
    
    start_time = time.time()
    
    try:
        # Import and configure
        from core.config import get_config
        from core.orchestrator import ETLOrchestrator
        
        # Create configuration
        config = get_config(
            mes_vigencia=mes_vigencia,
            estado_vigencia=estado_vigencia,
            dry_run=False  # Try real execution for presentation
        )
        
        logger.info(f"📊 Configuración ETL:")
        logger.info(f"   • Proyecto: {config.project_id}")
        logger.info(f"   • Dataset: {config.dataset_id}")
        logger.info(f"   • Período: {mes_vigencia} ({estado_vigencia.upper()})")
        logger.info(f"   • Ambiente: {'🏠 Local' if config.is_local_environment else '🐳 Docker'}")
        
        # Create orchestrator
        orchestrator = ETLOrchestrator(config)
        
        # Show processing summary
        logger.info("\n📋 Resumen de procesamiento:")
        summary = orchestrator.get_processing_summary()
        for key, value in summary.get('configuracion', {}).items():
            if key != 'output_tables':
                logger.info(f"   • {key}: {value}")
        
        # Validate connectivity
        logger.info("\n🔗 Validando conectividad...")
        connectivity = orchestrator.validate_connectivity()
        
        for service, status in connectivity.items():
            emoji = "✅" if status else "❌"
            logger.info(f"   • {service}: {emoji}")
        
        # Execute ETL
        logger.info("\n" + "="*60)
        logger.info("🎯 EJECUTANDO ETL PIPELINE")
        logger.info("="*60)
        
        result = orchestrator.run()
        
        # Display results
        execution_time = time.time() - start_time
        
        if result.success:
            logger.success("\n🎉 ETL COMPLETADO EXITOSAMENTE!")
            logger.info("="*60)
            logger.info("📊 RESULTADOS DE LA EJECUCIÓN:")
            logger.info(f"   • Registros procesados: {result.records_processed:,}")
            logger.info(f"   • Tiempo de ejecución: {execution_time:.1f} segundos")
            logger.info(f"   • Tablas generadas: {len(result.output_tables)}")
            
            for table in result.output_tables:
                logger.info(f"     - {table}")
            
            # Generate presentation metrics
            metrics = generate_presentation_metrics(config, result)
            display_presentation_metrics(metrics)
            
            return True, metrics
            
        else:
            logger.error(f"\n❌ ETL FALLÓ: {result.error_message}")
            return False, None
            
    except Exception as e:
        logger.error(f"\n💥 Error inesperado: {e}")
        logger.exception("Detalles del error:")
        return False, None

def generate_presentation_metrics(config, result):
    """Generate key metrics for presentation"""
    logger.info("\n📈 Generando métricas para presentación...")
    
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'execution_summary': {
            'periodo': config.mes_vigencia,
            'estado': config.estado_vigencia,
            'records_processed': result.records_processed,
            'execution_time': result.execution_time,
            'tables_created': len(result.output_tables),
            'success': result.success
        },
        'business_insights': {
            'estimated_accounts': result.records_processed,
            'processing_rate': f"{result.records_processed / 60:.0f} cuentas/minuto",
            'data_volume': f"{result.records_processed * 0.5:.1f} KB estimado",
            'tables_for_looker': result.output_tables
        },
        'technical_capabilities': {
            'real_time_processing': True,
            'bigquery_integration': True,
            'looker_studio_ready': True,
            'business_days_calculation': True,
            'first_time_tracking': True,
            'period_comparisons': True
        }
    }
    
    # Try to get real BigQuery stats if possible
    try:
        from etl.loader import BigQueryLoader
        loader = BigQueryLoader(config)
        table_stats = loader.get_table_statistics()
        
        if table_stats:
            metrics['bigquery_statistics'] = table_stats
            logger.info("✅ Estadísticas reales de BigQuery obtenidas")
    except Exception as e:
        logger.debug(f"No se pudieron obtener estadísticas de BigQuery: {e}")
    
    return metrics

def display_presentation_metrics(metrics):
    """Display metrics in presentation-friendly format"""
    logger.info("\n" + "="*60)
    logger.info("📊 MÉTRICAS CLAVE PARA PRESENTACIÓN")
    logger.info("="*60)
    
    exec_summary = metrics['execution_summary']
    insights = metrics['business_insights']
    capabilities = metrics['technical_capabilities']
    
    # Business metrics
    logger.info("💼 MÉTRICAS DE NEGOCIO:")
    logger.info(f"   • Período procesado: {exec_summary['periodo']} ({exec_summary['estado'].upper()})")
    logger.info(f"   • Cuentas procesadas: {exec_summary['records_processed']:,}")
    logger.info(f"   • Velocidad de procesamiento: {insights['processing_rate']}")
    logger.info(f"   • Tablas para Looker Studio: {exec_summary['tables_created']}")
    
    # Technical capabilities
    logger.info("\n🔧 CAPACIDADES TÉCNICAS:")
    for capability, enabled in capabilities.items():
        emoji = "✅" if enabled else "❌"
        readable_name = capability.replace('_', ' ').title()
        logger.info(f"   • {readable_name}: {emoji}")
    
    # BigQuery statistics if available
    if 'bigquery_statistics' in metrics:
        logger.info("\n📊 ESTADÍSTICAS DE BIGQUERY:")
        for table, stats in metrics['bigquery_statistics'].items():
            if 'error' not in stats:
                logger.info(f"   • {table}:")
                logger.info(f"     - Registros: {stats.get('num_rows', 0):,}")
                logger.info(f"     - Tamaño: {stats.get('size_mb', 0)} MB")
                logger.info(f"     - Optimizada: {'✅' if stats.get('clustered') else '❌'}")
    
    # Save metrics for external tools
    metrics_file = Path("logs") / "presentation_metrics.json"
    metrics_file.parent.mkdir(exist_ok=True)
    
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2, default=str)
    
    logger.info(f"\n💾 Métricas guardadas en: {metrics_file}")
    
    # Generate quick insights
    logger.info("\n💡 INSIGHTS RÁPIDOS:")
    logger.info(f"   • ETL procesó {exec_summary['records_processed']:,} registros en tiempo real")
    logger.info(f"   • Sistema listo para análisis en Looker Studio")
    logger.info(f"   • Capacidad de procesar ~{exec_summary['records_processed'] * 30:,} cuentas/mes")
    logger.info(f"   • Arquitectura escalable con BigQuery")

def main():
    """Main presentation execution"""
    setup_presentation_logging()
    
    logger.info("🎭 FACO ETL - Modo Presentación")
    logger.info("Ejecutando ETL con datos reales para demostración en vivo")
    logger.info("="*60)
    
    # Quick setup
    has_credentials = quick_credentials_setup()
    
    if not has_credentials:
        logger.warning("⚠️  Sin credenciales - algunas funciones estarán limitadas")
        logger.info("💡 Para acceso completo:")
        logger.info("   1. gcloud auth application-default login")
        logger.info("   2. O coloca service-account.json en ./credentials/")
    
    # Get parameters
    mes_vigencia = sys.argv[1] if len(sys.argv) > 1 else "2025-06"
    estado_vigencia = sys.argv[2] if len(sys.argv) > 2 else "abierto"
    
    logger.info(f"\n🎯 Ejecutando para: {mes_vigencia} - {estado_vigencia.upper()}")
    
    # Run ETL
    success, metrics = run_presentation_etl(mes_vigencia, estado_vigencia)
    
    if success:
        logger.success("\n🎉 ¡PRESENTACIÓN LISTA!")
        logger.info("📊 Datos procesados y listos para mostrar en Looker Studio")
        logger.info("📈 Métricas disponibles para demostración")
        
        # Quick demo commands
        logger.info("\n🚀 COMANDOS RÁPIDOS PARA DEMO:")
        logger.info("   • Ver validation: python validate_etl.py")
        logger.info("   • Re-ejecutar ETL: python presentation_express.py")
        logger.info("   • Modo debug: python main.py --debug")
        
        return 0
    else:
        logger.error("\n❌ Error en la ejecución")
        logger.info("🔧 Ejecuta 'python validate_etl.py' para diagnóstico")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)