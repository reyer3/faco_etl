#!/usr/bin/env python3
"""
FACO ETL - Main Entry Point

Simple orchestrator for cobranza analytics ETL pipeline.
KISS principle: Keep the main script minimal and delegate to modules.
"""

import click
import sys
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.config import get_config
from core.orchestrator import ETLOrchestrator
from core.logger import setup_logging


def setup_environment():
    """Setup environment and logging"""
    load_dotenv()
    config = get_config()
    setup_logging(config.log_level, config.log_file)
    return config


@click.command()
@click.option(
    "--mes",
    default="2025-06",
    help="Mes de vigencia (YYYY-MM)",
    show_default=True
)
@click.option(
    "--estado",
    type=click.Choice(["abierto", "finalizado"], case_sensitive=False),
    default="abierto",
    help="Estado de vigencia",
    show_default=True
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Ejecutar sin escribir a BigQuery"
)
@click.option(
    "--debug",
    is_flag=True,
    help="Modo debug con logging detallado"
)
@click.option(
    "--test-connectivity",
    is_flag=True,
    help="Solo probar conectividad sin ejecutar ETL"
)
@click.option(
    "--quick-summary",
    is_flag=True,
    help="Resumen rápido de datos disponibles (perfecto para presentaciones)"
)
@click.option(
    "--setup-help",
    is_flag=True,
    help="Mostrar ayuda para configurar credenciales"
)
def main(mes: str, estado: str, dry_run: bool, debug: bool, test_connectivity: bool, quick_summary: bool, setup_help: bool):
    """
    FACO ETL - Gestión de Cobranza Analytics
    
    Transforma datos raw de BigQuery en tablas agregadas para Looker Studio.
    
    Ejemplos:
        python main.py --mes 2025-06 --estado abierto
        python main.py --mes 2025-05 --estado finalizado --dry-run
        python main.py --test-connectivity
        python main.py --quick-summary --mes 2025-06  # Para presentaciones
        python main.py --setup-help
    """
    try:
        # Setup básico
        load_dotenv()
        
        if debug:
            logger.remove()
            logger.add(sys.stdout, level="DEBUG")
        else:
            logger.remove()
            logger.add(sys.stdout, level="INFO", 
                      format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")
        
        # Mostrar ayuda de setup si se solicita
        if setup_help:
            try:
                config = get_config(dry_run=True)  # No validar credenciales para mostrar ayuda
                print(config.get_credentials_help())
                return
            except Exception as e:
                logger.error(f"Error obteniendo ayuda de configuración: {e}")
                return
        
        # Para test de conectividad, intentar sin validación estricta primero
        if test_connectivity:
            logger.info(f"🔍 Probando conectividad...")
            try:
                config = get_config(dry_run=True)  # Permitir sin credenciales para test
                config.mes_vigencia = mes
                config.estado_vigencia = estado.lower()
                
                orchestrator = ETLOrchestrator(config)
                connectivity = orchestrator.validate_connectivity()
                
                logger.info("📊 Resultado de conectividad:")
                for service, status in connectivity.items():
                    emoji = "✅" if status else "❌"
                    logger.info(f"   {emoji} {service}: {'OK' if status else 'FAIL'}")
                
                if not connectivity.get("config_valid", False):
                    logger.error("❌ Configuración inválida")
                    return
                
                if not any(connectivity.values()):
                    logger.warning("⚠️  Sin conectividad - revisa credenciales con --setup-help")
                else:
                    logger.success("🎉 Conectividad OK - ETL listo para ejecutar")
                
                return
                
            except Exception as e:
                logger.error(f"❌ Error en test de conectividad: {e}")
                logger.info("💡 Usa --setup-help para configurar credenciales")
                return
        
        # Resumen rápido para presentaciones
        if quick_summary:
            logger.info(f"📊 RESUMEN RÁPIDO para presentación - {mes} ({estado})")
            try:
                config = get_config()
                config.mes_vigencia = mes
                config.estado_vigencia = estado.lower()
                
                # Intentar obtener un resumen rápido de datos
                from etl.extractor import BigQueryExtractor
                extractor = BigQueryExtractor(config)
                
                logger.info("🔍 Verificando datos disponibles...")
                data_summary = extractor.get_data_summary()
                
                if data_summary["disponible"]:
                    logger.success("📈 DATOS DISPONIBLES PARA PRESENTACIÓN:")
                    logger.info("="*50)
                    logger.info(f"📅 Período: {data_summary['fecha_inicio']} → {data_summary['fecha_fin']}")
                    logger.info(f"📁 Archivos de cartera: {len(data_summary['archivos'])}")
                    logger.info(f"⏰ Días de gestión disponibles: {data_summary['dias_gestion']}")
                    logger.info(f"📊 Estado del período: {data_summary['estado']}")
                    
                    # Obtener conteos básicos
                    logger.info("🔢 Obteniendo métricas de resumen...")
                    
                    calendario = extractor.extract_calendario()
                    if not calendario.empty:
                        asignaciones = extractor.extract_asignacion(calendario['ARCHIVO'].tolist())
                        
                        if not asignaciones.empty:
                            logger.info(f"👥 Total cuentas asignadas: {len(asignaciones):,}")
                            logger.info(f"🏢 Cuentas únicas: {asignaciones['cuenta'].nunique():,}")
                            logger.info(f"📱 Teléfonos únicos: {asignaciones['telefono'].nunique():,}")
                            
                            # Distribución por tramo
                            tramos = asignaciones['tramo_gestion'].value_counts()
                            logger.info("📊 Distribución por tramo:")
                            for tramo, count in tramos.items():
                                pct = (count / len(asignaciones)) * 100
                                logger.info(f"   • {tramo}: {count:,} ({pct:.1f}%)")
                            
                            # Distribución por negocio
                            negocios = asignaciones['negocio'].value_counts()
                            logger.info("📊 Distribución por negocio:")
                            for negocio, count in negocios.items():
                                pct = (count / len(asignaciones)) * 100
                                logger.info(f"   • {negocio}: {count:,} ({pct:.1f}%)")
                    
                    logger.info("="*50)
                    logger.success("✅ Datos listos para ETL y dashboards en Looker Studio")
                    logger.info("💡 Para procesar: python main.py --dry-run")
                    
                else:
                    logger.warning(f"⚠️  {data_summary.get('mensaje', 'No hay datos disponibles')}")
                    if 'error' in data_summary:
                        logger.error(f"❌ Error: {data_summary['error']}")
                
                return
                
            except Exception as e:
                logger.error(f"❌ Error en resumen rápido: {e}")
                logger.info("💡 Verifica conectividad con --test-connectivity")
                return
        
        # Configuración normal para ejecución de ETL
        try:
            config = get_config()  # Validación completa
        except ValueError as e:
            if "credentials not found" in str(e).lower():
                logger.error("🔑 Credenciales de Google Cloud no encontradas")
                logger.info("💡 Opciones rápidas:")
                logger.info("   1. gcloud auth application-default login")
                logger.info("   2. python main.py --setup-help")
                logger.info("   3. python main.py --dry-run (para testing sin BigQuery)")
                sys.exit(1)
            else:
                logger.error(f"❌ Error de configuración: {e}")
                sys.exit(1)
        
        logger.info(f"🚀 Iniciando FACO ETL para {mes} - Estado: {estado.upper()}")
        
        # Update config with CLI parameters
        config.mes_vigencia = mes
        config.estado_vigencia = estado.lower()
        config.dry_run = dry_run
        
        # Mostrar resumen
        if debug:
            summary = ETLOrchestrator(config).get_processing_summary()
            logger.debug(f"📋 Resumen de procesamiento: {summary}")
        
        # Run ETL
        orchestrator = ETLOrchestrator(config)
        result = orchestrator.run()
        
        if result.success:
            logger.success(f"✅ ETL completado exitosamente")
            logger.info(f"📊 Registros procesados: {result.records_processed:,}")
            logger.info(f"⏱️  Tiempo total: {result.execution_time}")
            logger.info(f"📋 Tablas generadas: {', '.join(result.output_tables)}")
        else:
            logger.error(f"❌ ETL falló: {result.error_message}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("⚠️  ETL interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"💥 Error inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()