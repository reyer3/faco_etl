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
def main(mes: str, estado: str, dry_run: bool, debug: bool):
    """
    FACO ETL - Gestión de Cobranza Analytics
    
    Transforma datos raw de BigQuery en tablas agregadas para Looker Studio.
    
    Ejemplos:
        python main.py --mes 2025-06 --estado abierto
        python main.py --mes 2025-05 --estado finalizado --dry-run
    """
    try:
        # Setup
        config = setup_environment()
        
        if debug:
            logger.remove()
            logger.add(sys.stdout, level="DEBUG")
        
        logger.info(f"🚀 Iniciando FACO ETL para {mes} - Estado: {estado.upper()}")
        
        # Update config with CLI parameters
        config.mes_vigencia = mes
        config.estado_vigencia = estado.lower()
        config.dry_run = dry_run
        
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