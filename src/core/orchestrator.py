"""
ETL Orchestrator for FACO ETL

KISS principle: Simple orchestration with clear separation of concerns.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from loguru import logger

from .config import ETLConfig


@dataclass
class ETLResult:
    """ETL execution result"""
    success: bool
    records_processed: int
    execution_time: str
    output_tables: List[str]
    error_message: Optional[str] = None


class ETLOrchestrator:
    """Simple orchestrator that coordinates ETL pipeline"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        
    def run(self) -> ETLResult:
        """Run the complete ETL pipeline"""
        start_time = datetime.now()
        
        try:
            logger.info("🏗️  Iniciando pipeline ETL")
            
            # Validate configuration
            self.config.validate()
            logger.info(f"✅ Configuración validada - Proyecto: {self.config.project_id}")
            
            # Mock ETL process for now
            records_processed = self._mock_etl_process()
            
            execution_time = str(datetime.now() - start_time)
            
            return ETLResult(
                success=True,
                records_processed=records_processed,
                execution_time=execution_time,
                output_tables=list(self.config.output_tables.values())
            )
            
        except Exception as e:
            execution_time = str(datetime.now() - start_time)
            logger.error(f"💥 Error en ETL: {e}")
            
            return ETLResult(
                success=False,
                records_processed=0,
                execution_time=execution_time,
                output_tables=[],
                error_message=str(e)
            )
    
    def _mock_etl_process(self) -> int:
        """Mock ETL process for initial testing"""
        import time
        
        logger.info(f"📅 Procesando mes: {self.config.mes_vigencia}")
        logger.info(f"📊 Estado vigencia: {self.config.estado_vigencia}")
        
        if self.config.dry_run:
            logger.warning("🏃‍♂️ Modo DRY-RUN activado - No se escribirá a BigQuery")
        
        # Simulate processing steps
        steps = [
            "📥 Extrayendo datos del calendario",
            "🔄 Procesando dimensiones de asignación", 
            "🤖 Agregando métricas de gestión BOT",
            "👨‍💼 Agregando métricas de gestión HUMANA",
            "📊 Calculando KPIs y métricas comparativas"
        ]
        
        for step in steps:
            logger.info(step + "...")
            time.sleep(0.8)  # Simulate processing time
        
        if not self.config.dry_run:
            logger.info("💾 Cargando tablas agregadas a BigQuery...")
            time.sleep(1)
        
        # Mock processed records
        return 42000