"""
ETL Orchestrator for FACO ETL

Real implementation using the complete ETL pipeline modules.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict
from loguru import logger
import pandas as pd

from .config import ETLConfig
from etl.extractor import BigQueryExtractor
from etl.transformer import CobranzaTransformer  
from etl.loader import BigQueryLoader
from etl.business_days import BusinessDaysProcessor


@dataclass
class ETLResult:
    """ETL execution result"""
    success: bool
    records_processed: int
    execution_time: str
    output_tables: List[str]
    data_quality_report: Optional[Dict] = None
    error_message: Optional[str] = None


class ETLOrchestrator:
    """Complete orchestrator that coordinates the real ETL pipeline"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        
        # Initialize ETL components
        self.extractor = BigQueryExtractor(config)
        self.business_days = BusinessDaysProcessor(config)
        self.transformer = CobranzaTransformer(config, self.business_days)
        self.loader = BigQueryLoader(config)
        
        logger.info("🏗️  ETL Orchestrator inicializado con componentes reales")
        
    def run(self) -> ETLResult:
        """Run the complete ETL pipeline"""
        start_time = datetime.now()
        
        try:
            logger.info("🚀 Iniciando pipeline ETL completo")
            
            # Validate configuration
            self.config.validate()
            logger.info(f"✅ Configuración validada - Proyecto: {self.config.project_id}")
            
            # Step 1: Extract data from BigQuery
            logger.info("📥 Paso 1: Extrayendo datos de BigQuery")
            raw_data = self.extractor.extract_all_data()
            
            if not raw_data or all(df.empty for df in raw_data.values()):
                raise ValueError("No se encontraron datos para procesar")
            
            extract_summary = {table: len(df) for table, df in raw_data.items()}
            logger.info(f"✅ Extracción completada: {extract_summary}")
            
            # Step 2: Transform data with business logic
            logger.info("🔄 Paso 2: Transformando datos con lógica de negocio")
            transformed_data = self.transformer.transform_all_data(raw_data)
            
            if not transformed_data or all(df.empty for df in transformed_data.values()):
                raise ValueError("La transformación no generó datos válidos")
            
            transform_summary = {table: len(df) for table, df in transformed_data.items()}
            logger.info(f"✅ Transformación completada: {transform_summary}")
            
            # Step 3: Validate data quality
            logger.info("🔍 Paso 3: Validando calidad de datos")
            quality_report = self.loader.validate_data_quality(transformed_data)
            
            # Check for critical quality issues
            critical_issues = []
            for table, report in quality_report.items():
                if report.get("status") == "EMPTY":
                    critical_issues.append(f"Tabla {table} está vacía")
            
            if critical_issues and not self.config.dry_run:
                raise ValueError(f"Problemas críticos de calidad: {', '.join(critical_issues)}")
            
            logger.info("✅ Validación de calidad completada")
            
            # Step 4: Load data to BigQuery
            logger.info("💾 Paso 4: Cargando datos a BigQuery")
            load_results = self.loader.load_all_tables(transformed_data)
            
            # Step 5: Create table descriptions and finalize
            logger.info("📝 Paso 5: Finalizando carga y documentación")
            self.loader.create_table_descriptions()
            
            # Calculate execution metrics
            execution_time = str(datetime.now() - start_time)
            total_records = sum(len(df) for df in transformed_data.values())
            
            # Prepare success result
            result = ETLResult(
                success=True,
                records_processed=total_records,
                execution_time=execution_time,
                output_tables=list(load_results.keys()),
                data_quality_report=quality_report
            )
            
            # Log success summary
            logger.success("🎉 ETL Pipeline completado exitosamente")
            logger.info(f"📊 Registros procesados: {total_records:,}")
            logger.info(f"⏱️  Tiempo total: {execution_time}")
            logger.info(f"📋 Tablas generadas: {', '.join(result.output_tables)}")
            
            # Log quality summary
            for table, report in quality_report.items():
                status = report.get("status", "UNKNOWN")
                row_count = report.get("row_count", 0)
                issues = report.get("issues", [])
                
                if status == "PASS":
                    logger.info(f"✅ {table}: {row_count:,} registros - Sin problemas")
                elif status == "WARNING":
                    logger.warning(f"⚠️  {table}: {row_count:,} registros - {len(issues)} advertencias")
                elif status == "EMPTY":
                    logger.warning(f"📭 {table}: Tabla vacía")
            
            return result
            
        except Exception as e:
            execution_time = str(datetime.now() - start_time)
            logger.error(f"💥 Error en ETL Pipeline: {e}")
            
            return ETLResult(
                success=False,
                records_processed=0,
                execution_time=execution_time,
                output_tables=[],
                error_message=str(e)
            )
    
    def validate_connectivity(self) -> Dict[str, bool]:
        """Validate connectivity to required services"""
        logger.info("🔗 Validando conectividad")
        
        connectivity_results = {}
        
        # Test BigQuery connectivity
        try:
            self.extractor.client.query("SELECT 1 as test").result()
            connectivity_results["bigquery"] = True
            logger.info("✅ BigQuery: Conectado")
        except Exception as e:
            connectivity_results["bigquery"] = False
            logger.error(f"❌ BigQuery: Error de conexión - {e}")
        
        # Test table existence
        try:
            # Try to access one of the core tables
            test_query = f"""
            SELECT COUNT(*) as row_count 
            FROM `{self.config.project_id}.{self.config.dataset_id}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
            LIMIT 1
            """
            result = self.extractor.client.query(test_query).result()
            connectivity_results["source_tables"] = True
            logger.info("✅ Tablas fuente: Accesibles")
        except Exception as e:
            connectivity_results["source_tables"] = False
            logger.warning(f"⚠️  Tablas fuente: {e}")
        
        # Test business days logic
        try:
            test_date = datetime.now().date()
            business_day = self.business_days.calculate_business_day_of_month(test_date)
            connectivity_results["business_days"] = business_day > 0
            logger.info(f"✅ Días hábiles: Funcionando (hoy es día hábil #{business_day})")
        except Exception as e:
            connectivity_results["business_days"] = False
            logger.error(f"❌ Días hábiles: Error - {e}")
        
        return connectivity_results
    
    def run_data_preview(self, limit: int = 100) -> Dict[str, pd.DataFrame]:
        """Run a limited preview of the ETL process for testing"""
        logger.info(f"👀 Ejecutando preview de datos (límite: {limit})")
        
        try:
            # Temporarily reduce batch size for preview
            original_batch_size = self.config.batch_size
            self.config.batch_size = min(limit, original_batch_size)
            
            # Extract limited data
            raw_data = self.extractor.extract_all_data()
            
            # Limit each DataFrame
            limited_data = {}
            for table, df in raw_data.items():
                if not df.empty:
                    limited_data[table] = df.head(limit)
                else:
                    limited_data[table] = df
            
            # Transform limited data
            preview_data = self.transformer.transform_all_data(limited_data)
            
            # Restore original batch size
            self.config.batch_size = original_batch_size
            
            # Log preview summary
            for table, df in preview_data.items():
                logger.info(f"📋 Preview {table}: {len(df)} registros, {len(df.columns)} columnas")
            
            return preview_data
            
        except Exception as e:
            logger.error(f"❌ Error en preview: {e}")
            return {}
    
    def get_processing_summary(self) -> Dict:
        """Get summary of what will be processed"""
        try:
            # Get calendar data to understand scope
            calendario_query = f"""
            SELECT 
                COUNT(*) as total_periodos,
                MIN(FECHA_ASIGNACION) as fecha_inicio,
                MAX(FECHA_CIERRE) as fecha_fin,
                STRING_AGG(DISTINCT ESTADO) as estados,
                COUNT(DISTINCT ARCHIVO) as archivos_unicos
            FROM `{self.config.project_id}.{self.config.dataset_id}.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v3`
            WHERE DATE_TRUNC(FECHA_ASIGNACION, MONTH) = DATE('{self.config.mes_vigencia}-01')
            AND UPPER(ESTADO) = UPPER('{self.config.estado_vigencia}')
            """
            
            calendar_summary = self.extractor.client.query(calendario_query).to_dataframe()
            
            if calendar_summary.empty:
                return {"error": "No se encontraron períodos para procesar"}
            
            row = calendar_summary.iloc[0]
            
            return {
                "mes_vigencia": self.config.mes_vigencia,
                "estado_vigencia": self.config.estado_vigencia,
                "total_periodos": int(row['total_periodos']),
                "fecha_inicio": row['fecha_inicio'],
                "fecha_fin": row['fecha_fin'],
                "estados": row['estados'],
                "archivos_unicos": int(row['archivos_unicos']),
                "configuracion": {
                    "batch_size": self.config.batch_size,
                    "max_workers": self.config.max_workers,
                    "include_saturdays": self.config.include_saturdays,
                    "dry_run": self.config.dry_run
                }
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo resumen de procesamiento: {e}")
            return {"error": str(e)}