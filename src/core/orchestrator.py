"""
ETL Orchestrator for FACO ETL

Coordinates the ETL pipeline with a granular, file-by-file processing
strategy for improved resilience and debugging capabilities.
"""

import sys
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict
import pandas as pd
from google.cloud import bigquery
from loguru import logger

from .config import ETLConfig


@dataclass
class ETLResult:
    """Represents the outcome of an ETL execution."""
    success: bool
    records_processed: int = 0
    files_processed: int = 0
    files_failed: int = 0
    execution_time: str = "0s"
    output_tables: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


class ETLOrchestrator:
    """Orchestrates the ETL pipeline, processing data file by file."""

    def __init__(self, config: ETLConfig):
        self.config = config
        self._extractor = None
        self._business_days = None
        self._transformer = None
        self._loader = None
        logger.info(f"🏗️ ETL Orchestrator inicializado.")

    def _initialize_components(self) -> bool:
        """Initializes the real ETL components."""
        if self._extractor:
            return True
        try:
            src_path = Path(__file__).parent.parent
            if str(src_path) not in sys.path:
                sys.path.insert(0, str(src_path))

            from etl.extractor import BigQueryExtractor
            from etl.business_days import BusinessDaysProcessor
            from etl.transformer import CobranzaTransformer
            from etl.loader import BigQueryLoader

            self._extractor = BigQueryExtractor(self.config)
            self._business_days = BusinessDaysProcessor(self.config)
            self._transformer = CobranzaTransformer(self.config, self._business_days)
            self._loader = BigQueryLoader(self.config)

            logger.info("✅ Componentes ETL reales inicializados.")
            return True
        except Exception as e:
            logger.error(f"💥 Error al inicializar componentes ETL: {e}")
            raise

    def run(self) -> ETLResult:
        """Runs the complete ETL pipeline."""
        start_time = datetime.now()
        try:
            logger.info("🚀 Iniciando pipeline ETL...")
            self.config.validate()
            logger.info(f"✅ Configuración validada - Proyecto: {self.config.project_id}, Período: {self.config.mes_vigencia}")
            if self._initialize_components():
                return self._run_real_etl_granular(start_time)
            else:
                # This part is now less likely to be reached unless modules are missing.
                logger.warning("Componentes reales no disponibles. No se puede ejecutar el ETL.")
                return ETLResult(success=False, error_message="Módulos ETL no encontrados.")
        except Exception as e:
            logger.exception("💥 Error fatal no controlado en el pipeline ETL. Proceso abortado.")
            execution_time = str(datetime.now() - start_time)
            return ETLResult(success=False, execution_time=execution_time, error_message=str(e))

    def _run_real_etl_granular(self, start_time: datetime) -> ETLResult:
        """Executes the ETL with refined business logic."""
        logger.info("🎯 Ejecutando pipeline ETL con lógica de negocio refinada.")

        # 1. Connectivity & Execution Plan
        if not self._extractor.test_connectivity():
            raise ConnectionError("Error de conectividad con BigQuery.")
        df_calendario = self._extractor.extract_calendario()
        if df_calendario.empty:
            raise ValueError("No se encontraron períodos en el calendario para procesar.")

        logger.info(f"🗓️  Plan de ejecución: {len(df_calendario)} períodos de asignación a procesar.")

        # 2. Extract Full Month Context (Debt and Payments)
        fechas_trandeuda = pd.to_datetime(df_calendario['FECHA_TRANDEUDA']).unique()
        df_deuda_contexto = self._extractor.extract_contexto_deuda(fechas_trandeuda)

        if not df_deuda_contexto.empty:
            nros_documento_unicos = df_deuda_contexto['nro_documento'].unique().tolist()
            df_pagos_contexto = self._extractor.extract_contexto_pagos(nros_documento_unicos)
        else:
            df_pagos_contexto = pd.DataFrame()

        # 3. Clean Target Tables
        if self.config.overwrite_tables:
            self._loader.clear_tables_for_month()

        total_records_processed, failed_files = 0, []

        # 4. Main Granular Processing Loop
        for index, periodo in df_calendario.iterrows():
            archivo_actual = periodo['ARCHIVO']
            logger.info(f"--- 🔄 Procesando Período {index + 1}/{len(df_calendario)}: {archivo_actual} ---")

            try:
                # 4a. EXTRACT data specific to this period (assignment, management)
                raw_data_periodo = self._extractor.extract_data_for_period(periodo)

                # 4b. FILTER and ADD context from pre-fetched data (in-memory, very fast)
                df_asignacion = raw_data_periodo.get('asignacion')
                if df_asignacion is not None and not df_asignacion.empty:
                    # We need the document numbers from the debt relevant to this specific assignment
                    # This requires joining a bit. Let's assume for now the debt context is broad.
                    # A more precise join would be to link assignment to trandeuda via account number.
                    # For now, we pass the full context and let the transformer handle the joins.
                    raw_data_periodo['trandeuda'] = df_deuda_contexto
                    raw_data_periodo['pagos'] = df_pagos_contexto
                else:
                    logger.warning(f"🟡 Archivo '{archivo_actual}' no tiene datos de asignación. Saltando.")
                    continue

                # 4c. TRANSFORM the data package for this period
                transformed_data_periodo = self._transformer.transform_all_data(raw_data_periodo)

                # 4d. LOAD data for this period in APPEND mode
                if any(not df.empty for df in transformed_data_periodo.values()):
                    logger.info(f"  -> Cargando datos transformados de '{archivo_actual}'...")
                    self._loader.load_all_tables(
                        transformed_data_periodo,
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                    )
                    records_in_period = sum(len(df) for df in transformed_data_periodo.values())
                    total_records_processed += records_in_period
                    logger.success(f"✅ Archivo '{archivo_actual}' procesado y cargado ({records_in_period:,} registros).")
            except Exception as e:
                logger.exception(f"❌ Error fatal procesando el archivo '{archivo_actual}'. Saltando al siguiente.")
                failed_files.append(archivo_actual)
                continue

        # 5. Finalization and Reporting
        execution_time = str(datetime.now() - start_time)
        logger.info("--- 🏁 Fin del procesamiento de todos los archivos. ---")
        if not self.config.dry_run:
            self._loader.optimize_for_looker_studio()
        if failed_files:
            logger.error(f"❌ {len(failed_files)} archivos fallaron: {failed_files}")

        return ETLResult(
            success=not failed_files,
            records_processed=total_records_processed,
            files_processed=len(df_calendario) - len(failed_files),
            files_failed=len(failed_files),
            execution_time=execution_time,
            output_tables=list(self.config.output_tables.values()),
            error_message=f"{len(failed_files)} archivos fallaron" if failed_files else "Proceso completado."
        )