"""
BigQuery Loader for FACO ETL

Optimized loading of transformed data to BigQuery with partitioning and clustering.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition, TimePartitioning, Clustering
from loguru import logger

from core.config import ETLConfig


class BigQueryLoader:
    """Loads transformed data to BigQuery with optimization"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        
    def load_all_tables(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Load all transformed tables to BigQuery"""
        logger.info("💾 Iniciando carga de tablas a BigQuery")
        
        load_results = {}
        
        for table_type, df in transformed_data.items():
            if df.empty:
                logger.warning(f"⚠️  Tabla {table_type} está vacía, omitiendo carga")
                continue
            
            table_name = self.config.output_tables.get(table_type, f"{self.config.output_table_prefix}_{table_type}")
            
            try:
                result = self._load_table(df, table_name, table_type)
                load_results[table_type] = result
                logger.info(f"✅ {table_type}: {len(df)} registros cargados en {table_name}")
                
            except Exception as e:
                logger.error(f"❌ Error cargando {table_type}: {e}")
                load_results[table_type] = f"ERROR: {str(e)}"
        
        # Create optimized views
        self._create_optimized_views()
        
        return load_results
    
    def _load_table(self, df: pd.DataFrame, table_name: str, table_type: str) -> str:
        """Load individual table with appropriate configuration"""
        
        if self.config.dry_run:
            logger.info(f"🏃‍♂️ DRY-RUN: Simulando carga de {len(df)} registros a {table_name}")
            return f"DRY-RUN: {len(df)} registros simulados"
        
        # Create full table ID
        table_id = f"{self.config.project_id}.{self.config.dataset_id}.{table_name}"
        
        # Configure load job based on table type
        job_config = self._get_load_job_config(table_type)
        
        # Prepare DataFrame for loading
        df_prepared = self._prepare_dataframe_for_bq(df, table_type)
        
        # Load data
        start_time = datetime.now()
        job = self.client.load_table_from_dataframe(
            df_prepared, 
            table_id, 
            job_config=job_config
        )
        
        # Wait for job completion
        job.result()
        
        load_time = datetime.now() - start_time
        
        # Verify load
        table = self.client.get_table(table_id)
        
        logger.info(f"📊 {table_name}: {table.num_rows} filas, {len(table.schema)} columnas, tiempo: {load_time}")
        
        return f"SUCCESS: {table.num_rows} registros en {load_time}"
    
    def _get_load_job_config(self, table_type: str) -> LoadJobConfig:
        """Get optimized load job configuration for table type"""
        
        job_config = LoadJobConfig()
        
        # Write disposition
        if self.config.overwrite_tables:
            job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = WriteDisposition.WRITE_APPEND
        
        # Schema detection
        job_config.autodetect = True
        
        # Table-specific optimizations
        if table_type == 'agregada':
            # Main aggregated table - partitioned by date, clustered by key dimensions
            job_config.time_partitioning = TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="FECHA_SERVICIO"
            )
            job_config.clustering_fields = ["CARTERA", "CANAL", "OPERADOR"]
            
        elif table_type == 'comparativas':
            # Comparative analysis - partitioned by date
            job_config.time_partitioning = TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="FECHA_SERVICIO"
            )
            job_config.clustering_fields = ["CARTERA", "DIA_HABIL_MES"]
            
        elif table_type == 'primera_vez':
            # First-time tracking - partitioned by date
            job_config.time_partitioning = TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="FECHA_PRIMERA_VEZ"
            )
            job_config.clustering_fields = ["cliente", "CARTERA", "CANAL"]
            
        elif table_type == 'base_cartera':
            # Base portfolio - partitioned by assignment date
            job_config.time_partitioning = TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="FECHA_ASIGNACION"
            )
            job_config.clustering_fields = ["CARTERA", "MOVIL_FIJA"]
        
        # Performance settings
        job_config.use_avro_logical_types = True
        job_config.max_bad_records = 100
        
        return job_config
    
    def _prepare_dataframe_for_bq(self, df: pd.DataFrame, table_type: str) -> pd.DataFrame:
        """Prepare DataFrame for BigQuery loading"""
        try:
            df_prepared = df.copy()
            
            # Handle date columns
            date_columns = [col for col in df_prepared.columns if 'FECHA' in col.upper() and '_FORMATO' not in col.upper()]
            for col in date_columns:
                if df_prepared[col].dtype == 'object':
                    try:
                        df_prepared[col] = pd.to_datetime(df_prepared[col], errors='coerce')
                    except:
                        logger.warning(f"No se pudo convertir columna fecha: {col}")
            
            # Handle numeric columns
            numeric_columns = df_prepared.select_dtypes(include=['float64']).columns
            for col in numeric_columns:
                # Replace inf and -inf with None
                df_prepared[col] = df_prepared[col].replace([float('inf'), float('-inf')], None)
            
            # Clean string columns
            string_columns = df_prepared.select_dtypes(include=['object']).columns
            for col in string_columns:
                if col not in date_columns:
                    df_prepared[col] = df_prepared[col].astype(str).replace('nan', None)
            
            # Add metadata columns
            df_prepared['ETL_FECHA_CARGA'] = datetime.now()
            df_prepared['ETL_MES_PROCESO'] = self.config.mes_vigencia
            df_prepared['ETL_ESTADO_VIGENCIA'] = self.config.estado_vigencia
            df_prepared['ETL_VERSION'] = '1.0'
            
            # Ensure required partition column exists
            if table_type == 'agregada' and 'FECHA_SERVICIO' not in df_prepared.columns:
                df_prepared['FECHA_SERVICIO'] = datetime.now().date()
            
            logger.debug(f"DataFrame preparado para {table_type}: {len(df_prepared)} filas, {len(df_prepared.columns)} columnas")
            
            return df_prepared
            
        except Exception as e:
            logger.error(f"Error preparando DataFrame para {table_type}: {e}")
            return df
    
    def _create_optimized_views(self):
        """Create optimized views for Looker Studio"""
        
        if self.config.dry_run:
            logger.info("🏃‍♂️ DRY-RUN: Simulando creación de vistas optimizadas")
            return
        
        views = {
            'looker_studio_main': self._create_main_looker_view(),
            'looker_studio_summary': self._create_summary_looker_view(),
            'looker_studio_trends': self._create_trends_looker_view()
        }
        
        for view_name, view_sql in views.items():
            if view_sql:
                try:
                    self._create_or_replace_view(view_name, view_sql)
                    logger.info(f"✅ Vista creada: {view_name}")
                except Exception as e:
                    logger.error(f"❌ Error creando vista {view_name}: {e}")
    
    def _create_main_looker_view(self) -> str:
        """Create main view optimized for Looker Studio dashboards"""
        return f"""
        CREATE OR REPLACE VIEW `{self.config.project_id}.{self.config.dataset_id}.{self.config.output_table_prefix}_looker_main` AS
        
        SELECT 
            -- Key dimensions for filtering
            FECHA_SERVICIO,
            CARTERA,
            CANAL,
            OPERADOR,
            GRUPO_RESPUESTA,
            MOVIL_FIJA,
            TEMPRANA_ALTAS_CUOTA_FRACCION,
            
            -- Date dimensions for time analysis
            EXTRACT(YEAR FROM FECHA_SERVICIO) as AÑO,
            EXTRACT(MONTH FROM FECHA_SERVICIO) as MES,
            EXTRACT(DAY FROM FECHA_SERVICIO) as DIA,
            FORMAT_DATE('%Y-%m', FECHA_SERVICIO) as AÑO_MES,
            FORMAT_DATE('%A', FECHA_SERVICIO) as DIA_SEMANA,
            
            -- Business day metrics
            DIA_HABIL_MES,
            
            -- Core metrics - Actions vs Clients
            total_interacciones as TOTAL_ACCIONES,
            clientes_unicos_contactados as CLIENTES_UNICOS,
            cuentas_unicas_contactadas as CUENTAS_UNICAS,
            
            -- Effectiveness metrics
            contactos_efectivos as CONTACTOS_EFECTIVOS,
            efectividad_canal as EFECTIVIDAD_PORCENTAJE,
            
            -- First-time metrics
            primera_vez_contactados as PRIMERA_VEZ_CONTACTADOS,
            primera_vez_efectivos as PRIMERA_VEZ_EFECTIVOS,
            ratio_primera_vez as RATIO_PRIMERA_VEZ,
            
            -- Financial metrics
            COALESCE(monto_total_comprometido, 0) as MONTO_COMPROMETIDO,
            COALESCE(cantidad_compromisos, 0) as CANTIDAD_COMPROMISOS,
            COALESCE(monto_promedio_compromiso, 0) as MONTO_PROMEDIO_COMPROMISO,
            
            -- Operational metrics
            duracion_total_minutos as DURACION_TOTAL_MIN,
            duracion_promedio_minutos as DURACION_PROMEDIO_MIN,
            interacciones_por_cliente as INTERACCIONES_POR_CLIENTE,
            
            -- Calculated ratios for Looker
            SAFE_DIVIDE(contactos_efectivos, total_interacciones) * 100 as EFECTIVIDAD_DISPLAY,
            SAFE_DIVIDE(cantidad_compromisos, total_interacciones) * 100 as TASA_COMPROMISO_DISPLAY,
            
            -- ETL metadata
            ETL_FECHA_CARGA,
            ETL_MES_PROCESO,
            ETL_ESTADO_VIGENCIA
            
        FROM `{self.config.project_id}.{self.config.dataset_id}.{self.config.output_tables['agregada']}`
        WHERE FECHA_SERVICIO IS NOT NULL
        """
    
    def _create_summary_looker_view(self) -> str:
        """Create summary view for executive dashboards"""
        return f"""
        CREATE OR REPLACE VIEW `{self.config.project_id}.{self.config.dataset_id}.{self.config.output_table_prefix}_looker_summary` AS
        
        SELECT 
            CARTERA,
            CANAL,
            FORMAT_DATE('%Y-%m', FECHA_SERVICIO) as MES_PROCESO,
            
            -- Aggregated metrics
            SUM(total_interacciones) as TOTAL_ACCIONES_MES,
            COUNT(DISTINCT clientes_unicos_contactados) as CLIENTES_UNICOS_MES,
            SUM(contactos_efectivos) as CONTACTOS_EFECTIVOS_MES,
            SUM(COALESCE(monto_total_comprometido, 0)) as MONTO_COMPROMETIDO_MES,
            
            -- Calculated KPIs
            SAFE_DIVIDE(SUM(contactos_efectivos), SUM(total_interacciones)) * 100 as EFECTIVIDAD_MES,
            SAFE_DIVIDE(SUM(cantidad_compromisos), SUM(total_interacciones)) * 100 as TASA_COMPROMISO_MES,
            SAFE_DIVIDE(SUM(duracion_total_minutos), SUM(total_interacciones)) as DURACION_PROMEDIO_MES,
            
            -- Comparative metrics
            LAG(SUM(total_interacciones)) OVER(
                PARTITION BY CARTERA, CANAL 
                ORDER BY FORMAT_DATE('%Y-%m', FECHA_SERVICIO)
            ) as ACCIONES_MES_ANTERIOR,
            
            LAG(SAFE_DIVIDE(SUM(contactos_efectivos), SUM(total_interacciones))) OVER(
                PARTITION BY CARTERA, CANAL 
                ORDER BY FORMAT_DATE('%Y-%m', FECHA_SERVICIO)
            ) as EFECTIVIDAD_MES_ANTERIOR
            
        FROM `{self.config.project_id}.{self.config.dataset_id}.{self.config.output_tables['agregada']}`
        WHERE FECHA_SERVICIO IS NOT NULL
        GROUP BY CARTERA, CANAL, FORMAT_DATE('%Y-%m', FECHA_SERVICIO)
        ORDER BY MES_PROCESO DESC, CARTERA, CANAL
        """
    
    def _create_trends_looker_view(self) -> str:
        """Create trends view for time series analysis"""
        return f"""
        CREATE OR REPLACE VIEW `{self.config.project_id}.{self.config.dataset_id}.{self.config.output_table_prefix}_looker_trends` AS
        
        SELECT 
            FECHA_SERVICIO,
            DIA_HABIL_MES,
            CARTERA,
            CANAL,
            
            -- Daily aggregates
            SUM(total_interacciones) as ACCIONES_DIA,
            SUM(clientes_unicos_contactados) as CLIENTES_DIA,
            SUM(contactos_efectivos) as EFECTIVOS_DIA,
            
            -- Moving averages (7-day)
            AVG(SUM(total_interacciones)) OVER(
                PARTITION BY CARTERA, CANAL
                ORDER BY FECHA_SERVICIO
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as ACCIONES_PROMEDIO_7D,
            
            AVG(SAFE_DIVIDE(SUM(contactos_efectivos), SUM(total_interacciones))) OVER(
                PARTITION BY CARTERA, CANAL
                ORDER BY FECHA_SERVICIO
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as EFECTIVIDAD_PROMEDIO_7D,
            
            -- Same business day comparison
            LAG(SUM(total_interacciones), 1) OVER(
                PARTITION BY CARTERA, CANAL, DIA_HABIL_MES
                ORDER BY FECHA_SERVICIO
            ) as ACCIONES_MISMO_DIA_HABIL_MES_ANTERIOR,
            
            -- Growth rates
            SAFE_DIVIDE(
                SUM(total_interacciones) - LAG(SUM(total_interacciones)) OVER(
                    PARTITION BY CARTERA, CANAL
                    ORDER BY FECHA_SERVICIO
                ),
                LAG(SUM(total_interacciones)) OVER(
                    PARTITION BY CARTERA, CANAL
                    ORDER BY FECHA_SERVICIO
                )
            ) * 100 as CRECIMIENTO_ACCIONES_DIA_ANTERIOR
            
        FROM `{self.config.project_id}.{self.config.dataset_id}.{self.config.output_tables['agregada']}`
        WHERE FECHA_SERVICIO IS NOT NULL
        GROUP BY FECHA_SERVICIO, DIA_HABIL_MES, CARTERA, CANAL
        ORDER BY FECHA_SERVICIO DESC, CARTERA, CANAL
        """
    
    def _create_or_replace_view(self, view_name: str, view_sql: str):
        """Create or replace a BigQuery view"""
        try:
            query = view_sql.strip()
            
            job_config = bigquery.QueryJobConfig()
            job_config.use_legacy_sql = False
            
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            
        except Exception as e:
            logger.error(f"Error creando vista {view_name}: {e}")
            raise
    
    def create_table_descriptions(self):
        """Add descriptions to tables for better documentation"""
        
        descriptions = {
            self.config.output_tables['agregada']: "Tabla principal agregada por dimensiones de negocio para análisis de gestión de cobranza. Incluye métricas diferenciadas entre acciones totales y clientes únicos.",
            
            self.config.output_tables['comparativas']: "Análisis comparativo período sobre período usando mismos días hábiles para métricas de gestión de cobranza.",
            
            self.config.output_tables['primera_vez']: "Seguimiento detallado de primera vez contactado/gestionado por cliente y dimensión de negocio.",
            
            self.config.output_tables['base_cartera']: "Resumen base de cartera sin gestiones - métricas de asignación y objetivos de recupero.",
            
            f"{self.config.output_table_prefix}_looker_main": "Vista principal optimizada para Looker Studio con métricas de gestión de cobranza agregadas.",
            
            f"{self.config.output_table_prefix}_looker_summary": "Vista resumen ejecutiva para dashboards de alto nivel con KPIs mensuales.",
            
            f"{self.config.output_table_prefix}_looker_trends": "Vista de tendencias para análisis de series temporales con promedios móviles y comparativas."
        }
        
        if self.config.dry_run:
            logger.info("🏃‍♂️ DRY-RUN: Simulando actualización de descripciones de tablas")
            return
        
        for table_name, description in descriptions.items():
            try:
                table_id = f"{self.config.project_id}.{self.config.dataset_id}.{table_name}"
                table = self.client.get_table(table_id)
                table.description = description
                self.client.update_table(table, ["description"])
                logger.debug(f"📝 Descripción actualizada para {table_name}")
            except Exception as e:
                logger.warning(f"No se pudo actualizar descripción para {table_name}: {e}")
    
    def validate_data_quality(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """Validate data quality before loading"""
        
        quality_report = {}
        
        for table_type, df in transformed_data.items():
            if df.empty:
                quality_report[table_type] = {"status": "EMPTY", "issues": ["DataFrame vacío"]}
                continue
            
            issues = []
            
            # Check for null values in key columns
            if table_type == 'agregada':
                key_columns = ['FECHA_SERVICIO', 'CARTERA', 'CANAL']
                for col in key_columns:
                    if col in df.columns and df[col].isnull().any():
                        issues.append(f"Valores nulos en columna clave: {col}")
            
            # Check for negative values in count columns
            count_columns = [col for col in df.columns if 'total_' in col or 'cantidad_' in col]
            for col in count_columns:
                if col in df.columns and (df[col] < 0).any():
                    issues.append(f"Valores negativos en columna de conteo: {col}")
            
            # Check for invalid percentages
            percentage_columns = [col for col in df.columns if 'ratio' in col or 'efectividad' in col or 'tasa' in col]
            for col in percentage_columns:
                if col in df.columns and ((df[col] < 0) | (df[col] > 1)).any():
                    issues.append(f"Porcentajes fuera de rango [0,1] en: {col}")
            
            status = "PASS" if not issues else "WARNING"
            quality_report[table_type] = {
                "status": status,
                "row_count": len(df),
                "column_count": len(df.columns),
                "issues": issues
            }
        
        return quality_report