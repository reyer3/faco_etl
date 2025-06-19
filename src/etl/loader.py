"""
BigQuery Loader for FACO ETL

Handles optimized loading of transformed data to BigQuery.
Supports granular, append-only loading for resilient, file-by-file processing.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any

from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning
from loguru import logger

from core.config import ETLConfig


class BigQueryLoader:
    """Load transformed data to BigQuery with optimization for Looker Studio"""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.client = bigquery.Client(
            project=config.project_id,
            credentials=config.credentials_object
        )
        self.dataset = f"{config.project_id}.{config.dataset_id}"

        # Centralized configuration for output tables
        self.table_configs = {
            'agregada': {
                'partition_field': 'FECHA_SERVICIO',
                'clustering_fields': ['CARTERA', 'CANAL', 'OPERADOR'],
                'description': 'Tabla principal agregada con métricas de gestión de cobranza.'
            },
            'comparativas': {
                'partition_field': 'fecha_actual',
                'clustering_fields': ['CARTERA', 'CANAL'],
                'description': 'Comparativas período-sobre-período usando mismo día hábil.'
            },
            'primera_vez': {
                'partition_field': 'FECHA_SERVICIO',
                'clustering_fields': ['CARTERA', 'CANAL', 'cliente'],
                'description': 'Tracking de primera interacción por cliente y dimensión.'
            },
            'base_cartera': {
                'partition_field': 'FECHA_ASIGNACION',
                'clustering_fields': ['CARTERA', 'MOVIL_FIJA'],
                'description': 'Métricas base de cartera sin gestiones para análisis de cobertura.'
            }
        }
        logger.info(f"💾 BigQuery Loader inicializado - Dataset: {self.dataset}")

    def clear_tables_for_month(self):
        """
        Deletes data for the current processing month from target tables.
        This is a critical step to ensure idempotency when using APPEND mode.
        """
        if self.config.dry_run:
            logger.warning("DRY-RUN: Se omitiría la limpieza de tablas para el mes actual.")
            return

        logger.warning(f"🧹 Limpiando datos del mes {self.config.mes_vigencia} de las tablas de destino...")
        month_start_date = f"{self.config.mes_vigencia}-01"

        for table_key, config in self.table_configs.items():
            table_name = self.config.output_tables.get(table_key)
            if not table_name:
                continue

            full_table_id = f"{self.dataset}.{table_name}"
            partition_field = config.get('partition_field')
            if not partition_field:
                logger.warning(f"  - 🟡 Tabla '{table_name}' no está particionada. No se puede limpiar por mes.")
                continue

            delete_query = f"DELETE FROM `{full_table_id}` WHERE DATE_TRUNC({partition_field}, MONTH) = DATE('{month_start_date}')"
            try:
                self.client.get_table(full_table_id)
                self.client.query(delete_query).result()
                logger.info(f"  - ✅ Datos de '{table_name}' para el mes {self.config.mes_vigencia} eliminados.")
            except Exception as e:
                if "Not found" in str(e):
                    logger.info(f"  - 🔵 Tabla '{table_name}' no existe todavía, no se necesita limpieza.")
                else:
                    logger.error(f"  - ❌ No se pudo limpiar la tabla '{table_name}': {e}")
                    raise

    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str, write_disposition: str) -> Dict[str, Any]:
        """
        Loads a DataFrame to a BigQuery table, creating and configuring it on the fly.
        This is the single point of contact for loading data.
        """
        if df.empty:
            return {'status': 'SKIPPED', 'rows_written': 0}

        full_table_name = self.config.output_tables[table_name]
        table_id = f"{self.dataset}.{full_table_name}"
        logger.info(f"  -> Cargando {len(df):,} registros a {table_id} (Modo: {write_disposition})")

        table_config_data = self.table_configs.get(table_name, {})
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=write_disposition,
            create_disposition="CREATE_IF_NEEDED",
        )

        # Configure partitioning and clustering from central config
        partition_field = table_config_data.get('partition_field')
        if partition_field and partition_field in df.columns:
            job_config.time_partitioning = TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field=partition_field)

        clustering_fields = table_config_data.get('clustering_fields', [])
        available_clustering_fields = [field for field in clustering_fields if field in df.columns]
        if available_clustering_fields:
            job_config.clustering_fields = available_clustering_fields

        if self.config.dry_run:
            logger.info(f"    DRY-RUN: Simularía carga a {table_id}")
            return {'status': 'DRY_RUN', 'rows_written': len(df)}

        try:
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()

            table = self.client.get_table(table_id)
            if not table.description:
                 table.description = table_config_data.get('description')
                 self.client.update_table(table, ["description"])

            return {'status': 'SUCCESS', 'rows_written': job.output_rows}

        except Exception as e:
            logger.error(f"    ❌ Error cargando datos a {table_id}: {e}")
            return {'status': 'ERROR', 'rows_written': 0, 'error': str(e)}

    def load_all_tables(self, transformed_data: Dict[str, pd.DataFrame], write_disposition: str) -> Dict[str, Dict[str, Any]]:
        """
        Loads a dictionary of DataFrames to their respective BigQuery tables,
        passing the specified write_disposition to each load job.
        """
        load_results = {}
        for table_name, df in transformed_data.items():
            if table_name in self.config.output_tables:
                load_results[table_name] = self.load_dataframe_to_table(df, table_name, write_disposition)
        return load_results

    def validate_data_quality(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """Validates data quality before loading to BigQuery."""
        logger.info("🔍 Validando calidad de datos...")
        quality_report = {}
        for table_name, df in data_dict.items():
            table_report = {
                'table_name': table_name,
                'row_count': len(df),
                'column_count': len(df.columns) if not df.empty else 0,
                'issues': [],
                'status': 'UNKNOWN'
            }
            if df.empty:
                table_report['status'] = 'EMPTY'
            else:
                required_columns = self._get_required_columns(table_name)
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    table_report['issues'].append(f"Columnas faltantes: {missing_columns}")

                key_columns = self._get_key_columns(table_name)
                for col in key_columns:
                    if col in df.columns and df[col].isnull().sum() > 0:
                        null_pct = (df[col].isnull().sum() / len(df)) * 100
                        if null_pct > 50:
                            table_report['issues'].append(f"Columna {col}: {null_pct:.1f}% valores nulos")

                if table_name == 'agregada':
                    key_dims = ['FECHA_SERVICIO', 'CARTERA', 'CANAL', 'OPERADOR', 'GRUPO_RESPUESTA']
                    available_dims = [dim for dim in key_dims if dim in df.columns]
                    if available_dims and df.duplicated(subset=available_dims).sum() > 0:
                        table_report['issues'].append(f"{df.duplicated(subset=available_dims).sum()} filas duplicadas en dims clave")

                if not table_report['issues']:
                    table_report['status'] = 'PASS'
                else:
                    table_report['status'] = 'WARNING'

            quality_report[table_name] = table_report
        return quality_report

    def _get_required_columns(self, table_name: str) -> List[str]:
        """Gets required columns for data quality checks."""
        return {
            'agregada': ['FECHA_SERVICIO', 'CARTERA', 'CANAL', 'total_interacciones'],
            'comparativas': ['fecha_actual', 'fecha_comparacion', 'CARTERA'],
            'primera_vez': ['cliente', 'FECHA_SERVICIO', 'CARTERA', 'CANAL'],
            'base_cartera': ['CARTERA', 'FECHA_ASIGNACION', 'total_cod_lunas']
        }.get(table_name, [])

    def _get_key_columns(self, table_name: str) -> List[str]:
        """Gets key columns to check for excessive nulls."""
        return {
            'agregada': ['FECHA_SERVICIO', 'CARTERA', 'CANAL'],
            'comparativas': ['fecha_actual', 'CARTERA'],
            'primera_vez': ['cliente', 'CARTERA'],
            'base_cartera': ['CARTERA', 'FECHA_ASIGNACION']
        }.get(table_name, [])

    def create_table_descriptions(self) -> None:
        """Sets detailed, multi-line descriptions for all managed tables."""
        logger.info("📝 Actualizando descripciones de tablas para documentación...")
        descriptions = {
            'agregada': "Tabla principal agregada para dashboards de Looker Studio con métricas de gestión de cobranza.",
            'comparativas': "Tabla de comparativas período-sobre-período usando lógica de mismo día hábil.",
            'primera_vez': "Tabla de tracking de primera interacción por cliente y dimensión.",
            'base_cartera': "Métricas base de cartera sin gestiones para análisis de cobertura."
        }
        for table_key, desc in descriptions.items():
            table_name = self.config.output_tables.get(table_key)
            if not table_name: continue
            table_id = f"{self.dataset}.{table_name}"
            try:
                table = self.client.get_table(table_id)
                table.description = desc
                self.client.update_table(table, ["description"])
                logger.info(f"  - Descripción actualizada para: {table_name}")
            except Exception as e:
                logger.warning(f"  - ⚠️ No se pudo actualizar descripción de {table_name}: {e}")

    def optimize_for_looker_studio(self) -> None:
        """Applies labels to tables for better organization and tracking."""
        logger.info("⚡ Aplicando optimizaciones (etiquetas) para Looker Studio...")
        for table_key in self.config.output_tables:
            table_name = self.config.output_tables[table_key]
            table_id = f"{self.dataset}.{table_name}"
            try:
                query = f"""
                ALTER TABLE `{table_id}` 
                SET OPTIONS (labels=[
                    ('source', 'faco_etl'), 
                    ('optimized_for', 'looker_studio'),
                    ('table_type', '{table_key}'),
                    ('last_updated', '{datetime.now().strftime("%Y%m%d")}')
                ])
                """
                if not self.config.dry_run:
                    self.client.query(query).result()
                    logger.info(f"  - ✅ Etiquetas aplicadas a: {table_name}")
            except Exception as e:
                logger.warning(f"  - ⚠️ Error aplicando optimización a {table_name}: {e}")

    def get_table_statistics(self) -> Dict[str, Dict]:
        """Retrieves and logs statistics for all managed tables."""
        logger.info("📊 Obteniendo estadísticas de tablas de destino...")
        statistics = {}
        for table_key in self.config.output_tables:
            table_name = self.config.output_tables[table_key]
            table_id = f"{self.dataset}.{table_name}"
            try:
                table = self.client.get_table(table_id)
                stats = {
                    'num_rows': table.num_rows,
                    'size_mb': round(table.num_bytes / (1024 * 1024), 2) if table.num_bytes else 0,
                    'last_modified': table.modified.isoformat() if table.modified else None
                }
                statistics[table_name] = stats
                logger.info(f"  - 📋 {table_name}: {stats['num_rows']:,} filas, {stats['size_mb']} MB")
            except Exception as e:
                statistics[table_name] = {'error': str(e)}
        return statistics