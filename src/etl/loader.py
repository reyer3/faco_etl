"""
BigQuery Loader for FACO ETL

Handles optimized loading of transformed data to BigQuery with
table optimization for Looker Studio performance.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, TimePartitioning
from loguru import logger

from core.config import ETLConfig


class BigQueryLoader:
    """Load transformed data to BigQuery with optimization for Looker Studio"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        self.dataset = f"{config.project_id}.{config.dataset_id}"
        
        # Table schemas and optimization settings
        self.table_configs = {
            'agregada': {
                'partition_field': 'FECHA_SERVICIO',
                'clustering_fields': ['CARTERA', 'CANAL', 'OPERADOR'],
                'description': 'Tabla principal agregada para dashboards de Looker Studio con métricas de gestión de cobranza'
            },
            'comparativas': {
                'partition_field': 'fecha_actual',
                'clustering_fields': ['CARTERA', 'CANAL'],
                'description': 'Comparativas período-sobre-período usando mismo día hábil'
            },
            'primera_vez': {
                'partition_field': 'FECHA_SERVICIO',
                'clustering_fields': ['CARTERA', 'CANAL', 'cliente'],
                'description': 'Tracking de primera interacción por cliente y dimensión'
            },
            'base_cartera': {
                'partition_field': 'FECHA_ASIGNACION',
                'clustering_fields': ['CARTERA', 'MOVIL_FIJA'],
                'description': 'Métricas base de cartera sin gestiones para análisis de cobertura'
            }
        }
        
        logger.info(f"💾 BigQuery Loader inicializado - Dataset: {self.dataset}")
    
    def validate_data_quality(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """
        Validate data quality before loading to BigQuery.
        
        Returns quality report for each table.
        """
        logger.info("🔍 Validando calidad de datos")
        
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
                table_report['issues'].append('Tabla vacía')
            else:
                # Check for required columns based on table type
                required_columns = self._get_required_columns(table_name)
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    table_report['issues'].append(f"Columnas faltantes: {missing_columns}")
                
                # Check for null values in key columns
                key_columns = self._get_key_columns(table_name)
                for col in key_columns:
                    if col in df.columns:
                        null_count = df[col].isnull().sum()
                        if null_count > 0:
                            null_pct = (null_count / len(df)) * 100
                            if null_pct > 50:  # More than 50% nulls is concerning
                                table_report['issues'].append(f"Columna {col}: {null_pct:.1f}% valores nulos")
                
                # Check for duplicate rows
                if table_name == 'agregada':
                    # For aggregated table, check for duplicates on key dimensions
                    key_dims = ['FECHA_SERVICIO', 'CARTERA', 'CANAL', 'OPERADOR', 'GRUPO_RESPUESTA']
                    available_dims = [dim for dim in key_dims if dim in df.columns]
                    if available_dims:
                        duplicate_count = df.duplicated(subset=available_dims).sum()
                        if duplicate_count > 0:
                            table_report['issues'].append(f"{duplicate_count} filas duplicadas en dimensiones clave")
                
                # Determine overall status
                if not table_report['issues']:
                    table_report['status'] = 'PASS'
                elif len(table_report['issues']) <= 2:
                    table_report['status'] = 'WARNING'
                else:
                    table_report['status'] = 'FAIL'
            
            quality_report[table_name] = table_report
            
            # Log table status
            status_emoji = {
                'PASS': '✅',
                'WARNING': '⚠️',
                'FAIL': '❌',
                'EMPTY': '📭'
            }
            
            emoji = status_emoji.get(table_report['status'], '❓')
            logger.info(f"{emoji} {table_name}: {len(df):,} registros - {table_report['status']}")
            
            if table_report['issues']:
                for issue in table_report['issues']:
                    logger.warning(f"    ⚠️  {issue}")
        
        return quality_report
    
    def _get_required_columns(self, table_name: str) -> List[str]:
        """Get required columns for each table type"""
        required_columns = {
            'agregada': ['FECHA_SERVICIO', 'CARTERA', 'CANAL', 'total_interacciones'],
            'comparativas': ['fecha_actual', 'fecha_comparacion', 'CARTERA'],
            'primera_vez': ['cliente', 'FECHA_SERVICIO', 'CARTERA', 'CANAL'],
            'base_cartera': ['CARTERA', 'FECHA_ASIGNACION', 'total_cod_lunas']
        }
        return required_columns.get(table_name, [])
    
    def _get_key_columns(self, table_name: str) -> List[str]:
        """Get key columns that shouldn't have too many nulls"""
        key_columns = {
            'agregada': ['FECHA_SERVICIO', 'CARTERA', 'CANAL'],
            'comparativas': ['fecha_actual', 'CARTERA'],
            'primera_vez': ['cliente', 'CARTERA'],
            'base_cartera': ['CARTERA', 'FECHA_ASIGNACION']
        }
        return key_columns.get(table_name, [])
    
    def create_optimized_table(self, table_name: str, df: pd.DataFrame) -> str:
        """Create BigQuery table with optimal configuration for Looker Studio"""
        
        full_table_name = f"{self.config.output_tables[table_name]}"
        table_id = f"{self.dataset}.{full_table_name}"
        
        logger.info(f"🏗️  Creando tabla optimizada: {table_id}")
        
        # Get table configuration
        table_config = self.table_configs.get(table_name, {})
        
        # Create table schema
        table = bigquery.Table(table_id)
        
        # Set partitioning if specified
        partition_field = table_config.get('partition_field')
        if partition_field and partition_field in df.columns:
            table.time_partitioning = TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
            logger.info(f"📅 Particionado por: {partition_field}")
        
        # Set clustering if specified
        clustering_fields = table_config.get('clustering_fields', [])
        available_clustering_fields = [field for field in clustering_fields if field in df.columns]
        if available_clustering_fields:
            table.clustering_fields = available_clustering_fields
            logger.info(f"🔗 Clustering por: {available_clustering_fields}")
        
        # Set table description
        table.description = table_config.get('description', f'Tabla {table_name} de FACO ETL')
        
        # Create or update table
        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ Tabla creada/actualizada: {table_id}")
            return table_id
        except Exception as e:
            logger.error(f"❌ Error creando tabla {table_id}: {e}")
            raise
    
    def load_dataframe_to_table(self, df: pd.DataFrame, table_id: str, 
                               write_disposition: str = "WRITE_TRUNCATE") -> Dict[str, Any]:
        """Load DataFrame to BigQuery table with error handling"""
        
        if df.empty:
            logger.warning(f"⚠️  DataFrame vacío para {table_id}")
            return {'status': 'SKIPPED', 'rows_written': 0}
        
        logger.info(f"💾 Cargando {len(df):,} registros a {table_id}")
        
        # Prepare job configuration
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True,  # Let BigQuery detect schema
            create_disposition="CREATE_IF_NEEDED"
        )
        
        if self.config.dry_run:
            logger.info(f"🏃‍♂️ DRY-RUN: Simularía cargar {len(df):,} registros a {table_id}")
            return {
                'status': 'DRY_RUN',
                'rows_written': len(df),
                'job_id': 'dry-run-job'
            }
        
        try:
            # Load data
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            
            # Wait for the job to complete
            job.result()
            
            # Get final table info
            table = self.client.get_table(table_id)
            
            logger.success(f"✅ Carga completada: {table.num_rows:,} registros en {table_id}")
            
            return {
                'status': 'SUCCESS',
                'rows_written': table.num_rows,
                'job_id': job.job_id,
                'table_size_mb': table.num_bytes / (1024 * 1024) if table.num_bytes else 0
            }
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos a {table_id}: {e}")
            return {
                'status': 'ERROR',
                'rows_written': 0,
                'error': str(e)
            }
    
    def load_all_tables(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
        """Load all transformed tables to BigQuery"""
        logger.info("🚀 Iniciando carga de todas las tablas")
        
        load_results = {}
        
        for table_name, df in transformed_data.items():
            if table_name not in self.config.output_tables:
                logger.warning(f"⚠️  Tabla {table_name} no está configurada, saltando")
                continue
            
            try:
                # Create optimized table
                table_id = self.create_optimized_table(table_name, df)
                
                # Load data
                load_result = self.load_dataframe_to_table(
                    df, 
                    table_id,
                    write_disposition="WRITE_TRUNCATE" if self.config.overwrite_tables else "WRITE_APPEND"
                )
                
                load_results[table_name] = load_result
                
            except Exception as e:
                logger.error(f"❌ Error procesando tabla {table_name}: {e}")
                load_results[table_name] = {
                    'status': 'ERROR',
                    'rows_written': 0,
                    'error': str(e)
                }
        
        # Log summary
        total_rows = sum(result.get('rows_written', 0) for result in load_results.values())
        successful_tables = sum(1 for result in load_results.values() if result.get('status') == 'SUCCESS')
        
        logger.success(f"🎉 Carga completada: {successful_tables}/{len(load_results)} tablas exitosas")
        logger.info(f"📊 Total registros cargados: {total_rows:,}")
        
        return load_results
    
    def create_table_descriptions(self) -> None:
        """Create detailed descriptions for all tables to help with documentation"""
        logger.info("📝 Actualizando descripciones de tablas")
        
        descriptions = {
            'agregada': """
Tabla principal agregada para dashboards de Looker Studio con métricas de gestión de cobranza.

DIMENSIONES DE AGREGACIÓN:
- FECHA_SERVICIO: Fecha del servicio/gestión
- CARTERA: Tipo de cartera (TEMPRANA, ALTAS_NUEVAS, etc.)
- CANAL: BOT o HUMANO
- OPERADOR: Agente específico o SISTEMA_BOT
- GRUPO_RESPUESTA/GLOSA_RESPUESTA: Clasificación de respuestas
- NIVELES 1-3: Detalle jerárquico de respuestas (solo HUMANO)

MÉTRICAS PRINCIPALES:
- total_interacciones: Total de acciones/llamadas
- clientes_unicos_contactados: Clientes únicos (sin duplicar)
- contactos_efectivos: Interacciones exitosas
- primera_vez_contactados: Clientes contactados por primera vez
- monto_total_comprometido: Compromisos de pago (solo HUMANO)

OPTIMIZACIÓN LOOKER STUDIO:
- Particionado por FECHA_SERVICIO (diario)
- Clustering por CARTERA, CANAL, OPERADOR para filtros rápidos
            """,
            
            'comparativas': """
Tabla de comparativas período-sobre-período usando lógica de mismo día hábil.

LÓGICA DE COMPARACIÓN:
- Compara métricas del día hábil N del mes actual vs día hábil N del mes anterior
- Excluye feriados de Perú y fines de semana
- Permite análisis de tendencias controlando por estacionalidad

CAMPOS CLAVE:
- fecha_actual: Fecha del período actual
- fecha_comparacion: Fecha equivalente del mes anterior
- dia_habil_numero: Qué día hábil del mes es
- puede_comparar: Si existe fecha de comparación válida

USO EN LOOKER STUDIO:
- Gráficos de tendencias mes-sobre-mes
- Alertas de variaciones significativas
- Análisis de performance por mismo día hábil
            """,
            
            'primera_vez': """
Tabla de tracking de primera interacción por cliente y dimensión.

PROPÓSITO:
- Identificar nuevos clientes contactados por primera vez
- Diferenciar entre clientes recurrentes vs nuevos
- Análisis de efectividad en captación de nuevos contactos

COMBINACIONES TRACKEADAS:
- Primera vez absoluta por cliente
- Primera vez por cliente-cartera-canal
- Primera vez por cliente-operador
- Primer contacto efectivo

APLICACIONES:
- Métricas de adquisición de clientes
- Análisis de efectividad por canal en nuevos contactos
- Segmentación entre clientes nuevos vs recurrentes
            """,
            
            'base_cartera': """
Métricas base de cartera sin gestiones para análisis de cobertura.

CONTENIDO:
- Información de todas las cuentas asignadas
- Métricas financieras agregadas (deuda, pagos)
- Dimensiones de segmentación de cartera

USOS PRINCIPALES:
- Análisis de cobertura de gestión (% cuentas gestionadas vs asignadas)
- Métricas de recuperación por tipo de cartera
- Base para calcular ratios de contactabilidad
- Benchmarking entre diferentes tipos de cartera

DIMENSIONES:
- CARTERA: Tipo de cartera
- MOVIL_FIJA: Tipo de producto/línea
- TEMPRANA_ALTAS_CUOTA_FRACCION: Segmento de gestión
            """
        }
        
        for table_name, description in descriptions.items():
            if table_name in self.config.output_tables:
                full_table_name = self.config.output_tables[table_name]
                table_id = f"{self.dataset}.{full_table_name}"
                
                try:
                    table = self.client.get_table(table_id)
                    table.description = description.strip()
                    table = self.client.update_table(table, ["description"])
                    logger.info(f"📝 Descripción actualizada: {table_name}")
                except Exception as e:
                    logger.warning(f"⚠️  No se pudo actualizar descripción de {table_name}: {e}")
    
    def optimize_for_looker_studio(self) -> None:
        """Apply additional optimizations specifically for Looker Studio performance"""
        logger.info("⚡ Aplicando optimizaciones para Looker Studio")
        
        for table_name in self.config.output_tables:
            full_table_name = self.config.output_tables[table_name]
            table_id = f"{self.dataset}.{full_table_name}"
            
            try:
                # Apply table labels
                optimization_query = f"""
                ALTER TABLE `{table_id}` 
                SET OPTIONS (
                    labels=[
                        ('source', 'faco_etl'), 
                        ('optimized_for', 'looker_studio'),
                        ('table_type', '{table_name}'),
                        ('last_updated', '{datetime.now().strftime("%Y%m%d")}')
                    ]
                )
                """
                
                if not self.config.dry_run:
                    self.client.query(optimization_query).result()
                    logger.info(f"✅ Optimización aplicada: {table_name}")
                else:
                    logger.info(f"🏃‍♂️ DRY-RUN: Optimización para {table_name}")
                    
            except Exception as e:
                logger.warning(f"⚠️  Error aplicando optimización a {table_name}: {e}")
    
    def get_table_statistics(self) -> Dict[str, Dict]:
        """Get statistics for all created tables"""
        logger.info("📊 Obteniendo estadísticas de tablas")
        
        statistics = {}
        
        for table_name in self.config.output_tables:
            full_table_name = self.config.output_tables[table_name]
            table_id = f"{self.dataset}.{full_table_name}"
            
            try:
                table = self.client.get_table(table_id)
                
                stats = {
                    'num_rows': table.num_rows,
                    'size_mb': round(table.num_bytes / (1024 * 1024), 2) if table.num_bytes else 0,
                    'num_columns': len(table.schema),
                    'partitioned': table.time_partitioning is not None,
                    'clustered': table.clustering_fields is not None,
                    'last_modified': table.modified.isoformat() if table.modified else None
                }
                
                statistics[table_name] = stats
                logger.info(f"📋 {table_name}: {stats['num_rows']:,} filas, {stats['size_mb']} MB")
                
            except Exception as e:
                logger.warning(f"⚠️  Error obteniendo estadísticas de {table_name}: {e}")
                statistics[table_name] = {'error': str(e)}
        
        return statistics