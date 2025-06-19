"""
BigQuery Data Extractor for FACO ETL

Handles extraction of data from BigQuery tables with date validation
and filename parsing as specified in requirements.
Refactored for robustness, maintainability, and performance.
"""

import re
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
from loguru import logger

from core.config import ETLConfig
from etl.queries import QUERIES  # Importar las consultas centralizadas

class BigQueryExtractor:
    """Extract data from BigQuery with business logic validation"""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        self.dataset = f"{config.project_id}.{config.dataset_id}"
        logger.info(f"🔌 BigQuery Extractor inicializado - Dataset: {self.dataset}")

    def _get_query(self, query_name: str, **params) -> str:
        """Obtiene y formatea una consulta SQL desde el repositorio central."""
        if query_name not in QUERIES:
            raise ValueError(f"La consulta '{query_name}' no fue encontrada.")

        # Parámetros por defecto que todas las consultas pueden necesitar
        default_params = {'dataset': self.dataset}
        all_params = {**default_params, **params}
        return QUERIES[query_name].format(**all_params)

    def _execute_query(self, query: str, job_id_prefix: str = "faco_etl_") -> pd.DataFrame:
        """Ejecuta una consulta y la convierte a DataFrame, manejando errores."""
        try:
            # Añadir un prefijo al job_id ayuda a identificar las consultas en la UI de BigQuery
            job_config = bigquery.QueryJobConfig(job_id_prefix=job_id_prefix)
            df = self.client.query(query, job_config=job_config).to_dataframe()
            return df
        except GoogleAPICallError as e:
            logger.error(f"❌ Error en la API de BigQuery al ejecutar la consulta: {e}")
            raise  # Relanzar para que el orquestador lo capture
        except Exception as e:
            logger.error(f"❌ Error inesperado al ejecutar la consulta: {e}")
            raise

    def extract_date_from_filename(self, filename: str) -> Optional[datetime]:
        """
        Extrae la fecha de un nombre de archivo usando patrones múltiples.
        El año para formatos DDMM se infiere del `mes_vigencia`.
        """
        target_year = int(self.config.mes_vigencia.split('-')[0])

        patterns = [
            (r'(\d{4})(\d{2})(\d{2})', lambda m: datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))),
            (r'TRAN_DEUDA_(\d{2})(\d{2})', lambda m: datetime(target_year, int(m.group(2)), int(m.group(1)))),
            (r'_(\d{2})(\d{2})_', lambda m: datetime(target_year, int(m.group(2)), int(m.group(1)))),
            (r'(\d{2})(\d{2})(\d{4})', lambda m: datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))),
        ]

        for pattern, date_parser in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    date_obj = date_parser(match)
                    logger.trace(f"📅 Fecha extraída de '{filename}': {date_obj.date()}")
                    return date_obj
                except (ValueError, TypeError):
                    continue

        logger.warning(f"❌ No se pudo extraer fecha del archivo: {filename}")
        return None

    def extract_calendario(self) -> pd.DataFrame:
        logger.info(f"📅 Extrayendo calendario para {self.config.mes_vigencia} - {self.config.estado_vigencia}")
        query = self._get_query(
            'get_calendario',
            mes_vigencia=self.config.mes_vigencia,
            estado_vigencia=self.config.estado_vigencia
        )
        df = self._execute_query(query, "faco_calendario")

        if df.empty:
            logger.warning(f"⚠️ No se encontraron períodos para {self.config.mes_vigencia} - {self.config.estado_vigencia}")
        else:
            logger.info(f"✅ Calendario extraído: {len(df)} períodos encontrados.")
            logger.debug(f"📊 Archivos en calendario: {df['ARCHIVO'].tolist()}")
        return df

    def extract_asignacion(self, archivos_calendario: List[str]) -> pd.DataFrame:
        if not archivos_calendario:
            logger.warning("⚠️ No hay archivos en calendario para extraer asignaciones.")
            return pd.DataFrame()

        archivos_con_extension = [f"{archivo}.txt" for archivo in archivos_calendario]
        logger.info(f"👥 Extrayendo asignaciones para {len(archivos_calendario)} archivos.")

        # Pasar la lista como parámetro de array para evitar inyección y problemas de formato
        query = self._get_query('get_asignacion', archivos=archivos_con_extension)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("archivos", "STRING", archivos_con_extension)]
        )

        try:
            df = self.client.query(query, job_config=job_config).to_dataframe()
            logger.info(f"✅ Asignaciones extraídas: {len(df):,} registros.")
            logger.info(f"📊 Cuentas únicas: {df['cuenta'].nunique():,}, Cod_lunas únicos: {df['cod_luna'].nunique():,}")
            return df
        except Exception as e:
            logger.error(f"❌ Error extrayendo asignaciones: {e}")
            raise

    def _paginated_extraction(self, query_name: str, ids: List, **extra_params) -> pd.DataFrame:
        """
        Extrae datos en lotes para listas largas de IDs.
        Crítico para evitar errores de longitud de consulta.
        """
        if not ids:
            return pd.DataFrame()

        all_dfs = []
        id_key = "cod_lunas" if query_name.startswith("get_gestiones") else "archivos"

        for i in range(0, len(ids), self.config.batch_size):
            batch_ids = ids[i:i + self.config.batch_size]
            logger.debug(f"  - Extrayendo lote {i//self.config.batch_size + 1}, {len(batch_ids)} IDs.")

            # Usamos UNNEST para pasar arrays, más seguro y eficiente que formatear strings.
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter(id_key, "INT64" if id_key == "cod_lunas" else "STRING", batch_ids)]
            )
            query = self._get_query(query_name, **extra_params)

            df_batch = self.client.query(query, job_config=job_config).to_dataframe()
            all_dfs.append(df_batch)

        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    def extract_gestiones_by_period(self, cod_lunas: List[int], fecha_inicio: datetime, fecha_fin: datetime) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if not cod_lunas:
            logger.warning("⚠️ No hay cod_lunas para extraer gestiones.")
            return pd.DataFrame(), pd.DataFrame()

        params = {
            'fecha_inicio': fecha_inicio.strftime('%Y-%m-%d'),
            'fecha_fin': fecha_fin.strftime('%Y-%m-%d')
        }

        logger.info(f"🤖 Extrayendo gestiones BOT para {len(cod_lunas):,} cod_lunas en lotes de {self.config.batch_size}.")
        df_bot = self._paginated_extraction('get_gestiones_bot', cod_lunas, **params)
        logger.info(f"✅ Gestiones BOT extraídas: {len(df_bot):,} interacciones.")

        logger.info(f"👨‍💼 Extrayendo gestiones HUMANAS para {len(cod_lunas):,} cod_lunas en lotes de {self.config.batch_size}.")
        df_humano = self._paginated_extraction('get_gestiones_humano', cod_lunas, **params)
        logger.info(f"✅ Gestiones HUMANAS extraídas: {len(df_humano):,} interacciones.")

        return df_bot, df_humano

    def extract_financial_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        logger.info(f"💰 Extrayendo datos financieros para período {self.config.mes_vigencia}")

        # 1. Extraer Trandeuda basado en nombre de archivo
        query_all_files = self._get_query('get_all_trandeuda_files')
        all_trandeuda_files_df = self._execute_query(query_all_files, "faco_list_trandeuda")

        valid_trandeuda_files = [
            filename for filename in all_trandeuda_files_df['archivo']
            if (extracted_date := self.extract_date_from_filename(filename))
            and extracted_date.year == int(self.config.mes_vigencia.split('-')[0])
            and extracted_date.month == int(self.config.mes_vigencia.split('-')[1])
        ]

        if valid_trandeuda_files:
            logger.info(f"📄 Archivos Trandeuda válidos encontrados: {len(valid_trandeuda_files)}")
            df_deuda = self._paginated_extraction('get_trandeuda_data', valid_trandeuda_files)
            logger.info(f"✅ Trandeuda extraída: {len(df_deuda):,} registros.")
        else:
            df_deuda = pd.DataFrame()
            logger.warning("⚠️ No se encontraron archivos de trandeuda válidos para el período.")

        # 2. Extraer Pagos basado en fecha_pago
        query_pagos = self._get_query('get_pagos_data', mes_vigencia=self.config.mes_vigencia)
        df_pagos = self._execute_query(query_pagos, "faco_pagos")
        logger.info(f"✅ Pagos extraídos: {len(df_pagos):,} registros.")

        return df_deuda, df_pagos

    def extract_all_data(self) -> Dict[str, pd.DataFrame]:
        """Orquesta la extracción completa de datos con validaciones temporales."""
        logger.info("🚀 Iniciando extracción completa de datos")

        df_calendario = self.extract_calendario()
        if df_calendario.empty:
            # El orquestador manejará este error, no necesitamos abortar aquí
            return {'calendario': df_calendario}

        archivos_calendario = df_calendario['ARCHIVO'].unique().tolist()
        df_asignacion = self.extract_asignacion(archivos_calendario)
        if df_asignacion.empty:
            return {'calendario': df_calendario, 'asignacion': df_asignacion}

        fecha_inicio = df_calendario['FECHA_ASIGNACION'].min()
        fecha_fin = df_calendario['FECHA_CIERRE'].max()
        if pd.isna(fecha_fin):
            fecha_fin = datetime.now()
            logger.info(f"📅 Período abierto detectado. Usando fecha actual como fin: {fecha_fin.date()}")

        logger.info(f"⏰ Período de gestión válido: {fecha_inicio.date()} a {fecha_fin.date()}")

        cod_lunas_unicos = df_asignacion['cod_luna'].unique().tolist()
        df_bot, df_humano = self.extract_gestiones_by_period(cod_lunas_unicos, fecha_inicio, fecha_fin)

        df_deuda, df_pagos = self.extract_financial_data()

        result = {
            'calendario': df_calendario, 'asignacion': df_asignacion,
            'voicebot': df_bot, 'mibotair': df_humano,
            'trandeuda': df_deuda, 'pagos': df_pagos
        }

        logger.success("🎉 Extracción completa finalizada.")
        return result

    def test_connectivity(self) -> bool:
        """Test BigQuery connectivity and table access"""
        try:
            self._execute_query("SELECT 1", "faco_connectivity_test")
            logger.info("✅ Conectividad con BigQuery OK.")
            return True
        except Exception as e:
            logger.error(f"❌ Falla de conectividad con BigQuery: {e}")
            return False