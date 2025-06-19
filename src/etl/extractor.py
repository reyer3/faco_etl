"""
BigQuery Data Extractor for FACO ETL

Extrae datos de BigQuery de forma robusta, utilizando parámetros de consulta,
paginación para grandes volúmenes de datos y manejo explícito de credenciales.
"""

import re
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
from loguru import logger

from core.config import ETLConfig
from .queries import QUERIES


class BigQueryExtractor:
    """Extrae datos de BigQuery con lógica de negocio y validación."""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.client = bigquery.Client(
            project=config.project_id,
            credentials=config.credentials_object
        )
        self.dataset_id = f"{config.project_id}.{config.dataset_id}"
        logger.info(f"🔌 BigQuery Extractor inicializado para dataset: {self.dataset_id}")

    def _execute_query(self, query_template: str, params: List, job_id_prefix: str) -> pd.DataFrame:
        """Ejecuta una consulta parametrizada y maneja los errores."""
        query = query_template.format(dataset=self.dataset_id)
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        full_job_id_prefix = f"faco_{job_id_prefix}_"

        try:
            df = self.client.query(query, job_config=job_config, job_id_prefix=full_job_id_prefix).to_dataframe()
            return df
        except GoogleAPICallError as e:
            logger.error(f"❌ Error en la API de BigQuery [Job Prefix: {full_job_id_prefix}]: {e.message}")
            raise
        except Exception as e:
            logger.error(f"❌ Error inesperado ejecutando consulta [Job Prefix: {full_job_id_prefix}]: {e}")
            raise

    def test_connectivity(self) -> bool:
        """Prueba la conectividad básica con BigQuery."""
        try:
            self.client.query("SELECT 1").result()
            logger.info("✅ Conectividad BigQuery verificada")
            return True
        except Exception as e:
            logger.error(f"❌ Error de conectividad con BigQuery: {e}")
            return False

    def extract_calendario(self) -> pd.DataFrame:
        """Extrae los datos del calendario para el período configurado."""
        logger.info(f"📅 Extrayendo calendario para {self.config.mes_vigencia} - {self.config.estado_vigencia}")
        params = [
            bigquery.ScalarQueryParameter("mes_vigencia", "STRING", f"{self.config.mes_vigencia}-01"),
            bigquery.ScalarQueryParameter("estado_vigencia", "STRING", self.config.estado_vigencia),
        ]
        df = self._execute_query(QUERIES['get_calendario'], params, "calendario")
        logger.info(f"✅ Calendario extraído: {len(df)} períodos encontrados.")
        return df

    def _paginated_extraction(self, query_name: str, ids: List[Any], id_type: str, id_key: str, **extra_params) -> pd.DataFrame:
        """Extrae datos en lotes para listas largas de IDs."""
        if not ids:
            return pd.DataFrame()
        all_dfs = []
        for i in range(0, len(ids), self.config.batch_size):
            batch_ids = ids[i:i + self.config.batch_size]
            logger.debug(f"  - Procesando lote para '{query_name}' ({i//self.config.batch_size + 1}), {len(batch_ids)} IDs.")
            params = [bigquery.ArrayQueryParameter(id_key, id_type, batch_ids)]
            for key, value in extra_params.items():
                params.append(bigquery.ScalarQueryParameter(key, "STRING", value))
            df_batch = self._execute_query(QUERIES[query_name], params, f"{query_name}_batch")
            all_dfs.append(df_batch)
        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    def extract_gestiones_by_period(self, cod_lunas: List[int], fecha_inicio: pd.Timestamp, fecha_fin: pd.Timestamp) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Extrae gestiones de BOT y HUMANAS usando paginación."""
        if not cod_lunas:
            return pd.DataFrame(), pd.DataFrame()
        params = {
            'fecha_inicio': fecha_inicio.strftime('%Y-%m-%d'),
            'fecha_fin': fecha_fin.strftime('%Y-%m-%d')
        }
        df_bot = self._paginated_extraction('get_gestiones_bot', cod_lunas, "INT64", "cod_lunas", **params)
        df_humano = self._paginated_extraction('get_gestiones_humano', cod_lunas, "INT64", "cod_lunas", **params)
        return df_bot, df_humano

    def _extract_date_from_filename(self, filename: str) -> Optional[datetime]:
        """Extrae la fecha de un nombre de archivo."""
        target_year = int(self.config.mes_vigencia.split('-')[0])
        patterns = [
            (r'(\d{4})(\d{2})(\d{2})', lambda m: datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))),
            (r'TRAN_DEUDA_(\d{2})(\d{2})', lambda m: datetime(target_year, int(m.group(2)), int(m.group(1)))),
            (r'(\d{2})(\d{2})(\d{4})', lambda m: datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))),
        ]
        for pattern, date_parser in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    return date_parser(match)
                except (ValueError, TypeError):
                    continue
        return None

    def extract_contexto_deuda(self, fechas_trandeuda_unicas: List[pd.Timestamp]) -> pd.DataFrame:
        """Extrae toda la trandeuda relevante para un conjunto de fechas."""
        logger.info(f"💰 Extrayendo contexto de deuda para {len(fechas_trandeuda_unicas)} fecha(s) única(s)...")
        all_trandeuda_files_df = self._execute_query(QUERIES['get_all_trandeuda_files'], [], "list_trandeuda_all")

        fechas_set = {f.date() for f in fechas_trandeuda_unicas}
        valid_files = [
            fn for fn in all_trandeuda_files_df['archivo']
            if (date := self._extract_date_from_filename(fn)) and date.date() in fechas_set
        ]

        if not valid_files:
            logger.warning("⚠️ No se encontraron archivos de Trandeuda para las fechas del calendario.")
            return pd.DataFrame()

        logger.info(f"  -> {len(valid_files)} archivos de Trandeuda encontrados. Extrayendo datos...")
        df_deuda = self._paginated_extraction('get_trandeuda_data', valid_files, "STRING", "archivos")
        logger.info(f"  -> ✅ Deuda total extraída: {len(df_deuda):,} registros.")
        return df_deuda

    def extract_contexto_pagos(self, nros_documento: List[str]) -> pd.DataFrame:
        """Extrae todos los pagos para una lista de números de documento."""
        if not nros_documento:
            logger.warning("⚠️ No hay números de documento para buscar pagos.")
            return pd.DataFrame()

        logger.info(f"💳 Extrayendo contexto de pagos para {len(nros_documento):,} documentos...")
        df_pagos = self._paginated_extraction('get_pagos_by_nro_documento', nros_documento, "STRING", "nros_documento")
        logger.info(f"  -> ✅ Pagos totales extraídos: {len(df_pagos):,} registros.")
        return df_pagos

    def extract_data_for_period(self, calendario_periodo: pd.Series) -> Dict[str, pd.DataFrame]:
        """Extrae solo los datos de asignación y gestión para un período."""
        archivo = calendario_periodo['ARCHIVO']
        logger.info(f"▶️  Extrayendo datos de asignación/gestión para: {archivo}")

        data = {'calendario': calendario_periodo.to_frame().T}

        # 1. Asignación
        archivos_txt = [f"{archivo}.txt"]
        params = [bigquery.ArrayQueryParameter("archivos", "STRING", archivos_txt)]
        df_asignacion = self._execute_query(QUERIES['get_asignacion'], params, "asignacion_periodo")
        data['asignacion'] = df_asignacion

        # 2. Gestiones
        if not df_asignacion.empty:
            cod_lunas_unicos = df_asignacion['cod_luna'].unique().tolist()
            fecha_inicio = pd.to_datetime(calendario_periodo['FECHA_ASIGNACION'])
            fecha_cierre = pd.to_datetime(calendario_periodo['FECHA_CIERRE'])
            if pd.isna(fecha_cierre):
                fecha_cierre = pd.Timestamp.now()

            df_bot, df_humano = self.extract_gestiones_by_period(cod_lunas_unicos, fecha_inicio, fecha_cierre)
            data['voicebot'], data['mibotair'] = df_bot, df_humano
        else:
            data['voicebot'], data['mibotair'] = pd.DataFrame(), pd.DataFrame()

        return data