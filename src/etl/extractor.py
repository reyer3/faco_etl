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
        self.client = self._initialize_client()
        self.dataset_id = f"{config.project_id}.{config.dataset_id}"
        logger.info(f"🔌 BigQuery Extractor inicializado para dataset: {self.dataset_id}")

    def _initialize_client(self) -> bigquery.Client:
        """Inicializa el cliente de BigQuery pasando las credenciales explícitamente."""
        try:
            client = bigquery.Client(
                project=self.config.project_id,
                credentials=self.config.credentials_object
            )
            logger.info(f"✅ Cliente BigQuery inicializado para proyecto: {self.config.project_id}")
            return client
        except Exception as e:
            logger.error(f"❌ Falló la inicialización del cliente de BigQuery: {e}")
            raise

    def _execute_query(self, query_template: str, params: List[bigquery.ScalarQueryParameter],
                       job_id_prefix: str) -> pd.DataFrame:
        """Ejecuta una consulta parametrizada y maneja los errores."""
        query = query_template.format(dataset=self.dataset_id)
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        try:
            df = self.client.query(query, job_config=job_config).to_dataframe()
            return df
        except GoogleAPICallError as e:
            logger.error(f"❌ Error en la API de BigQuery [Job Prefix: {job_id_prefix}]: {e.message}")
            raise
        except Exception as e:
            logger.error(f"❌ Error inesperado ejecutando consulta [Job Prefix: {job_id_prefix}]: {e}")
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

    def extract_asignacion(self, archivos_calendario: List[str]) -> pd.DataFrame:
        """Extrae las asignaciones para los archivos del calendario."""
        if not archivos_calendario:
            logger.warning("⚠️ No hay archivos de calendario para procesar, se omite extracción de asignaciones.")
            return pd.DataFrame()

        archivos_txt = [f"{archivo}.txt" for archivo in archivos_calendario]
        logger.info(f"👥 Extrayendo asignaciones para {len(archivos_calendario)} archivos.")

        params = [bigquery.ArrayQueryParameter("archivos", "STRING", archivos_txt)]
        df = self._execute_query(QUERIES['get_asignacion'], params, "asignacion")
        logger.info(f"✅ Asignaciones extraídas: {len(df):,} registros.")
        return df

    def _paginated_extraction(self, query_name: str, ids: List[Any], id_type: str, id_key: str,
                              **extra_params) -> pd.DataFrame:
        """Extrae datos en lotes para listas largas de IDs."""
        if not ids:
            return pd.DataFrame()

        all_dfs = []
        for i in range(0, len(ids), self.config.batch_size):
            batch_ids = ids[i:i + self.config.batch_size]
            logger.debug(
                f"  - Procesando lote para '{query_name}' ({i // self.config.batch_size + 1}), {len(batch_ids)} IDs.")

            params = [bigquery.ArrayQueryParameter(id_key, id_type, batch_ids)]
            for key, value in extra_params.items():
                params.append(bigquery.ScalarQueryParameter(key, "STRING", value))

            df_batch = self._execute_query(QUERIES[query_name], params, f"{query_name}_batch")
            all_dfs.append(df_batch)

        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    def extract_gestiones_by_period(self, cod_lunas: List[int], fecha_inicio: pd.Timestamp, fecha_fin: pd.Timestamp) -> \
            Tuple[pd.DataFrame, pd.DataFrame]:
        """Extrae gestiones de BOT y HUMANAS usando paginación."""
        if not cod_lunas:
            logger.warning("⚠️ No hay cod_lunas para extraer gestiones.")
            return pd.DataFrame(), pd.DataFrame()

        params = {
            'fecha_inicio': fecha_inicio.strftime('%Y-%m-%d'),
            'fecha_fin': fecha_fin.strftime('%Y-%m-%d')
        }

        logger.info(
            f"🤖 Extrayendo gestiones BOT para {len(cod_lunas):,} cod_lunas en lotes de {self.config.batch_size}.")
        df_bot = self._paginated_extraction('get_gestiones_bot', cod_lunas, "INT64", "cod_lunas", **params)
        logger.info(f"✅ Gestiones BOT extraídas: {len(df_bot):,} interacciones.")

        logger.info(
            f"👨‍💼 Extrayendo gestiones HUMANAS para {len(cod_lunas):,} cod_lunas en lotes de {self.config.batch_size}.")
        df_humano = self._paginated_extraction('get_gestiones_humano', cod_lunas, "INT64", "cod_lunas", **params)
        logger.info(f"✅ Gestiones HUMANAS extraídas: {len(df_humano):,} interacciones.")

        return df_bot, df_humano

    def _extract_date_from_filename(self, filename: str) -> Optional[datetime]:
        """Extrae la fecha de un nombre de archivo, infiriendo el año del período en curso."""
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
                    return date_parser(match)
                except (ValueError, TypeError):
                    continue
        return None

    def extract_financial_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Extrae datos financieros (deuda y pagos) para el período."""
        logger.info(f"💰 Extrayendo datos financieros para el período {self.config.mes_vigencia}")

        # Extraer Trandeuda
        all_trandeuda_files_df = self._execute_query(QUERIES['get_all_trandeuda_files'], [], "list_trandeuda")
        valid_files = [
            fn for fn in all_trandeuda_files_df['archivo']
            if (date := self._extract_date_from_filename(fn))
               and date.year == int(self.config.mes_vigencia.split('-')[0])
               and date.month == int(self.config.mes_vigencia.split('-')[1])
        ]
        if valid_files:
            logger.info(f"📄 Archivos Trandeuda válidos encontrados: {len(valid_files)}")
            df_deuda = self._paginated_extraction('get_trandeuda_data', valid_files, "STRING", "archivos")
            logger.info(f"✅ Trandeuda extraída: {len(df_deuda):,} registros.")
        else:
            df_deuda = pd.DataFrame()
            logger.warning("⚠️ No se encontraron archivos de trandeuda válidos para el período.")

        # Extraer Pagos
        params = [bigquery.ScalarQueryParameter("mes_vigencia", "STRING", f"{self.config.mes_vigencia}-01")]
        df_pagos = self._execute_query(QUERIES['get_pagos_data'], params, "pagos")
        logger.info(f"✅ Pagos extraídos: {len(df_pagos):,} registros.")

        return df_deuda, df_pagos

    def extract_all_data(self) -> Dict[str, pd.DataFrame]:
        """Orquesta la extracción completa de datos, asegurando la lógica temporal."""
        logger.info("🚀 Iniciando extracción completa de datos")

        data = {}
        # 1. Calendario
        df_calendario = self.extract_calendario()
        data['calendario'] = df_calendario
        if df_calendario.empty:
            logger.error("Extracción detenida: No se encontraron datos en el calendario para el período.")
            return data

        # 2. Asignación
        archivos_calendario = df_calendario['ARCHIVO'].unique().tolist()
        df_asignacion = self.extract_asignacion(archivos_calendario)
        data['asignacion'] = df_asignacion
        if df_asignacion.empty:
            logger.error("Extracción detenida: No se encontraron asignaciones para los archivos del calendario.")
            return data

        # 3. Gestiones (con manejo de fechas robusto)
        df_calendario['FECHA_ASIGNACION'] = pd.to_datetime(df_calendario['FECHA_ASIGNACION'])
        df_calendario['FECHA_CIERRE'] = pd.to_datetime(df_calendario['FECHA_CIERRE'])

        fecha_inicio = df_calendario['FECHA_ASIGNACION'].min()
        fecha_fin = df_calendario['FECHA_CIERRE'].max()

        if pd.isna(fecha_fin):
            fecha_fin = pd.Timestamp.now()
            logger.info(f"📅 Período abierto detectado. Usando fecha actual como fin: {fecha_fin.date()}")

        logger.info(f"⏰ Período de gestión válido: {fecha_inicio.date()} a {fecha_fin.date()}")

        cod_lunas_unicos = df_asignacion['cod_luna'].unique().tolist()
        df_bot, df_humano = self.extract_gestiones_by_period(cod_lunas_unicos, fecha_inicio, fecha_fin)
        data['voicebot'] = df_bot
        data['mibotair'] = df_humano

        # 4. Datos Financieros
        df_deuda, df_pagos = self.extract_financial_data()
        data['trandeuda'] = df_deuda
        data['pagos'] = df_pagos

        logger.success("🎉 Extracción completa finalizada.")
        return data
