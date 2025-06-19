"""
BigQuery Data Extractor for FACO ETL

Extrae datos raw de BigQuery según las especificaciones de negocio.
"""

import pandas as pd
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from loguru import logger
from typing import Dict, Optional, List
from datetime import datetime
import re
import os

from core.config import ETLConfig
from .queries import QUERIES


class BigQueryExtractor:
    """Extractor de datos de BigQuery con manejo de errores robusto"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.client = self._initialize_client()
        
    def _initialize_client(self) -> bigquery.Client:
        """Inicializa cliente BigQuery con manejo de credenciales"""
        try:
            # Set credentials if available
            if self.config.has_credentials:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.config.credentials_path
                logger.info(f"🔑 Usando credenciales: {self.config.credentials_path}")
            else:
                logger.info("🔑 Usando credenciales por defecto (gcloud)")
            
            client = bigquery.Client(project=self.config.project_id)
            logger.info(f"✅ Cliente BigQuery inicializado para proyecto: {self.config.project_id}")
            return client
            
        except DefaultCredentialsError:
            logger.error("❌ Error de credenciales de BigQuery")
            logger.info(self.config.get_credentials_help())
            raise
        except Exception as e:
            logger.error(f"❌ Error inicializando BigQuery: {e}")
            raise
    
    def test_connectivity(self) -> bool:
        """Prueba conectividad básica con BigQuery"""
        try:
            query = "SELECT 1 as test"
            result = self.client.query(query).result()
            logger.info("✅ Conectividad BigQuery verificada")
            return True
        except Exception as e:
            logger.error(f"❌ Error de conectividad BigQuery: {e}")
            return False
    
    def extract_calendario(self) -> pd.DataFrame:
        """Extrae datos del calendario para el período especificado"""
        query = QUERIES['get_calendario'].format(
            dataset=f"{self.config.project_id}.{self.config.dataset_id}",
            mes_vigencia=self.config.mes_vigencia,
            estado_vigencia=self.config.estado_vigencia
        )
        
        logger.info(f"📅 Extrayendo calendario para {self.config.mes_vigencia} - {self.config.estado_vigencia}")
        logger.debug(f"Query: {query}")
        
        df = self.client.query(query).to_dataframe()
        logger.info(f"✅ Calendario extraído: {len(df)} períodos")
        
        if not df.empty:
            logger.info(f"   📊 Archivos encontrados: {df['ARCHIVO'].tolist()}")
            logger.info(f"   📅 Rango fechas: {df['FECHA_ASIGNACION'].min()} a {df['FECHA_ASIGNACION'].max()}")
        
        return df
    
    def extract_asignacion(self, archivos_calendario: List[str]) -> pd.DataFrame:
        """Extrae asignaciones para los archivos del calendario"""
        if not archivos_calendario:
            logger.warning("⚠️ No hay archivos de calendario para procesar")
            return pd.DataFrame()
        
        # Agregar .txt a los archivos del calendario
        archivos_txt = [f"{archivo}.txt" for archivo in archivos_calendario]
        
        # Use parameterized query for safety
        archivos_placeholder = ', '.join([f"'{archivo}'" for archivo in archivos_txt])
        
        query = QUERIES['get_asignacion'].format(
            dataset=f"{self.config.project_id}.{self.config.dataset_id}",
            archivos=archivos_placeholder
        )
        
        logger.info(f"👥 Extrayendo asignaciones para {len(archivos_calendario)} archivos")
        logger.debug(f"Archivos: {archivos_txt[:3]}{'...' if len(archivos_txt) > 3 else ''}")
        
        df = self.client.query(query).to_dataframe()
        logger.info(f"✅ Asignaciones extraídas: {len(df)} registros")
        
        if not df.empty:
            logger.info(f"   📊 Cod_lunas únicos: {df['cod_luna'].nunique():,}")
            logger.info(f"   👥 Cuentas únicas: {df['cuenta'].nunique():,}")
            logger.info(f"   📱 Teléfonos únicos: {df['telefono'].nunique():,}")
        
        return df
    
    def extract_gestiones_temporales(self, cod_lunas: List[int], 
                                   fecha_inicio: datetime, 
                                   fecha_fin: datetime) -> tuple:
        """Extrae gestiones bot y humana dentro del período válido"""
        if not cod_lunas:
            logger.warning("⚠️ No hay cod_lunas para extraer gestiones")
            return pd.DataFrame(), pd.DataFrame()
        
        # Limitar a lotes para evitar queries muy grandes
        batch_size = self.config.batch_size
        cod_lunas_batches = [cod_lunas[i:i + batch_size] for i in range(0, len(cod_lunas), batch_size)]
        
        df_bot_total = pd.DataFrame()
        df_humano_total = pd.DataFrame()
        
        logger.info(f"🔄 Extrayendo gestiones para {len(cod_lunas):,} cod_lunas en {len(cod_lunas_batches)} lotes")
        
        for batch_num, cod_lunas_batch in enumerate(cod_lunas_batches, 1):
            logger.info(f"   Lote {batch_num}/{len(cod_lunas_batches)} ({len(cod_lunas_batch)} cod_lunas)")
            
            cod_lunas_placeholder = ', '.join(map(str, cod_lunas_batch))
            
            # Gestiones BOT usando queries centralizadas
            query_bot = QUERIES['get_gestiones_bot'].format(
                dataset=f"{self.config.project_id}.{self.config.dataset_id}",
                cod_lunas=cod_lunas_placeholder,
                fecha_inicio=fecha_inicio.date(),
                fecha_fin=fecha_fin.date()
            )
            
            # Gestiones HUMANAS usando queries centralizadas
            query_humano = QUERIES['get_gestiones_humano'].format(
                dataset=f"{self.config.project_id}.{self.config.dataset_id}",
                cod_lunas=cod_lunas_placeholder,
                fecha_inicio=fecha_inicio.date(),
                fecha_fin=fecha_fin.date()
            )
            
            try:
                df_bot_batch = self.client.query(query_bot).to_dataframe()
                df_humano_batch = self.client.query(query_humano).to_dataframe()
                
                df_bot_total = pd.concat([df_bot_total, df_bot_batch], ignore_index=True)
                df_humano_total = pd.concat([df_humano_total, df_humano_batch], ignore_index=True)
                
                logger.debug(f"      BOT: {len(df_bot_batch)} | HUMANO: {len(df_humano_batch)}")
                
            except Exception as e:
                logger.error(f"❌ Error en lote {batch_num}: {e}")
                continue
        
        logger.info(f"🤖 Gestiones BOT extraídas: {len(df_bot_total):,} interacciones")
        logger.info(f"👨‍💼 Gestiones HUMANAS extraídas: {len(df_humano_total):,} interacciones")
        
        if not df_bot_total.empty:
            logger.info(f"   🤖 BOT - Período: {df_bot_total['date'].min()} a {df_bot_total['date'].max()}")
        if not df_humano_total.empty:
            logger.info(f"   👨‍💼 HUMANO - Período: {df_humano_total['date'].min()} a {df_humano_total['date'].max()}")
        
        return df_bot_total, df_humano_total
    
    def extract_financiero_by_fecha_archivo(self, archivos_periodo: List[str]) -> tuple:
        """Extrae trandeuda y pagos usando fecha del archivo, no creado_el"""
        if not archivos_periodo:
            logger.warning("⚠️ No hay archivos para extraer datos financieros")
            return pd.DataFrame(), pd.DataFrame()
        
        # Para trandeuda, buscar archivos que coincidan con el período
        logger.info("🔍 Buscando archivos de trandeuda válidos...")
        
        # Obtener todos los archivos disponibles
        query_archivos = QUERIES['get_all_trandeuda_files'].format(
            dataset=f"{self.config.project_id}.{self.config.dataset_id}"
        )
        
        df_archivos = self.client.query(query_archivos).to_dataframe()
        archivos_disponibles = df_archivos['archivo'].tolist() if not df_archivos.empty else []
        
        # Filtrar por fechas del período
        archivos_validos = []
        mes_objetivo = int(self.config.mes_vigencia.split('-')[1])
        año_objetivo = int(self.config.mes_vigencia.split('-')[0])
        
        for archivo in archivos_disponibles:
            fecha_archivo = self._extraer_fecha_de_archivo(archivo)
            if fecha_archivo and fecha_archivo.month == mes_objetivo and fecha_archivo.year == año_objetivo:
                archivos_validos.append(archivo)
        
        logger.info(f"📄 Archivos de trandeuda válidos encontrados: {len(archivos_validos)}")
        
        # Extraer trandeuda
        df_deuda = pd.DataFrame()
        if archivos_validos:
            archivos_placeholder = ', '.join([f"'{archivo}'" for archivo in archivos_validos])
            query_deuda = QUERIES['get_trandeuda_data'].format(
                dataset=f"{self.config.project_id}.{self.config.dataset_id}",
                archivos=archivos_placeholder
            )
            df_deuda = self.client.query(query_deuda).to_dataframe()
        
        # Extraer pagos usando queries centralizadas
        query_pagos = QUERIES['get_pagos_data'].format(
            dataset=f"{self.config.project_id}.{self.config.dataset_id}",
            mes_vigencia=self.config.mes_vigencia
        )
        df_pagos = self.client.query(query_pagos).to_dataframe()
        
        logger.info(f"✅ Datos financieros extraídos:\")\n        logger.info(f\"   💰 Trandeuda: {len(df_deuda):,} registros\")\n        logger.info(f\"   💳 Pagos: {len(df_pagos):,} registros\")\n        \n        return df_deuda, df_pagos\n    \n    def _extraer_fecha_de_archivo(self, nombre_archivo: str) -> Optional[datetime]:\n        \"\"\"Extrae fecha del nombre del archivo con múltiples patrones\"\"\"\n        patrones = [\n            r'(\\d{4})(\\d{2})(\\d{2})',  # YYYYMMDD\n            r'(\\d{2})(\\d{2})(\\d{4})',  # DDMMYYYY\n            r'TRAN_DEUDA_(\\d{2})(\\d{2})',  # TRAN_DEUDA_DDMM\n            r'_(\\d{8})',  # _YYYYMMDD\n        ]\n        \n        for patron in patrones:\n            match = re.search(patron, nombre_archivo)\n            if match:\n                try:\n                    grupos = match.groups()\n                    if len(grupos) == 3:\n                        if len(grupos[0]) == 4:  # YYYY-MM-DD\n                            year, month, day = grupos\n                        else:  # DD-MM-YYYY\n                            day, month, year = grupos\n                        return datetime(int(year), int(month), int(day))\n                    elif len(grupos) == 2:  # DD-MM (asumir año actual)\n                        day, month = grupos\n                        year = datetime.now().year\n                        return datetime(year, int(month), int(day))\n                except ValueError:\n                    continue\n        \n        logger.debug(f\"⚠️ No se pudo extraer fecha de: {nombre_archivo}\")\n        return None\n    \n    def extract_all_data(self) -> Dict[str, pd.DataFrame]:\n        \"\"\"Extrae todos los datos necesarios para el ETL\"\"\"\n        logger.info(\"🚀 Iniciando extracción completa de datos\")\n        \n        data = {}\n        \n        try:\n            # 1. Extraer calendario\n            logger.info(\"📅 Paso 1: Extrayendo calendario...\")\n            data['calendario'] = self.extract_calendario()\n            if data['calendario'].empty:\n                logger.error(\"❌ No se encontraron períodos en el calendario\")\n                return data\n            \n            # 2. Extraer asignaciones\n            logger.info(\"👥 Paso 2: Extrayendo asignaciones...\")\n            archivos_calendario = data['calendario']['ARCHIVO'].tolist()\n            data['asignacion'] = self.extract_asignacion(archivos_calendario)\n            \n            if data['asignacion'].empty:\n                logger.error(\"❌ No se encontraron asignaciones\")\n                return data\n            \n            # 3. Extraer gestiones dentro del período\n            logger.info(\"🎯 Paso 3: Extrayendo gestiones...\")\n            cod_lunas = data['asignacion']['cod_luna'].unique().tolist()\n            fecha_inicio = data['calendario']['FECHA_ASIGNACION'].min()\n            fecha_fin = data['calendario']['FECHA_CIERRE'].max()\n            \n            data['voicebot'], data['mibotair'] = self.extract_gestiones_temporales(\n                cod_lunas, fecha_inicio, fecha_fin\n            )\n            \n            # 4. Extraer datos financieros\n            logger.info(\"💰 Paso 4: Extrayendo datos financieros...\")\n            data['trandeuda'], data['pagos'] = self.extract_financiero_by_fecha_archivo(\n                archivos_calendario\n            )\n            \n            # Resumen final\n            logger.success(\"🎉 Extracción completa finalizada\")\n            total_records = 0\n            for tabla, df in data.items():\n                count = len(df)\n                total_records += count\n                logger.info(f\"   📊 {tabla}: {count:,} registros\")\n            \n            logger.info(f\"📈 Total de registros extraídos: {total_records:,}\")\n            \n            return data\n            \n        except Exception as e:\n            logger.error(f\"💥 Error durante la extracción: {e}\")\n            raise\n    \n    def get_data_summary(self) -> Dict:\n        \"\"\"Obtiene un resumen rápido de los datos disponibles\"\"\"\n        try:\n            # Quick calendar check\n            calendario = self.extract_calendario()\n            \n            if calendario.empty:\n                return {\n                    \"disponible\": False,\n                    \"mensaje\": f\"No hay datos para {self.config.mes_vigencia} - {self.config.estado_vigencia}\"\n                }\n            \n            return {\n                \"disponible\": True,\n                \"periodos_encontrados\": len(calendario),\n                \"archivos\": calendario['ARCHIVO'].tolist(),\n                \"fecha_inicio\": calendario['FECHA_ASIGNACION'].min().strftime('%Y-%m-%d'),\n                \"fecha_fin\": calendario['FECHA_CIERRE'].max().strftime('%Y-%m-%d'),\n                \"dias_gestion\": calendario['DIAS_GESTION'].iloc[0],\n                \"estado\": calendario['ESTADO'].iloc[0]\n            }\n            \n        except Exception as e:\n            return {\n                \"disponible\": False,\n                \"error\": str(e)\n            }