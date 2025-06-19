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

from core.config import ETLConfig


class BigQueryExtractor:
    """Extractor de datos de BigQuery con manejo de errores robusto"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.client = self._initialize_client()
        
    def _initialize_client(self) -> bigquery.Client:
        """Inicializa cliente BigQuery con manejo de credenciales"""
        try:
            # Try with explicit credentials first
            if hasattr(self.config, 'credentials_path') and self.config.credentials_path:
                import os
                if os.path.exists(self.config.credentials_path):
                    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.config.credentials_path
            
            client = bigquery.Client(project=self.config.project_id)
            logger.info(f"✅ Cliente BigQuery inicializado para proyecto: {self.config.project_id}")
            return client
            
        except DefaultCredentialsError:
            logger.error("❌ Error de credenciales de BigQuery")
            logger.info("💡 Opciones para autenticación:")
            logger.info("   1. Ejecutar: gcloud auth application-default login")
            logger.info("   2. Configurar variable: GOOGLE_APPLICATION_CREDENTIALS")
            logger.info("   3. Crear archivo: credentials/key.json")
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
        query = f"""
        SELECT 
            ARCHIVO,
            COD_LUNA as cant_cod_luna_unique,
            CUENTA as cant_registros_archivo,
            FECHA_ASIGNACION,
            FECHA_TRANDEUDA,
            FECHA_CIERRE,
            VENCIMIENTO,
            DIAS_GESTION,
            DIAS_PARA_CIERRE,
            ESTADO
        FROM `{self.config.project_id}.{self.config.dataset_id}.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v3`
        WHERE DATE_TRUNC(FECHA_ASIGNACION, MONTH) = DATE('{self.config.mes_vigencia}-01')
        AND UPPER(ESTADO) = UPPER('{self.config.estado_vigencia}')
        ORDER BY FECHA_ASIGNACION DESC
        """
        
        logger.info(f"📅 Extrayendo calendario para {self.config.mes_vigencia} - {self.config.estado_vigencia}")
        df = self.client.query(query).to_dataframe()
        logger.info(f"✅ Calendario extraído: {len(df)} períodos")
        return df
    
    def extract_asignacion(self, archivos_calendario: List[str]) -> pd.DataFrame:
        """Extrae asignaciones para los archivos del calendario"""
        if not archivos_calendario:
            logger.warning("⚠️ No hay archivos de calendario para procesar")
            return pd.DataFrame()
        
        # Agregar .txt a los archivos del calendario
        archivos_txt = [f"{archivo}.txt" for archivo in archivos_calendario]
        archivos_str = "', '".join(archivos_txt)
        
        query = f"""
        SELECT 
            cod_luna,
            cuenta,
            cliente,
            telefono,
            dni,
            tramo_gestion,
            negocio,
            zona,
            archivo,
            min_vto,
            fraccionamiento,
            cuota_fracc_act,
            rango_renta,
            tipo_linea,
            decil_contacto,
            decil_pago,
            DATE(creado_el) as fecha_creacion
        FROM `{self.config.project_id}.{self.config.dataset_id}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
        WHERE archivo IN ('{archivos_str}')
        """
        
        logger.info(f"👥 Extrayendo asignaciones para {len(archivos_calendario)} archivos")
        df = self.client.query(query).to_dataframe()
        logger.info(f"✅ Asignaciones extraídas: {len(df)} registros")
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
        
        for batch_num, cod_lunas_batch in enumerate(cod_lunas_batches, 1):
            logger.info(f"🔄 Procesando lote {batch_num}/{len(cod_lunas_batches)} ({len(cod_lunas_batch)} cod_lunas)")
            
            cod_lunas_str = ','.join(map(str, cod_lunas_batch))
            
            # Gestiones BOT
            query_bot = f"""
            SELECT 
                SAFE_CAST(document AS INT64) as cod_luna,
                date,
                management,
                compromiso,
                duracion,
                phone,
                campaign_name,
                weight,
                origin
            FROM `{self.config.project_id}.{self.config.dataset_id}.voicebot_P3fV4dWNeMkN5RJMhV8e`
            WHERE SAFE_CAST(document AS INT64) IN ({cod_lunas_str})
            AND DATE(date) BETWEEN '{fecha_inicio.date()}' AND '{fecha_fin.date()}'
            AND DATE(date) >= '2025-01-01'
            ORDER BY date DESC
            """
            
            # Gestiones HUMANAS
            query_humano = f"""
            SELECT 
                SAFE_CAST(document AS INT64) as cod_luna,
                date,
                management,
                n1,
                n2, 
                n3,
                monto_compromiso,
                fecha_compromiso,
                nombre_agente,
                phone,
                duracion,
                campaign_name,
                weight
            FROM `{self.config.project_id}.{self.config.dataset_id}.mibotair_P3fV4dWNeMkN5RJMhV8e`
            WHERE SAFE_CAST(document AS INT64) IN ({cod_lunas_str})
            AND DATE(date) BETWEEN '{fecha_inicio.date()}' AND '{fecha_fin.date()}'
            ORDER BY date DESC
            """
            
            df_bot_batch = self.client.query(query_bot).to_dataframe()
            df_humano_batch = self.client.query(query_humano).to_dataframe()
            
            df_bot_total = pd.concat([df_bot_total, df_bot_batch], ignore_index=True)
            df_humano_total = pd.concat([df_humano_total, df_humano_batch], ignore_index=True)
        
        logger.info(f"🤖 Gestiones BOT extraídas: {len(df_bot_total)} interacciones")
        logger.info(f"👨‍💼 Gestiones HUMANAS extraídas: {len(df_humano_total)} interacciones")
        
        return df_bot_total, df_humano_total
    
    def extract_financiero_by_fecha_archivo(self, archivos_periodo: List[str]) -> tuple:
        """Extrae trandeuda y pagos usando fecha del archivo, no creado_el"""
        if not archivos_periodo:
            return pd.DataFrame(), pd.DataFrame()
        
        # Extraer fechas de los nombres de archivos
        archivos_validos = []
        for archivo in archivos_periodo:
            fecha_archivo = self._extraer_fecha_de_archivo(archivo)
            if fecha_archivo and fecha_archivo.month == int(self.config.mes_vigencia.split('-')[1]):
                archivos_validos.append(archivo)
        
        if not archivos_validos:
            logger.warning("⚠️ No se encontraron archivos financieros válidos para el período")
            return pd.DataFrame(), pd.DataFrame()
        
        # Trandeuda por archivos válidos
        archivos_str = "', '".join(archivos_validos)
        query_deuda = f"""
        SELECT 
            cod_cuenta,
            nro_documento,
            monto_exigible,
            fecha_vencimiento,
            archivo
        FROM `{self.config.project_id}.{self.config.dataset_id}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
        WHERE archivo IN ('{archivos_str}')
        """
        
        # Pagos por fecha_pago en la columna
        query_pagos = f"""
        SELECT 
            cod_sistema,
            nro_documento,
            monto_cancelado,
            fecha_pago,
            archivo
        FROM `{self.config.project_id}.{self.config.dataset_id}.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
        WHERE DATE_TRUNC(fecha_pago, MONTH) = DATE('{self.config.mes_vigencia}-01')
        """
        
        logger.info("💰 Extrayendo datos financieros")
        df_deuda = self.client.query(query_deuda).to_dataframe()
        df_pagos = self.client.query(query_pagos).to_dataframe()
        
        logger.info(f"✅ Trandeuda extraída: {len(df_deuda)} registros")
        logger.info(f"✅ Pagos extraídos: {len(df_pagos)} registros")
        
        return df_deuda, df_pagos
    
    def _extraer_fecha_de_archivo(self, nombre_archivo: str) -> Optional[datetime]:
        """Extrae fecha del nombre del archivo con múltiples patrones"""
        patrones = [
            r'(\d{4})(\d{2})(\d{2})',  # YYYYMMDD
            r'(\d{2})(\d{2})(\d{4})',  # DDMMYYYY
            r'TRAN_DEUDA_(\d{2})(\d{2})',  # TRAN_DEUDA_DDMM
            r'_(\d{8})',  # _YYYYMMDD
        ]
        
        for patron in patrones:
            match = re.search(patron, nombre_archivo)
            if match:
                try:
                    grupos = match.groups()
                    if len(grupos) == 3:
                        if len(grupos[0]) == 4:  # YYYY-MM-DD
                            year, month, day = grupos
                        else:  # DD-MM-YYYY
                            day, month, year = grupos
                        return datetime(int(year), int(month), int(day))
                    elif len(grupos) == 2:  # DD-MM (asumir año actual)
                        day, month = grupos
                        year = datetime.now().year
                        return datetime(year, int(month), int(day))
                except ValueError:
                    continue
        
        logger.warning(f"⚠️ No se pudo extraer fecha de: {nombre_archivo}")
        return None
    
    def extract_all_data(self) -> Dict[str, pd.DataFrame]:
        """Extrae todos los datos necesarios para el ETL"""
        logger.info("🚀 Iniciando extracción completa de datos")
        
        data = {}
        
        # 1. Extraer calendario
        data['calendario'] = self.extract_calendario()
        if data['calendario'].empty:
            logger.error("❌ No se encontraron períodos en el calendario")
            return data
        
        # 2. Extraer asignaciones
        archivos_calendario = data['calendario']['ARCHIVO'].tolist()
        data['asignacion'] = self.extract_asignacion(archivos_calendario)
        
        if data['asignacion'].empty:
            logger.error("❌ No se encontraron asignaciones")
            return data
        
        # 3. Extraer gestiones dentro del período
        cod_lunas = data['asignacion']['cod_luna'].unique().tolist()
        fecha_inicio = data['calendario']['FECHA_ASIGNACION'].min()
        fecha_fin = data['calendario']['FECHA_CIERRE'].max()
        
        data['voicebot'], data['mibotair'] = self.extract_gestiones_temporales(
            cod_lunas, fecha_inicio, fecha_fin
        )
        
        # 4. Extraer datos financieros
        data['trandeuda'], data['pagos'] = self.extract_financiero_by_fecha_archivo(
            archivos_calendario
        )
        
        # Resumen final
        logger.success("🎉 Extracción completa finalizada")
        for tabla, df in data.items():
            logger.info(f"   📊 {tabla}: {len(df):,} registros")
        
        return data