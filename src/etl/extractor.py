"""
BigQuery Data Extractor for FACO ETL

Handles extraction of cobranza data with temporal validation and fecha extraction.
"""

import re
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional
from google.cloud import bigquery
from loguru import logger

from core.config import ETLConfig


class BigQueryExtractor:
    """Extracts data from BigQuery with business logic validation"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        
    def extract_all_data(self) -> Dict[str, pd.DataFrame]:
        """Extract all required data for ETL pipeline"""
        logger.info("📥 Iniciando extracción de datos de BigQuery")
        
        data = {}
        
        # 1. Extract calendario data
        data['calendario'] = self.extract_calendario()
        logger.info(f"✅ Calendario extraído: {len(data['calendario'])} períodos")
        
        # 2. Extract asignacion data for valid archivos
        archivos_validos = data['calendario']['ARCHIVO'].tolist()
        data['asignacion'] = self.extract_asignacion(archivos_validos)
        logger.info(f"✅ Asignación extraída: {len(data['asignacion'])} registros")
        
        # 3. Extract gestiones with temporal validation
        cod_lunas = data['asignacion']['cod_luna'].unique().tolist()
        fecha_inicio = data['calendario']['FECHA_ASIGNACION'].min()
        fecha_fin = data['calendario']['FECHA_CIERRE'].max()
        
        data['voicebot'], data['mibotair'] = self.extract_gestiones(
            cod_lunas, fecha_inicio, fecha_fin
        )
        logger.info(f"✅ Gestiones extraídas - BOT: {len(data['voicebot'])}, HUMANO: {len(data['mibotair'])}")
        
        # 4. Extract financial data 
        archivos_trandeuda = self.get_valid_trandeuda_files()
        data['trandeuda'], data['pagos'] = self.extract_financial_data(archivos_trandeuda)
        logger.info(f"✅ Datos financieros - Deuda: {len(data['trandeuda'])}, Pagos: {len(data['pagos'])}")
        
        return data
    
    def extract_calendario(self) -> pd.DataFrame:
        """Extract calendario data for the specified month and status"""
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
        """
        
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            logger.error(f"Error extrayendo calendario: {e}")
            # Fallback: create mock calendario for testing
            return self._create_mock_calendario()
    
    def extract_asignacion(self, archivos_validos: List[str]) -> pd.DataFrame:
        """Extract asignacion data for valid archivos"""
        if not archivos_validos:
            logger.warning("No hay archivos válidos en calendario")
            return pd.DataFrame()
        
        archivos_with_txt = [f"'{arch}.txt'" for arch in archivos_validos]
        archivos_str = ", ".join(archivos_with_txt)
        
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
            tipo_linea,
            DATE(creado_el) as fecha_creado
        FROM `{self.config.project_id}.{self.config.dataset_id}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
        WHERE archivo IN ({archivos_str})
        """
        
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            logger.error(f"Error extrayendo asignación: {e}")
            return pd.DataFrame()
    
    def extract_gestiones(self, cod_lunas: List[int], 
                         fecha_inicio: date, fecha_fin: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Extract gestiones bot and humana with temporal validation"""
        
        if not cod_lunas:
            logger.warning("No hay cod_lunas para extraer gestiones")
            return pd.DataFrame(), pd.DataFrame()
        
        # Limit to reasonable batch size
        cod_lunas_batch = cod_lunas[:self.config.batch_size] if len(cod_lunas) > self.config.batch_size else cod_lunas
        cod_lunas_str = ", ".join(map(str, cod_lunas_batch))
        
        # Extract Voicebot data
        query_bot = f"""
        SELECT 
            SAFE_CAST(document AS INT64) as cod_luna,
            date,
            management,
            sub_management,
            compromiso,
            interes,
            duracion,
            phone,
            campaign_name,
            observacion
        FROM `{self.config.project_id}.{self.config.dataset_id}.voicebot_P3fV4dWNeMkN5RJMhV8e`
        WHERE SAFE_CAST(document AS INT64) IN ({cod_lunas_str})
        AND DATE(date) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND DATE(date) >= '2025-01-01'  -- Filter out invalid dates
        AND SAFE_CAST(document AS INT64) IS NOT NULL
        """
        
        # Extract Mibotair data  
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
            campaign_name,
            duracion,
            observacion
        FROM `{self.config.project_id}.{self.config.dataset_id}.mibotair_P3fV4dWNeMkN5RJMhV8e`
        WHERE SAFE_CAST(document AS INT64) IN ({cod_lunas_str})
        AND DATE(date) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND SAFE_CAST(document AS INT64) IS NOT NULL
        """
        
        try:
            df_bot = self.client.query(query_bot).to_dataframe()
            df_humano = self.client.query(query_humano).to_dataframe()
            return df_bot, df_humano
        except Exception as e:
            logger.error(f"Error extrayendo gestiones: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def extract_financial_data(self, archivos_validos: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Extract trandeuda and pagos data using filename dates"""
        
        # Extract trandeuda using archivo filter
        query_deuda = ""
        if archivos_validos:
            archivos_str = "', '".join(archivos_validos)
            query_deuda = f"""
            SELECT 
                cod_cuenta,
                monto_exigible,
                fecha_vencimiento,
                archivo
            FROM `{self.config.project_id}.{self.config.dataset_id}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
            WHERE archivo IN ('{archivos_str}')
            """
        
        # Extract pagos using fecha_pago column (not archivo date)
        query_pagos = f"""
        SELECT 
            cod_sistema,
            monto_cancelado,
            fecha_pago,
            archivo
        FROM `{self.config.project_id}.{self.config.dataset_id}.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
        WHERE DATE_TRUNC(fecha_pago, MONTH) = DATE('{self.config.mes_vigencia}-01')
        """
        
        try:
            df_deuda = self.client.query(query_deuda).to_dataframe() if query_deuda else pd.DataFrame()
            df_pagos = self.client.query(query_pagos).to_dataframe()
            return df_deuda, df_pagos
        except Exception as e:
            logger.error(f"Error extrayendo datos financieros: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def get_valid_trandeuda_files(self) -> List[str]:
        """Get valid trandeuda filenames by extracting date from filename"""
        try:
            query = f"""
            SELECT DISTINCT archivo
            FROM `{self.config.project_id}.{self.config.dataset_id}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
            """
            df_archivos = self.client.query(query).to_dataframe()
            
            valid_files = []
            target_year, target_month = self.config.mes_vigencia.split('-')
            
            for archivo in df_archivos['archivo']:
                fecha_archivo = self.extract_date_from_filename(archivo)
                if fecha_archivo and fecha_archivo.year == int(target_year) and fecha_archivo.month == int(target_month):
                    valid_files.append(archivo)
            
            logger.info(f"📁 Archivos trandeuda válidos encontrados: {len(valid_files)}")
            return valid_files
            
        except Exception as e:
            logger.error(f"Error obteniendo archivos trandeuda: {e}")
            return []
    
    def extract_date_from_filename(self, filename: str) -> Optional[datetime]:
        """Extract date from filename using multiple patterns"""
        patterns = [
            r'(\d{4})(\d{2})(\d{2})',  # YYYYMMDD
            r'(\d{2})(\d{2})(\d{4})',  # DDMMYYYY (need to swap)
            r'TRAN_DEUDA_(\d{2})(\d{2})',  # TRAN_DEUDA_DDMM format
            r'_(\d{8})',  # _YYYYMMDD
            r'(\d{4})-(\d{2})-(\d{2})',  # YYYY-MM-DD
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    if 'TRAN_DEUDA' in pattern:
                        # Special case: TRAN_DEUDA_DDMM - assume current year
                        day, month = match.groups()
                        year = datetime.now().year
                        return datetime(int(year), int(month), int(day))
                    elif len(match.groups()) == 3:
                        g1, g2, g3 = match.groups()
                        # Try different date interpretations
                        if len(g1) == 4:  # YYYY format
                            return datetime(int(g1), int(g2), int(g3))
                        elif len(g3) == 4:  # DD MM YYYY format
                            return datetime(int(g3), int(g2), int(g1))
                except (ValueError, TypeError):
                    continue
        
        logger.debug(f"No se pudo extraer fecha de: {filename}")
        return None
    
    def _create_mock_calendario(self) -> pd.DataFrame:
        """Create mock calendario data for testing when table doesn't exist"""
        logger.warning("⚠️  Creando calendario mock para testing")
        
        mock_data = {
            'ARCHIVO': [f'Mock_Cartera_{self.config.mes_vigencia.replace("-", "")}'],
            'cant_cod_luna_unique': [1000],
            'cant_registros_archivo': [1000], 
            'FECHA_ASIGNACION': [datetime.strptime(f'{self.config.mes_vigencia}-01', '%Y-%m-%d').date()],
            'FECHA_TRANDEUDA': [datetime.strptime(f'{self.config.mes_vigencia}-01', '%Y-%m-%d').date()],
            'FECHA_CIERRE': [datetime.strptime(f'{self.config.mes_vigencia}-28', '%Y-%m-%d').date()],
            'VENCIMIENTO': [30],
            'DIAS_GESTION': [30],
            'DIAS_PARA_CIERRE': [30],
            'ESTADO': [self.config.estado_vigencia.upper()]
        }
        
        return pd.DataFrame(mock_data)