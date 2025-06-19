"""
BigQuery Data Extractor for FACO ETL

Handles extraction of data from BigQuery tables with date validation
and filename parsing as specified in requirements.
"""

import re
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from google.cloud import bigquery
from loguru import logger

from core.config import ETLConfig


class BigQueryExtractor:
    """Extract data from BigQuery with business logic validation"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        self.dataset = f"{config.project_id}.{config.dataset_id}"
        
        logger.info(f"🔌 BigQuery Extractor inicializado - Dataset: {self.dataset}")
    
    def extract_date_from_filename(self, filename: str) -> Optional[datetime]:
        """
        Extract date from filename using multiple patterns.
        
        Examples:
        - 'TRAN_DEUDA_3103_MINIFICADO.txt' -> extract '31' and '03' as day/month
        - 'Cartera_Agencia_20250617.txt' -> extract '20250617' as YYYYMMDD
        """
        patterns = [
            # Pattern: YYYYMMDD (8 digits)
            (r'(\d{4})(\d{2})(\d{2})', lambda m: datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))),
            
            # Pattern: TRAN_DEUDA_DDMM_* 
            (r'TRAN_DEUDA_(\d{2})(\d{2})', lambda m: datetime(2025, int(m.group(2)), int(m.group(1)))),
            
            # Pattern: *_DDMM_*
            (r'_(\d{2})(\d{2})_', lambda m: datetime(2025, int(m.group(2)), int(m.group(1)))),
            
            # Pattern: DDMMYYYY
            (r'(\d{2})(\d{2})(\d{4})', lambda m: datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))),
        ]
        
        for pattern, date_parser in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    date_obj = date_parser(match)
                    logger.debug(f"📅 Fecha extraída de '{filename}': {date_obj.date()}")
                    return date_obj
                except (ValueError, TypeError) as e:
                    logger.debug(f"⚠️  Error parseando fecha de '{filename}' con patrón '{pattern}': {e}")
                    continue
        
        logger.warning(f"❌ No se pudo extraer fecha del archivo: {filename}")
        return None
    
    def extract_calendario(self) -> pd.DataFrame:
        """Extract calendar data for the specified period"""
        logger.info(f"📅 Extrayendo calendario para {self.config.mes_vigencia} - {self.config.estado_vigencia}")
        
        query = f"""
        SELECT 
            ARCHIVO,
            COD_LUNA as cant_cod_luna_unique,  -- Using corrected nomenclature
            CUENTA as cant_registros_archivo,
            FECHA_ASIGNACION,
            FECHA_TRANDEUDA,
            FECHA_CIERRE,
            VENCIMIENTO,
            DIAS_GESTION,
            DIAS_PARA_CIERRE,
            ESTADO
        FROM `{self.dataset}.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v3`
        WHERE DATE_TRUNC(FECHA_ASIGNACION, MONTH) = DATE('{self.config.mes_vigencia}-01')
        AND UPPER(ESTADO) = UPPER('{self.config.estado_vigencia}')
        ORDER BY FECHA_ASIGNACION DESC
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"✅ Calendario extraído: {len(df)} períodos encontrados")
            
            if df.empty:
                logger.warning(f"⚠️  No se encontraron períodos para {self.config.mes_vigencia} - {self.config.estado_vigencia}")
            else:
                logger.debug(f"📊 Archivos en calendario: {df['ARCHIVO'].tolist()}")
                
            return df
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo calendario: {e}")
            return pd.DataFrame()
    
    def extract_asignacion(self, archivos_calendario: List[str]) -> pd.DataFrame:
        """Extract assignment data for files in calendar"""
        if not archivos_calendario:
            logger.warning("⚠️  No hay archivos en calendario para extraer asignaciones")
            return pd.DataFrame()
        
        # Add .txt extension to match asignacion table format
        archivos_con_extension = [f"{archivo}.txt" for archivo in archivos_calendario]
        archivos_str = "', '".join(archivos_con_extension)
        
        logger.info(f"👥 Extrayendo asignaciones para {len(archivos_calendario)} archivos")
        
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
            decil_contacto,
            decil_pago,
            tipo_linea,
            DATE(creado_el) as fecha_carga
        FROM `{self.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
        WHERE archivo IN ('{archivos_str}')
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"✅ Asignaciones extraídas: {len(df):,} registros")
            logger.info(f"📊 Cuentas únicas: {df['cuenta'].nunique():,}")
            logger.info(f"📊 Cod_lunas únicos: {df['cod_luna'].nunique():,}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo asignaciones: {e}")
            return pd.DataFrame()
    
    def extract_gestiones_by_period(self, cod_lunas: List[int], 
                                   fecha_inicio: datetime, 
                                   fecha_fin: datetime) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Extract bot and human management data within valid period"""
        
        if not cod_lunas:
            logger.warning("⚠️  No hay cod_lunas para extraer gestiones")
            return pd.DataFrame(), pd.DataFrame()
        
        # Limit batch size to avoid query timeouts
        batch_size = min(self.config.batch_size, 50000)
        cod_lunas_batch = cod_lunas[:batch_size]
        cod_lunas_str = ','.join(map(str, cod_lunas_batch))
        
        logger.info(f"🤖 Extrayendo gestiones BOT para {len(cod_lunas_batch):,} cod_lunas")
        logger.info(f"📅 Período: {fecha_inicio.date()} a {fecha_fin.date()}")
        
        # Extract BOT gestiones
        query_bot = f"""
        SELECT 
            SAFE_CAST(document AS INT64) as cod_luna,
            date,
            management,
            sub_management,
            compromiso,
            fecha_compromiso,
            duracion,
            phone,
            campaign_name,
            origin,
            weight
        FROM `{self.dataset}.voicebot_P3fV4dWNeMkN5RJMhV8e`
        WHERE SAFE_CAST(document AS INT64) IN ({cod_lunas_str})
        AND DATE(date) BETWEEN '{fecha_inicio.date()}' AND '{fecha_fin.date()}'
        AND DATE(date) >= '2025-01-01'  -- Filter out erroneous dates
        AND SAFE_CAST(document AS INT64) IS NOT NULL
        ORDER BY date DESC
        """
        
        # Extract HUMAN gestiones  
        query_humano = f"""
        SELECT 
            SAFE_CAST(document AS INT64) as cod_luna,
            date,
            management,
            sub_management,
            n1,
            n2, 
            n3,
            monto_compromiso,
            fecha_compromiso,
            nombre_agente,
            correo_agente,
            phone,
            duracion,
            campaign_name,
            origin,
            weight
        FROM `{self.dataset}.mibotair_P3fV4dWNeMkN5RJMhV8e`
        WHERE SAFE_CAST(document AS INT64) IN ({cod_lunas_str})
        AND DATE(date) BETWEEN '{fecha_inicio.date()}' AND '{fecha_fin.date()}'
        AND SAFE_CAST(document AS INT64) IS NOT NULL
        ORDER BY date DESC
        """
        
        try:
            # Execute queries in parallel for better performance
            logger.debug("🔄 Ejecutando consultas de gestión...")
            df_bot = self.client.query(query_bot).to_dataframe()
            df_humano = self.client.query(query_humano).to_dataframe()
            
            logger.info(f"✅ Gestiones BOT extraídas: {len(df_bot):,} interacciones")
            logger.info(f"✅ Gestiones HUMANAS extraídas: {len(df_humano):,} interacciones")
            
            if not df_bot.empty:
                logger.info(f"🤖 BOT - Cod_lunas únicos: {df_bot['cod_luna'].nunique():,}")
                logger.info(f"🤖 BOT - Período: {df_bot['date'].min()} a {df_bot['date'].max()}")
            
            if not df_humano.empty:
                logger.info(f"👨‍💼 HUMANO - Cod_lunas únicos: {df_humano['cod_luna'].nunique():,}")
                logger.info(f"👨‍💼 HUMANO - Período: {df_humano['date'].min()} a {df_humano['date'].max()}")
            
            return df_bot, df_humano
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo gestiones: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def extract_financial_data(self, archivos_calendario: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract financial data (trandeuda and pagos) using filename dates.
        NO usar creado_el - usar fecha del nombre del archivo.
        """
        if not archivos_calendario:
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info(f"💰 Extrayendo datos financieros para período {self.config.mes_vigencia}")
        
        # Filter trandeuda files by date extracted from filename
        year, month = self.config.mes_vigencia.split('-')
        target_month = int(month)
        target_year = int(year)
        
        # Get all trandeuda files and filter by extracted date
        query_all_trandeuda = f"""
        SELECT DISTINCT archivo
        FROM `{self.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
        """
        
        try:
            all_trandeuda_files = self.client.query(query_all_trandeuda).to_dataframe()
            valid_trandeuda_files = []
            
            for _, row in all_trandeuda_files.iterrows():
                filename = row['archivo']
                extracted_date = self.extract_date_from_filename(filename)
                
                if extracted_date and extracted_date.year == target_year and extracted_date.month == target_month:
                    valid_trandeuda_files.append(filename)
            
            logger.info(f"📄 Archivos trandeuda válidos encontrados: {len(valid_trandeuda_files)}")
            
            # Extract trandeuda data
            if valid_trandeuda_files:
                files_str = "', '".join(valid_trandeuda_files)
                query_deuda = f"""
                SELECT 
                    cod_cuenta,
                    nro_documento,
                    fecha_vencimiento,
                    monto_exigible,
                    archivo
                FROM `{self.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
                WHERE archivo IN ('{files_str}')
                """
                
                df_deuda = self.client.query(query_deuda).to_dataframe()
                logger.info(f"✅ Trandeuda extraída: {len(df_deuda):,} registros")
            else:
                df_deuda = pd.DataFrame()
                logger.warning("⚠️  No se encontraron archivos de trandeuda válidos")
            
            # Extract pagos using fecha_pago column (as specified)
            query_pagos = f"""
            SELECT 
                cod_sistema,
                nro_documento,
                monto_cancelado,
                fecha_pago,
                archivo
            FROM `{self.dataset}.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
            WHERE DATE_TRUNC(fecha_pago, MONTH) = DATE('{self.config.mes_vigencia}-01')
            """
            
            df_pagos = self.client.query(query_pagos).to_dataframe()
            logger.info(f"✅ Pagos extraídos: {len(df_pagos):,} registros")
            
            return df_deuda, df_pagos
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos financieros: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def extract_all_data(self) -> Dict[str, pd.DataFrame]:
        """Extract all required data following the correct temporal validation"""
        logger.info("🚀 Iniciando extracción completa de datos")
        
        # Step 1: Extract calendar
        df_calendario = self.extract_calendario()
        if df_calendario.empty:
            logger.error("❌ No se pudo extraer calendario - abortando extracción")
            return {}
        
        archivos_calendario = df_calendario['ARCHIVO'].tolist()
        
        # Step 2: Extract assignments
        df_asignacion = self.extract_asignacion(archivos_calendario)
        if df_asignacion.empty:
            logger.error("❌ No se pudo extraer asignaciones - abortando extracción")
            return {}
        
        # Step 3: Get period boundaries for temporal validation
        fecha_inicio = df_calendario['FECHA_ASIGNACION'].min()
        fecha_fin = df_calendario['FECHA_CIERRE'].max()
        
        # Handle case where FECHA_CIERRE might be null (open periods)
        if pd.isna(fecha_fin):
            fecha_fin = datetime.now()
            logger.info(f"📅 Período abierto detectado - usando fecha actual como fin: {fecha_fin.date()}")
        
        logger.info(f"⏰ Período de gestión válido: {fecha_inicio.date()} a {fecha_fin.date()}")
        
        # Step 4: Extract gestiones within valid period
        cod_lunas_unicos = df_asignacion['cod_luna'].unique().tolist()
        df_bot, df_humano = self.extract_gestiones_by_period(cod_lunas_unicos, fecha_inicio, fecha_fin)
        
        # Step 5: Extract financial data
        df_deuda, df_pagos = self.extract_financial_data(archivos_calendario)
        
        # Prepare result dictionary
        result = {
            'calendario': df_calendario,
            'asignacion': df_asignacion,
            'voicebot': df_bot,
            'mibotair': df_humano,
            'trandeuda': df_deuda,
            'pagos': df_pagos
        }
        
        # Log summary
        total_records = sum(len(df) for df in result.values())
        logger.success(f"🎉 Extracción completa finalizada")
        logger.info(f"📊 Total de registros extraídos: {total_records:,}")
        
        for table, df in result.items():
            if not df.empty:
                logger.info(f"   📋 {table}: {len(df):,} registros")
            else:
                logger.warning(f"   📭 {table}: Tabla vacía")
        
        return result
    
    def test_connectivity(self) -> bool:
        """Test BigQuery connectivity and table access"""
        try:
            # Test basic connectivity
            test_query = "SELECT 1 as test"
            self.client.query(test_query).result()
            logger.info("✅ Conectividad BigQuery OK")
            
            # Test table access
            calendar_query = f"""
            SELECT COUNT(*) as total_rows
            FROM `{self.dataset}.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v3`
            LIMIT 1
            """
            result = self.client.query(calendar_query).result()
            row_count = list(result)[0].total_rows
            logger.info(f"✅ Acceso a tabla calendario OK - {row_count:,} registros")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error de conectividad: {e}")
            return False