"""
Data Transformer for FACO ETL

Implements business logic for cobranza analytics with dimension aggregation,
first-time tracking, and business days integration.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional
from loguru import logger

from core.config import ETLConfig
from .business_days import BusinessDaysProcessor


class CobranzaTransformer:
    """Transforms raw data into aggregated business dimensions"""
    
    def __init__(self, config: ETLConfig, business_days: BusinessDaysProcessor):
        self.config = config
        self.business_days = business_days
        
        # Business dimension mappings
        self.dimension_fields = [
            'FECHA_SERVICIO',
            'CARTERA', 
            'VENCIMIENTO',
            'FECHA_ASIGNACION',
            'FECHA_INICIO_GESTION',
            'FECHA_CIERRE',
            'OBJ_RECUPERO',
            'GRUPO_RESPUESTA',
            'GLOSA_RESPUESTA', 
            'CANAL',
            'OPERADOR',
            'NIVEL_1',
            'NIVEL_2',
            'NIVEL_3',
            'MOVIL_FIJA',
            'TEMPRANA_ALTAS_CUOTA_FRACCION'
        ]
        
        logger.info(f"🔄 Transformer configurado con {len(self.dimension_fields)} dimensiones")
    
    def transform_all_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform all raw data into business-ready aggregated tables"""
        logger.info("🔄 Iniciando transformación de datos")
        
        transformed_data = {}
        
        # 1. Create base dimensions
        base_dimensions = self._create_base_dimensions(
            raw_data['asignacion'], 
            raw_data['calendario']
        )
        logger.info(f"✅ Dimensiones base creadas: {len(base_dimensions)} registros")
        
        # 2. Process and aggregate gestiones
        df_gestiones_agregadas = self._process_gestiones(
            base_dimensions,
            raw_data['voicebot'],
            raw_data['mibotair']
        )
        logger.info(f"✅ Gestiones agregadas: {len(df_gestiones_agregadas)} registros")
        
        # 3. Add financial metrics
        df_with_financial = self._add_financial_metrics(
            df_gestiones_agregadas,
            raw_data['trandeuda'],
            raw_data['pagos'],
            base_dimensions
        )
        logger.info(f"✅ Métricas financieras agregadas")
        
        # 4. Calculate business day metrics
        df_with_business_days = self.business_days.add_business_day_metrics(df_with_financial)
        logger.info(f"✅ Métricas de días hábiles agregadas")
        
        # 5. Create final aggregated table
        transformed_data['agregada'] = self._finalize_aggregated_table(df_with_business_days)
        
        # 6. Create comparative analysis
        transformed_data['comparativas'] = self._create_comparative_analysis(df_with_business_days)
        
        # 7. Create first-time tracking
        transformed_data['primera_vez'] = self._create_first_time_tracking(
            base_dimensions, raw_data['voicebot'], raw_data['mibotair']
        )
        
        # 8. Create base portfolio summary
        transformed_data['base_cartera'] = self._create_base_portfolio_summary(base_dimensions)
        
        return transformed_data
    
    def _create_base_dimensions(self, df_asignacion: pd.DataFrame, df_calendario: pd.DataFrame) -> pd.DataFrame:
        """Create base dimensions from asignacion and calendario data"""
        try:
            if df_asignacion.empty:
                return pd.DataFrame()
            
            # Merge asignacion with calendario
            df_base = df_asignacion.merge(
                df_calendario[['ARCHIVO', 'FECHA_ASIGNACION', 'FECHA_CIERRE', 'FECHA_TRANDEUDA', 'DIAS_GESTION']],
                left_on='archivo',
                right_on=df_calendario['ARCHIVO'] + '.txt',
                how='left'
            )
            
            # Create derived dimensions
            df_base['CARTERA'] = df_base['archivo'].apply(self._extract_cartera_type)
            df_base['MOVIL_FIJA'] = df_base['negocio']
            df_base['TEMPRANA_ALTAS_CUOTA_FRACCION'] = df_base.apply(self._create_tramo_dimension, axis=1)
            df_base['FECHA_INICIO_GESTION'] = df_base['FECHA_ASIGNACION']
            
            # Calculate objective recovery rate
            df_base['OBJ_RECUPERO'] = df_base['tramo_gestion'].apply(self._calculate_recovery_objective)
            
            return df_base
            
        except Exception as e:
            logger.error(f"Error creando dimensiones base: {e}")
            return pd.DataFrame()
    
    def _extract_cartera_type(self, archivo: str) -> str:
        """Extract cartera type from archivo name"""
        archivo_upper = archivo.upper()
        if 'TEMPRANA' in archivo_upper:
            return 'TEMPRANA'
        elif 'CF_ANN' in archivo_upper or 'CUOTA_FIJA' in archivo_upper:
            return 'CUOTA_FIJA_ANUAL'
        elif 'AN_' in archivo_upper or 'ALTAS' in archivo_upper:
            return 'ALTAS_NUEVAS'
        else:
            return 'OTRAS'
    
    def _create_tramo_dimension(self, row) -> str:
        """Create combined tramo dimension"""
        tramo = str(row.get('tramo_gestion', ''))
        fraccionamiento = str(row.get('fraccionamiento', ''))
        
        result = tramo
        if fraccionamiento == 'SI':
            result += ' - FRACCIONADO'
        
        return result
    
    def _calculate_recovery_objective(self, tramo_gestion: str) -> float:
        """Calculate recovery objective based on tramo"""
        if pd.isna(tramo_gestion):
            return 0.20
        
        tramo_upper = str(tramo_gestion).upper()
        if 'AL VCTO' in tramo_upper or 'VENCIMIENTO' in tramo_upper:
            return 0.15  # 15% for al vencimiento
        elif 'TEMPRANA' in tramo_upper or '4 Y 15' in tramo_upper:
            return 0.25  # 25% for temprana
        else:
            return 0.20  # 20% default
    
    def _process_gestiones(self, base_dimensions: pd.DataFrame, 
                          df_voicebot: pd.DataFrame, df_mibotair: pd.DataFrame) -> pd.DataFrame:
        """Process and aggregate gestiones by business dimensions"""
        all_gestiones = []
        
        # Process voicebot gestiones
        if not df_voicebot.empty:
            df_bot_processed = self._process_bot_gestiones(base_dimensions, df_voicebot)
            if not df_bot_processed.empty:
                all_gestiones.append(df_bot_processed)
        
        # Process mibotair gestiones
        if not df_mibotair.empty:
            df_humano_processed = self._process_humano_gestiones(base_dimensions, df_mibotair)
            if not df_humano_processed.empty:
                all_gestiones.append(df_humano_processed)
        
        # Combine all gestiones
        if all_gestiones:
            df_combined = pd.concat(all_gestiones, ignore_index=True)
            return self._aggregate_by_dimensions(df_combined)
        else:
            return pd.DataFrame()
    
    def _process_bot_gestiones(self, base_dimensions: pd.DataFrame, df_voicebot: pd.DataFrame) -> pd.DataFrame:
        """Process voicebot gestiones with business dimensions"""
        try:
            # Merge with base dimensions
            df_bot = df_voicebot.merge(
                base_dimensions[['cod_luna', 'CARTERA', 'MOVIL_FIJA', 'TEMPRANA_ALTAS_CUOTA_FRACCION', 
                               'FECHA_ASIGNACION', 'FECHA_CIERRE', 'OBJ_RECUPERO', 'cliente', 'cuenta']],
                on='cod_luna',
                how='inner'
            )
            
            if df_bot.empty:
                return pd.DataFrame()
            
            # Add bot-specific dimensions
            df_bot['CANAL'] = 'BOT'
            df_bot['OPERADOR'] = 'SISTEMA_BOT'
            df_bot['GRUPO_RESPUESTA'] = df_bot['management']
            df_bot['GLOSA_RESPUESTA'] = df_bot['management']
            df_bot['NIVEL_1'] = df_bot['management']
            df_bot['NIVEL_2'] = ''
            df_bot['NIVEL_3'] = ''
            df_bot['FECHA_SERVICIO'] = pd.to_datetime(df_bot['date']).dt.date
            df_bot['VENCIMIENTO'] = None  # Could be derived from min_vto if available
            df_bot['FECHA_INICIO_GESTION'] = df_bot['FECHA_ASIGNACION']
            
            # Add first-time tracking
            df_bot = self._add_first_time_tracking(df_bot, ['cliente', 'CARTERA', 'CANAL'])
            
            return df_bot
            
        except Exception as e:
            logger.error(f"Error procesando gestiones bot: {e}")
            return pd.DataFrame()
    
    def _process_humano_gestiones(self, base_dimensions: pd.DataFrame, df_mibotair: pd.DataFrame) -> pd.DataFrame:
        """Process mibotair gestiones with business dimensions"""
        try:
            # Merge with base dimensions
            df_humano = df_mibotair.merge(
                base_dimensions[['cod_luna', 'CARTERA', 'MOVIL_FIJA', 'TEMPRANA_ALTAS_CUOTA_FRACCION',
                               'FECHA_ASIGNACION', 'FECHA_CIERRE', 'OBJ_RECUPERO', 'cliente', 'cuenta']],
                on='cod_luna',
                how='inner'
            )
            
            if df_humano.empty:
                return pd.DataFrame()
            
            # Add humano-specific dimensions
            df_humano['CANAL'] = 'HUMANO'
            df_humano['OPERADOR'] = df_humano['nombre_agente'].fillna('SIN_AGENTE')
            df_humano['GRUPO_RESPUESTA'] = df_humano['management']
            df_humano['GLOSA_RESPUESTA'] = df_humano.apply(self._create_glosa_respuesta, axis=1)
            df_humano['NIVEL_1'] = df_humano['n1'].fillna('')
            df_humano['NIVEL_2'] = df_humano['n2'].fillna('')
            df_humano['NIVEL_3'] = df_humano['n3'].fillna('')
            df_humano['FECHA_SERVICIO'] = pd.to_datetime(df_humano['date']).dt.date
            df_humano['VENCIMIENTO'] = None
            df_humano['FECHA_INICIO_GESTION'] = df_humano['FECHA_ASIGNACION']
            
            # Add first-time tracking
            df_humano = self._add_first_time_tracking(df_humano, ['cliente', 'CARTERA', 'CANAL', 'OPERADOR'])
            
            return df_humano
            
        except Exception as e:
            logger.error(f"Error procesando gestiones humano: {e}")
            return pd.DataFrame()
    
    def _create_glosa_respuesta(self, row) -> str:
        """Create comprehensive glosa_respuesta from n1, n2, n3"""
        components = []
        for field in ['n1', 'n2', 'n3']:
            value = row.get(field)
            if pd.notna(value) and str(value).strip():
                components.append(str(value).strip())
        
        return ' - '.join(components) if components else row.get('management', '')
    
    def _add_first_time_tracking(self, df: pd.DataFrame, groupby_fields: List[str]) -> pd.DataFrame:
        """Add first-time tracking flags to gestiones"""
        try:
            df = df.sort_values('date')
            
            # Mark first interaction per group
            df['es_primera_vez'] = (
                df.groupby(groupby_fields)['date'].transform('min') == df['date']
            )
            
            # Mark first effective contact
            effective_contacts = df[
                df['management'].str.contains('CONTACTO_EFECTIVO|Contacto_Efectivo', case=False, na=False)
            ]
            
            if not effective_contacts.empty:
                first_effective = effective_contacts.groupby(groupby_fields)['date'].transform('min')
                df['es_primer_contacto_efectivo'] = (
                    df['date'].isin(first_effective) & 
                    df['management'].str.contains('CONTACTO_EFECTIVO|Contacto_Efectivo', case=False, na=False)
                )
            else:
                df['es_primer_contacto_efectivo'] = False
            
            return df
            
        except Exception as e:
            logger.error(f"Error agregando tracking primera vez: {e}")
            df['es_primera_vez'] = False
            df['es_primer_contacto_efectivo'] = False
            return df
    
    def _aggregate_by_dimensions(self, df_gestiones: pd.DataFrame) -> pd.DataFrame:
        """Aggregate gestiones by business dimensions"""
        try:
            if df_gestiones.empty:
                return pd.DataFrame()
            
            # Group by all dimension fields
            groupby_fields = [field for field in self.dimension_fields if field in df_gestiones.columns]
            
            # Aggregate metrics
            agg_dict = {
                # Action metrics (each interaction counts)
                'cod_luna': 'count',  # total_interacciones
                'duracion': ['sum', 'mean'],  # duracion_total, duracion_promedio
                
                # Client metrics (unique clients)
                'cliente': 'nunique',  # clientes_unicos_contactados
                'cuenta': 'nunique',   # cuentas_unicas_contactadas
                
                # First-time metrics
                'es_primera_vez': 'sum',  # primera_vez_contactados
                'es_primer_contacto_efectivo': 'sum',  # primera_vez_efectivos
                
                # Financial metrics (for humano channel)
                'monto_compromiso': ['sum', 'count'],  # monto_total, cantidad_compromisos
            }
            
            # Add effectiveness metrics
            df_gestiones['es_contacto_efectivo'] = df_gestiones['management'].str.contains(
                'CONTACTO_EFECTIVO|Contacto_Efectivo', case=False, na=False
            )
            agg_dict['es_contacto_efectivo'] = 'sum'
            
            # Perform aggregation
            df_agg = df_gestiones.groupby(groupby_fields).agg(agg_dict).reset_index()
            
            # Flatten column names
            df_agg.columns = [
                col[0] if col[1] == '' else f"{col[0]}_{col[1]}" 
                for col in df_agg.columns
            ]
            
            # Rename to business-friendly names
            column_mapping = {
                'cod_luna_count': 'total_interacciones',
                'duracion_sum': 'duracion_total_minutos',
                'duracion_mean': 'duracion_promedio_minutos',
                'cliente_nunique': 'clientes_unicos_contactados',
                'cuenta_nunique': 'cuentas_unicas_contactadas',
                'es_primera_vez_sum': 'primera_vez_contactados',
                'es_primer_contacto_efectivo_sum': 'primera_vez_efectivos',
                'es_contacto_efectivo_sum': 'contactos_efectivos',
                'monto_compromiso_sum': 'monto_total_comprometido',
                'monto_compromiso_count': 'cantidad_compromisos'
            }
            
            df_agg = df_agg.rename(columns=column_mapping)
            
            # Calculate KPIs
            df_agg = self._calculate_kpis(df_agg)
            
            return df_agg
            
        except Exception as e:
            logger.error(f"Error agregando por dimensiones: {e}")
            return pd.DataFrame()
    
    def _calculate_kpis(self, df_agg: pd.DataFrame) -> pd.DataFrame:
        """Calculate business KPIs"""
        try:
            # Effectiveness metrics
            df_agg['efectividad_canal'] = (
                df_agg['contactos_efectivos'] / df_agg['total_interacciones']
            ).fillna(0)
            
            df_agg['tasa_compromiso'] = (
                df_agg['cantidad_compromisos'] / df_agg['total_interacciones']
            ).fillna(0)
            
            df_agg['ratio_primera_vez'] = (
                df_agg['primera_vez_contactados'] / df_agg['clientes_unicos_contactados']
            ).fillna(0)
            
            # Average metrics
            df_agg['monto_promedio_compromiso'] = (
                df_agg['monto_total_comprometido'] / df_agg['cantidad_compromisos']
            ).fillna(0)
            
            df_agg['interacciones_por_cliente'] = (
                df_agg['total_interacciones'] / df_agg['clientes_unicos_contactados']
            ).fillna(0)
            
            return df_agg
            
        except Exception as e:
            logger.error(f"Error calculando KPIs: {e}")
            return df_agg
    
    def _add_financial_metrics(self, df_gestiones: pd.DataFrame, df_trandeuda: pd.DataFrame, 
                              df_pagos: pd.DataFrame, base_dimensions: pd.DataFrame) -> pd.DataFrame:
        """Add financial metrics to aggregated data"""
        try:
            # This would be implemented to add financial context
            # For now, return the gestiones data as-is
            return df_gestiones
            
        except Exception as e:
            logger.error(f"Error agregando métricas financieras: {e}")
            return df_gestiones
    
    def _finalize_aggregated_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finalize the main aggregated table"""
        try:
            # Add any final calculations or formatting
            df = df.copy()
            
            # Format dates
            date_columns = ['FECHA_SERVICIO', 'FECHA_ASIGNACION', 'FECHA_INICIO_GESTION', 'FECHA_CIERRE']
            for col in date_columns:
                if col in df.columns:
                    df[f'{col}_FORMATO'] = pd.to_datetime(df[col]).dt.strftime('%d/%m/%Y')
            
            # Round numeric columns
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                if 'ratio' in col.lower() or 'efectividad' in col.lower() or 'tasa' in col.lower():
                    df[col] = df[col].round(4)
                elif 'monto' in col.lower():
                    df[col] = df[col].round(2)
                elif 'duracion' in col.lower():
                    df[col] = df[col].round(1)
            
            logger.info(f"✅ Tabla agregada finalizada: {len(df)} registros, {len(df.columns)} columnas")
            return df
            
        except Exception as e:
            logger.error(f"Error finalizando tabla agregada: {e}")
            return df
    
    def _create_comparative_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create period-over-period comparative analysis"""
        # Placeholder for comparative analysis
        # This would implement same business day comparison logic
        return pd.DataFrame()
    
    def _create_first_time_tracking(self, base_dimensions: pd.DataFrame, 
                                   df_voicebot: pd.DataFrame, df_mibotair: pd.DataFrame) -> pd.DataFrame:
        """Create detailed first-time tracking table"""
        # Placeholder for first-time tracking table
        return pd.DataFrame()
    
    def _create_base_portfolio_summary(self, base_dimensions: pd.DataFrame) -> pd.DataFrame:
        """Create base portfolio summary without gestiones"""
        try:
            if base_dimensions.empty:
                return pd.DataFrame()
            
            summary = base_dimensions.groupby([
                'CARTERA', 'MOVIL_FIJA', 'TEMPRANA_ALTAS_CUOTA_FRACCION', 'FECHA_ASIGNACION'
            ]).agg({
                'cod_luna': 'count',
                'cuenta': 'nunique',
                'cliente': 'nunique',
                'OBJ_RECUPERO': 'mean'
            }).reset_index()
            
            summary.columns = [
                'CARTERA', 'MOVIL_FIJA', 'TEMPRANA_ALTAS_CUOTA_FRACCION', 'FECHA_ASIGNACION',
                'total_cod_lunas_asignados', 'cuentas_unicas_asignadas', 'clientes_unicos_asignados',
                'objetivo_recupero_promedio'
            ]
            
            return summary
            
        except Exception as e:
            logger.error(f"Error creando resumen base cartera: {e}")
            return pd.DataFrame()