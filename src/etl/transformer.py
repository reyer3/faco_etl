"""
Data Transformer for FACO ETL

Implements business logic for aggregating cobranza data by specified dimensions
with differentiation between total actions vs unique clients metrics.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any
from loguru import logger

from core.config import ETLConfig
from etl.business_days import BusinessDaysProcessor


class CobranzaTransformer:
    """Transform raw data into aggregated business dimensions"""
    
    def __init__(self, config: ETLConfig, business_days: BusinessDaysProcessor):
        self.config = config
        self.business_days = business_days
        
        # Define the aggregation dimensions as specified (FIXED: removed duplicates)
        self.aggregation_dimensions = [
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
            'SERVICIO'
        ]
        
        logger.info(f"🔄 Transformer inicializado con {len(self.aggregation_dimensions)} dimensiones de agregación")
    
    def create_base_dimensions(self, df_asignacion: pd.DataFrame, df_calendario: pd.DataFrame) -> pd.DataFrame:
        """Create base dimensions from assignment and calendar data"""
        logger.info("📋 Creando dimensiones base")
        
        # Merge assignment with calendar data
        df_base = df_asignacion.merge(
            df_calendario[['ARCHIVO', 'FECHA_ASIGNACION', 'FECHA_CIERRE', 'FECHA_TRANDEUDA']],
            left_on='archivo',
            right_on=df_calendario['ARCHIVO'] + '.txt',
            how='left'
        )
        
        # Create derived dimensions
        df_base['CARTERA'] = df_base['archivo'].apply(self._extract_cartera_type)
        df_base['SERVICIO'] = df_base['negocio']
        
        # FIXED: Replace problematic management segment creation
        df_base['CARTERA'] = self._create_management_segment_safe(df_base)
        
        # Set management period dates
        df_base['FECHA_INICIO_GESTION'] = df_base['FECHA_ASIGNACION']
        df_base['VENCIMIENTO'] = df_base['min_vto']
        
        # Calculate recovery objective based on business rules
        df_base['OBJ_RECUPERO'] = df_base['tramo_gestion'].apply(self._calculate_recovery_objective)
        
        logger.info(f"✅ Dimensiones base creadas: {len(df_base)} registros")
        return df_base
    
    def _extract_cartera_type(self, filename: str) -> str:
        """Extract portfolio type from filename"""
        if pd.isna(filename):
            return 'OTRAS'
            
        filename_upper = str(filename).upper()
        
        if 'TEMPRANA' in filename_upper:
            return 'TEMPRANA'
        elif 'CF_ANN' in filename_upper or 'CUOTA_FIJA' in filename_upper:
            return 'CUOTA_FIJA_ANUAL'
        elif '_AN_' in filename_upper or 'ALTAS_NUEVAS' in filename_upper:
            return 'ALTAS_NUEVAS'
        elif 'COBRANDING' in filename_upper:
            return 'COBRANDING'
        else:
            return 'OTRAS'
    
    def _create_management_segment_safe(self, df: pd.DataFrame) -> pd.Series:
        """Create management segment combining tramo_gestion and fraccionamiento - SAFE VERSION"""
        segments = []
        for _, row in df.iterrows():
            # FIXED: Safe string concatenation with None checks
            segment = str(row.get('tramo_gestion', '')) if pd.notna(row.get('tramo_gestion')) else ''
            
            if row.get('fraccionamiento') == 'SI':
                segment += ' - FRACCIONADO'
            
            cuota_fracc = row.get('cuota_fracc_act')
            if cuota_fracc and pd.notna(cuota_fracc) and str(cuota_fracc).strip():
                segment += f" - CUOTA_{cuota_fracc}"
                
            segments.append(segment if segment else 'NO_ESPECIFICADO')
        return pd.Series(segments, index=df.index)
    
    def _calculate_recovery_objective(self, tramo_gestion: str) -> float:
        """Calculate recovery objective based on management segment"""
        if pd.isna(tramo_gestion):
            return 0.20
            
        objectives = {
            'AL VCTO': 0.15,        # 15% for at maturity
            'ENTRE 4 Y 15D': 0.25,  # 25% for early collection
            'TEMPRANA': 0.20,       # 20% for early
            'TARDIA': 0.30          # 30% for late
        }
        return objectives.get(str(tramo_gestion), 0.20)  # Default 20%
    
    def process_gestiones_with_first_time_tracking(self, df_gestiones: pd.DataFrame, 
                                                  df_base: pd.DataFrame,
                                                  canal: str) -> pd.DataFrame:
        """
        Process management data with first-time tracking per client and dimension combination.
        
        This is critical for distinguishing between total actions vs unique clients.
        """
        if df_gestiones.empty:
            logger.warning(f"⚠️  No hay gestiones {canal} para procesar")
            return pd.DataFrame()
        
        logger.info(f"🔄 Procesando gestiones {canal} con tracking de primera vez")
        
        # Merge with base dimensions
        df_enriched = df_gestiones.merge(df_base, on='cod_luna', how='inner')
        
        if df_enriched.empty:
            logger.warning(f"⚠️  No hay coincidencias entre gestiones {canal} y asignaciones")
            return pd.DataFrame()
        
        # Add channel-specific columns
        df_enriched['CANAL'] = canal
        
        if canal == 'BOT':
            df_enriched['OPERADOR'] = 'SISTEMA_BOT'
            df_enriched['GRUPO_RESPUESTA'] = df_enriched['management'].fillna('NO_DISPONIBLE')
            df_enriched['GLOSA_RESPUESTA'] = df_enriched['management'].fillna('NO_DISPONIBLE')
            df_enriched['NIVEL_1'] = df_enriched['management'].fillna('NO_DISPONIBLE')
            df_enriched['NIVEL_2'] = ''
            df_enriched['NIVEL_3'] = ''
            df_enriched['monto_compromiso'] = 0  # Bots don't handle money commitments
        else:  # HUMANO
            df_enriched['OPERADOR'] = df_enriched['nombre_agente'].fillna('SIN_AGENTE')
            df_enriched['GRUPO_RESPUESTA'] = df_enriched['management'].fillna('NO_DISPONIBLE')
            df_enriched['GLOSA_RESPUESTA'] = self._create_detailed_response_safe(df_enriched)
            df_enriched['NIVEL_1'] = df_enriched['n1'].fillna('')
            df_enriched['NIVEL_2'] = df_enriched['n2'].fillna('')
            df_enriched['NIVEL_3'] = df_enriched['n3'].fillna('')
            df_enriched['monto_compromiso'] = df_enriched['monto_compromiso'].fillna(0)
        
        # Add service date and business day information
        df_enriched['FECHA_SERVICIO'] = df_enriched['date'].dt.date
        df_enriched = self.business_days.add_business_day_columns(df_enriched, 'date')
        
        # Mark first-time interactions per client and dimension combination
        df_enriched = self._mark_first_time_interactions(df_enriched)
        
        # Mark first effective contact per client
        df_enriched = self._mark_first_effective_contact(df_enriched)
        
        logger.info(f"✅ Gestiones {canal} procesadas: {len(df_enriched)} interacciones")
        return df_enriched
    
    def _create_detailed_response_safe(self, df: pd.DataFrame) -> pd.Series:
        """Create detailed response combining n1, n2, n3 levels - SAFE VERSION"""
        responses = []
        for _, row in df.iterrows():
            parts = []
            for level in ['n1', 'n2', 'n3']:
                value = row.get(level)
                if pd.notna(value) and str(value).strip():
                    parts.append(str(value).strip())
            
            if parts:
                response = ' - '.join(parts)
            else:
                # Fallback to management field
                management = row.get('management')
                response = str(management) if pd.notna(management) else 'NO_DISPONIBLE'
            
            responses.append(response)
        return pd.Series(responses, index=df.index)
    
    def _mark_first_time_interactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mark first-time interactions per client and key dimension combinations"""
        df = df.sort_values('date').copy()
        
        # Key dimension combinations for first-time tracking
        dimension_combinations = [
            ['cliente', 'CARTERA', 'CANAL'],
            ['cliente', 'CARTERA', 'CANAL', 'OPERADOR'],
            ['cliente', 'CANAL'],
            ['cliente', 'GRUPO_RESPUESTA'],
        ]
        
        for dims in dimension_combinations:
            column_name = f"es_primera_vez_{'_'.join(dims).lower()}"
            df[column_name] = (
                df.groupby(dims)['date'].transform('min') == df['date']
            )
        
        # General first-time flag (first interaction ever for this client)
        df['es_primera_vez_cliente'] = (
            df.groupby('cliente')['date'].transform('min') == df['date']
        )
        
        logger.debug("✅ Flags de primera vez marcados")
        return df
    
    def _mark_first_effective_contact(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mark first effective contact per client"""
        effective_contacts = df[
            df['management'].isin(['CONTACTO_EFECTIVO', 'Contacto_Efectivo'])
        ].copy()
        
        if not effective_contacts.empty:
            df['es_primer_contacto_efectivo'] = (
                df.groupby('cliente')['date'].transform(
                    lambda x: x == x.min() if df.loc[x.index, 'management'].isin(['CONTACTO_EFECTIVO', 'Contacto_Efectivo']).any() else False
                )
            )
        else:
            df['es_primer_contacto_efectivo'] = False
        
        return df
    
    def aggregate_by_dimensions(self, df_gestiones_enriched: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate data by the specified business dimensions.
        
        Differentiates between:
        - Total actions (each interaction counts)
        - Unique clients (each client counts once per dimension combination)
        """
        if df_gestiones_enriched.empty:
            return pd.DataFrame()
        
        logger.info(f"📊 Agregando por dimensiones de negocio: {len(df_gestiones_enriched)} registros")
        
        # Ensure all required dimensions exist
        for dim in self.aggregation_dimensions:
            if dim not in df_gestiones_enriched.columns:
                logger.warning(f"⚠️  Dimensión faltante: {dim}, agregando valor por defecto")
                df_gestiones_enriched[dim] = 'NO_DISPONIBLE'
        
        # Group by all specified dimensions
        grouped = df_gestiones_enriched.groupby(self.aggregation_dimensions)
        
        # Aggregate metrics
        aggregated = grouped.agg({
            # ACTIONS METRICS (each interaction counts)
            'cod_luna': 'count',  # total_interacciones
            'duracion': ['sum', 'mean'],  # duracion_total, duracion_promedio
            
            # CLIENT METRICS (unique clients)
            'cliente': 'nunique',  # clientes_unicos_contactados
            
            # EFFECTIVENESS METRICS
            'management': [
                lambda x: (x.isin(['CONTACTO_EFECTIVO', 'Contacto_Efectivo'])).sum(),  # contactos_efectivos
                lambda x: (x.str.contains('COMPROMISO|Compromiso', na=False)).sum()  # compromisos_declarados
            ],
            
            # FIRST-TIME METRICS
            'es_primera_vez_cliente': 'sum',  # primera_vez_contactados
            'es_primer_contacto_efectivo': 'sum',  # primera_vez_efectivos
            'es_primera_vez_cliente_cartera_canal': 'sum',  # primera_vez_por_cartera_canal
            
            # FINANCIAL METRICS (only for human channel)
            'monto_compromiso': ['sum', 'count'],  # monto_total, cantidad_compromisos
            
            # BUSINESS DAY METRICS
            'dia_habil_del_mes': 'first',  # business day of month
            'es_dia_habil': 'first',  # is business day
            
        }).reset_index()
        
        # Flatten column names
        aggregated.columns = [
            '_'.join(col).strip('_') if isinstance(col, tuple) else col 
            for col in aggregated.columns
        ]
        
        # Rename columns for clarity
        column_mapping = {
            'cod_luna_count': 'total_interacciones',
            'duracion_sum': 'duracion_total_minutos',
            'duracion_mean': 'duracion_promedio_minutos',
            'cliente_nunique': 'clientes_unicos_contactados',
            'management_<lambda_0>': 'contactos_efectivos',
            'management_<lambda_1>': 'compromisos_declarados',
            'es_primera_vez_cliente_sum': 'primera_vez_contactados',
            'es_primer_contacto_efectivo_sum': 'primera_vez_efectivos',
            'es_primera_vez_cliente_cartera_canal_sum': 'primera_vez_cartera_canal',
            'monto_compromiso_sum': 'monto_total_comprometido',
            'monto_compromiso_count': 'cantidad_compromisos',
            'dia_habil_del_mes_first': 'dia_habil_del_mes',
            'es_dia_habil_first': 'es_dia_habil'
        }
        
        # Apply column mapping (only for existing columns)
        for old_name, new_name in column_mapping.items():
            if old_name in aggregated.columns:
                aggregated = aggregated.rename(columns={old_name: new_name})
        
        # Calculate KPIs
        aggregated = self._calculate_aggregated_kpis(aggregated)
        
        # Add DD/MM/YYYY format
        aggregated['FECHA_FORMATO'] = pd.to_datetime(aggregated['FECHA_SERVICIO']).dt.strftime('%d/%m/%Y')
        
        logger.info(f"✅ Agregación completada: {len(aggregated)} combinaciones de dimensiones")
        return aggregated
    
    def _calculate_aggregated_kpis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate KPIs for aggregated data"""
        # Effectiveness ratios
        df['efectividad_canal'] = np.where(
            df['total_interacciones'] > 0,
            df['contactos_efectivos'] / df['total_interacciones'],
            0
        )
        
        df['tasa_compromiso'] = np.where(
            df['total_interacciones'] > 0,
            df['cantidad_compromisos'] / df['total_interacciones'],
            0
        )
        
        # First-time ratios
        df['ratio_primera_vez'] = np.where(
            df['clientes_unicos_contactados'] > 0,
            df['primera_vez_contactados'] / df['clientes_unicos_contactados'],
            0
        )
        
        # Productivity metrics
        df['interacciones_por_cliente'] = np.where(
            df['clientes_unicos_contactados'] > 0,
            df['total_interacciones'] / df['clientes_unicos_contactados'],
            0
        )
        
        # Financial ratios (for human channel)
        df['monto_promedio_compromiso'] = np.where(
            df['cantidad_compromisos'] > 0,
            df['monto_total_comprometido'] / df['cantidad_compromisos'],
            0
        )
        
        return df
    
    def create_period_comparisons(self, df_current: pd.DataFrame) -> pd.DataFrame:
        """
        Create period-over-period comparisons using same business day logic.
        
        This is critical for the comparative analysis Ricky requested.
        """
        if df_current.empty:
            return pd.DataFrame()
        
        logger.info("🔄 Creando comparativas de período usando mismo día hábil")
        
        comparisons = []
        
        for _, row in df_current.iterrows():
            current_date = pd.to_datetime(row['FECHA_SERVICIO']).date()
            
            # Get same business day from previous month
            prev_month_date = self.business_days.get_same_business_day_previous_month(current_date)
            
            if prev_month_date:
                comparison_data = row.to_dict()
                comparison_data.update({
                    'fecha_actual': current_date,
                    'fecha_comparacion': prev_month_date,
                    'puede_comparar': True,
                    'dia_habil_numero': self.business_days.calculate_business_day_of_month(current_date),
                })
                
                # Get comparison info
                comparison_info = self.business_days.get_comparison_periods_info(current_date)
                comparison_data.update(comparison_info)
                
                comparisons.append(comparison_data)
        
        if comparisons:
            df_comparisons = pd.DataFrame(comparisons)
            logger.info(f"✅ Comparativas creadas: {len(df_comparisons)} registros")
            return df_comparisons
        else:
            logger.warning("⚠️  No se pudieron crear comparativas")
            return pd.DataFrame()
    
    def transform_all_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Main transformation method that orchestrates all transformations"""
        logger.info("🚀 Iniciando transformación completa de datos")
        
        if not raw_data or all(df.empty for df in raw_data.values()):
            logger.error("❌ No hay datos raw para transformar")
            return {}
        
        # Extract DataFrames
        df_calendario = raw_data.get('calendario', pd.DataFrame())
        df_asignacion = raw_data.get('asignacion', pd.DataFrame())
        df_voicebot = raw_data.get('voicebot', pd.DataFrame())
        df_mibotair = raw_data.get('mibotair', pd.DataFrame())
        df_trandeuda = raw_data.get('trandeuda', pd.DataFrame())
        df_pagos = raw_data.get('pagos', pd.DataFrame())
        
        # Step 1: Create base dimensions
        df_base = self.create_base_dimensions(df_asignacion, df_calendario)
        
        # Step 2: Process bot gestiones
        df_bot_enriched = pd.DataFrame()
        if not df_voicebot.empty:
            df_bot_enriched = self.process_gestiones_with_first_time_tracking(
                df_voicebot, df_base, 'BOT'
            )
        
        # Step 3: Process human gestiones
        df_humano_enriched = pd.DataFrame()
        if not df_mibotair.empty:
            df_humano_enriched = self.process_gestiones_with_first_time_tracking(
                df_mibotair, df_base, 'HUMANO'
            )
        
        # Step 4: Combine all gestiones
        gestiones_combined = []
        if not df_bot_enriched.empty:
            gestiones_combined.append(df_bot_enriched)
        if not df_humano_enriched.empty:
            gestiones_combined.append(df_humano_enriched)
        
        if gestiones_combined:
            df_all_gestiones = pd.concat(gestiones_combined, ignore_index=True)
            logger.info(f"📊 Total gestiones combinadas: {len(df_all_gestiones)}")
        else:
            logger.warning("⚠️  No hay gestiones para procesar")
            return {}
        
        # Step 5: Aggregate by dimensions
        df_agregada = self.aggregate_by_dimensions(df_all_gestiones)
        
        # Step 6: Create comparisons
        df_comparativas = self.create_period_comparisons(df_agregada)
        
        # Step 7: Create first-time tracking table
        df_primera_vez = self._create_first_time_tracking_table(df_all_gestiones)
        
        # Step 8: Create base portfolio metrics
        df_base_cartera = self._create_base_portfolio_metrics(df_base, df_trandeuda, df_pagos)
        
        # Prepare result
        result = {
            'agregada': df_agregada,
            'comparativas': df_comparativas,
            'primera_vez': df_primera_vez,
            'base_cartera': df_base_cartera
        }
        
        # Log transformation summary
        logger.success("🎉 Transformación completa finalizada")
        for table, df in result.items():
            if not df.empty:
                logger.info(f"   📋 {table}: {len(df):,} registros, {len(df.columns)} columnas")
            else:
                logger.warning(f"   📭 {table}: Tabla vacía")
        
        return result
    
    def _create_first_time_tracking_table(self, df_gestiones: pd.DataFrame) -> pd.DataFrame:
        """Create dedicated table for first-time interaction tracking"""
        if df_gestiones.empty:
            return pd.DataFrame()
        
        # Filter only first-time interactions
        first_time_df = df_gestiones[df_gestiones['es_primera_vez_cliente'] == True].copy()
        
        if first_time_df.empty:
            return pd.DataFrame()
        
        # Select relevant columns for tracking
        tracking_columns = [
            'cliente', 'FECHA_SERVICIO', 'CARTERA', 'CANAL', 'OPERADOR',
            'GRUPO_RESPUESTA', 'dia_habil_del_mes', 'es_primer_contacto_efectivo'
        ]
        
        df_tracking = first_time_df[tracking_columns].copy()
        df_tracking['timestamp_primera_interaccion'] = pd.Timestamp.now()
        
        logger.info(f"📝 Tabla de primera vez creada: {len(df_tracking)} clientes únicos")
        return df_tracking
    
    def _create_base_portfolio_metrics(self, df_base: pd.DataFrame, 
                                     df_trandeuda: pd.DataFrame, 
                                     df_pagos: pd.DataFrame) -> pd.DataFrame:
        """Create base portfolio metrics table"""
        logger.info("📊 Creando métricas base de cartera")
        
        # Aggregate by portfolio dimensions
        portfolio_dims = ['CARTERA', 'FECHA_ASIGNACION', 'SERVICIO']
        
        df_portfolio = df_base.groupby(portfolio_dims).agg({
            'cod_luna': 'count',
            'cuenta': 'nunique',
            'cliente': 'nunique'
        }).reset_index()
        
        df_portfolio.columns = portfolio_dims + ['total_cod_lunas', 'cuentas_unicas', 'clientes_unicos']
        
        # Add financial metrics if available
        if not df_trandeuda.empty:
            # Aggregate debt by account
            debt_summary = df_trandeuda.groupby('cod_cuenta').agg({
                'monto_exigible': 'sum'
            }).reset_index()
            debt_summary['cod_cuenta'] = debt_summary['cod_cuenta'].astype(str)
            
            # Merge with base (need to match account format)
            df_base_str = df_base.copy()
            df_base_str['cuenta_str'] = df_base_str['cuenta'].astype(str)
            
            debt_by_portfolio = df_base_str.merge(debt_summary, left_on='cuenta_str', right_on='cod_cuenta', how='left')
            debt_aggregated = debt_by_portfolio.groupby(portfolio_dims)['monto_exigible'].sum().reset_index()
            
            df_portfolio = df_portfolio.merge(debt_aggregated, on=portfolio_dims, how='left')
            df_portfolio['monto_exigible'] = df_portfolio['monto_exigible'].fillna(0)
        
        if not df_pagos.empty:
            # Similar process for payments
            payment_summary = df_pagos.groupby('cod_sistema').agg({
                'monto_cancelado': 'sum'
            }).reset_index()
            payment_summary['cod_sistema'] = payment_summary['cod_sistema'].astype(str)
            
            df_base_str = df_base.copy()
            df_base_str['cuenta_str'] = df_base_str['cuenta'].astype(str)
            
            payment_by_portfolio = df_base_str.merge(payment_summary, left_on='cuenta_str', right_on='cod_sistema', how='left')
            payment_aggregated = payment_by_portfolio.groupby(portfolio_dims)['monto_cancelado'].sum().reset_index()
            
            df_portfolio = df_portfolio.merge(payment_aggregated, on=portfolio_dims, how='left')
            df_portfolio['monto_cancelado'] = df_portfolio['monto_cancelado'].fillna(0)
            
            # Calculate recovery ratio
            df_portfolio['ratio_recuperacion'] = np.where(
                df_portfolio['monto_exigible'] > 0,
                df_portfolio['monto_cancelado'] / df_portfolio['monto_exigible'],
                0
            )
        
        logger.info(f"✅ Métricas base de cartera: {len(df_portfolio)} combinaciones")
        return df_portfolio