"""
Business Days Processor for FACO ETL

Handles working days calculations for Peru with custom holidays and comparisons.
"""

import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from holidays import Peru
from loguru import logger

from core.config import ETLConfig


class BusinessDaysProcessor:
    """Handles business days calculations with Peru holidays"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.country_code = config.country_code
        self.include_saturdays = config.include_saturdays
        
        # Initialize Peru holidays
        self.holidays = Peru(years=range(2020, 2030))
        
        # Add custom Peru holidays if needed
        self._add_custom_holidays()
        
        logger.info(f"📅 BusinessDays configurado para {self.country_code} - Incluir sábados: {self.include_saturdays}")
    
    def _add_custom_holidays(self):
        """Add custom holidays specific to Peru business"""
        # Add any company-specific or regional holidays
        # self.holidays.append({"2025-06-29": "San Pedro y San Pablo"})
        pass
    
    def calculate_business_day_of_month(self, target_date: date) -> int:
        """Calculate which business day of the month the target date is"""
        try:
            first_day = target_date.replace(day=1)
            
            business_days = self._get_business_days_range(first_day, target_date)
            return len(business_days)
            
        except Exception as e:
            logger.error(f"Error calculando día hábil del mes para {target_date}: {e}")
            return 1
    
    def get_same_business_day_previous_month(self, target_date: date) -> Optional[date]:
        """Get the date of the same business day in the previous month"""
        try:
            business_day_number = self.calculate_business_day_of_month(target_date)
            
            # Calculate previous month
            if target_date.month == 1:
                prev_month_first = target_date.replace(year=target_date.year - 1, month=12, day=1)
            else:
                prev_month_first = target_date.replace(month=target_date.month - 1, day=1)
            
            # Get last day of previous month
            if prev_month_first.month == 12:
                prev_month_last = prev_month_first.replace(year=prev_month_first.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                prev_month_last = prev_month_first.replace(month=prev_month_first.month + 1, day=1) - timedelta(days=1)
            
            # Get business days of previous month
            prev_month_business_days = self._get_business_days_range(prev_month_first, prev_month_last)
            
            # Return the business day at the same position, or last if not enough days
            if len(prev_month_business_days) >= business_day_number:
                return prev_month_business_days[business_day_number - 1]
            else:
                return prev_month_business_days[-1] if prev_month_business_days else None
                
        except Exception as e:
            logger.error(f"Error obteniendo mismo día hábil mes anterior para {target_date}: {e}")
            return None
    
    def _get_business_days_range(self, start_date: date, end_date: date) -> List[date]:
        """Get list of business days between start and end date (inclusive)"""
        business_days = []
        current_date = start_date
        
        while current_date <= end_date:
            if self._is_business_day(current_date):
                business_days.append(current_date)
            current_date += timedelta(days=1)
        
        return business_days
    
    def _is_business_day(self, check_date: date) -> bool:
        """Check if a date is a business day"""
        # Check if it's a weekend
        weekday = check_date.weekday()  # Monday = 0, Sunday = 6
        
        if weekday == 6:  # Sunday
            return False
        elif weekday == 5 and not self.include_saturdays:  # Saturday
            return False
        
        # Check if it's a holiday
        if check_date in self.holidays:
            return False
        
        return True
    
    def add_business_day_metrics(self, df: pd.DataFrame, date_column: str = 'FECHA_SERVICIO') -> pd.DataFrame:
        """Add business day metrics to a DataFrame"""
        try:
            df = df.copy()
            
            # Ensure date column is date type
            if df[date_column].dtype == 'object':
                df[date_column] = pd.to_datetime(df[date_column]).dt.date
            elif hasattr(df[date_column].dtype, 'type') and 'datetime' in str(df[date_column].dtype):
                df[date_column] = df[date_column].dt.date
            
            # Add business day metrics
            df['DIA_HABIL_MES'] = df[date_column].apply(
                lambda x: self.calculate_business_day_of_month(x) if pd.notna(x) else None
            )
            
            df['FECHA_MISMO_DIA_HABIL_MES_ANTERIOR'] = df[date_column].apply(
                lambda x: self.get_same_business_day_previous_month(x) if pd.notna(x) else None
            )
            
            df['ES_DIA_HABIL'] = df[date_column].apply(
                lambda x: self._is_business_day(x) if pd.notna(x) else False
            )
            
            df['DIA_SEMANA'] = df[date_column].apply(
                lambda x: x.strftime('%A') if pd.notna(x) else None
            )
            
            logger.info(f"✅ Métricas de días hábiles agregadas a {len(df)} registros")
            return df
            
        except Exception as e:
            logger.error(f"Error agregando métricas de días hábiles: {e}")
            return df
    
    def get_business_days_summary(self, month_year: str) -> Dict:
        """Get summary of business days for a given month"""
        try:
            year, month = map(int, month_year.split('-'))
            first_day = date(year, month, 1)
            
            # Get last day of month
            if month == 12:
                last_day = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                last_day = date(year, month + 1, 1) - timedelta(days=1)
            
            business_days = self._get_business_days_range(first_day, last_day)
            total_days = (last_day - first_day).days + 1
            
            return {
                'mes': month_year,
                'total_dias': total_days,
                'dias_habiles': len(business_days),
                'dias_fin_semana': total_days - len(business_days),
                'primer_dia_habil': business_days[0] if business_days else None,
                'ultimo_dia_habil': business_days[-1] if business_days else None,
                'feriados_en_mes': [d for d in self.holidays if first_day <= d <= last_day]
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo resumen días hábiles para {month_year}: {e}")
            return {}
    
    def create_business_calendar(self, year: int) -> pd.DataFrame:
        """Create a business calendar DataFrame for a full year"""
        try:
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)
            
            calendar_data = []
            current_date = start_date
            
            while current_date <= end_date:
                business_day_of_month = self.calculate_business_day_of_month(current_date)
                is_business_day = self._is_business_day(current_date)
                
                calendar_data.append({
                    'fecha': current_date,
                    'año': current_date.year,
                    'mes': current_date.month,
                    'dia': current_date.day,
                    'dia_semana': current_date.strftime('%A'),
                    'es_dia_habil': is_business_day,
                    'dia_habil_mes': business_day_of_month if is_business_day else None,
                    'es_feriado': current_date in self.holidays,
                    'es_fin_semana': current_date.weekday() >= 5
                })
                
                current_date += timedelta(days=1)
            
            df_calendar = pd.DataFrame(calendar_data)
            logger.info(f"📅 Calendario de negocio creado para {year}: {len(df_calendar)} días")
            
            return df_calendar
            
        except Exception as e:
            logger.error(f"Error creando calendario de negocio para {year}: {e}")
            return pd.DataFrame()
    
    def validate_business_day_logic(self) -> Dict:
        """Validate business day calculation logic with known dates"""
        test_cases = [
            date(2025, 6, 19),  # Thursday - should be business day
            date(2025, 6, 21),  # Saturday - should not be business day (unless included)
            date(2025, 6, 22),  # Sunday - should not be business day
            date(2025, 1, 1),   # New Year - should not be business day
            date(2025, 7, 28),  # Fiestas Patrias - should not be business day
        ]
        
        results = {}
        for test_date in test_cases:
            results[str(test_date)] = {
                'es_dia_habil': self._is_business_day(test_date),
                'dia_habil_mes': self.calculate_business_day_of_month(test_date),
                'dia_semana': test_date.strftime('%A'),
                'es_feriado': test_date in self.holidays
            }
        
        logger.info("🧪 Validación de lógica días hábiles completada")
        return results