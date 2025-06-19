"""
Business Days Processor for FACO ETL

Handles working days calculations specific to Peru (PE) with support
for same business day comparisons across periods.
"""

import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict
from loguru import logger
import holidays

from core.config import ETLConfig


class PeruHolidaysCalendar(holidays.HolidayBase):
    """
    Custom holidays calendar for Peru with major national holidays
    """
    country = "PE"
    
    def _populate(self, year):
        # Fixed holidays
        self[date(year, 1, 1)] = "Año Nuevo"
        self[date(year, 5, 1)] = "Día del Trabajo" 
        self[date(year, 7, 28)] = "Fiestas Patrias"
        self[date(year, 7, 29)] = "Fiestas Patrias"
        self[date(year, 8, 30)] = "Santa Rosa de Lima"
        self[date(year, 10, 8)] = "Combate de Angamos"
        self[date(year, 11, 1)] = "Todos los Santos"
        self[date(year, 12, 8)] = "Inmaculada Concepción"
        self[date(year, 12, 25)] = "Navidad"
        
        # Variable holidays (simplified - you may want to add more)
        # Maundy Thursday and Good Friday (Easter-based)
        easter = self._easter(year)
        self[easter - timedelta(days=3)] = "Jueves Santo"
        self[easter - timedelta(days=2)] = "Viernes Santo"
    
    def _easter(self, year):
        """Calculate Easter date for given year"""
        # Simplified Easter calculation
        a = year % 19
        b = year // 100
        c = year % 100
        d = b // 4
        e = b % 4
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - d - g + 15) % 30
        i = c // 4
        k = c % 4
        l = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * l) // 451
        month = (h + l - 7 * m + 114) // 31
        day = ((h + l - 7 * m + 114) % 31) + 1
        return date(year, month, day)


class BusinessDaysProcessor:
    """Business days processor with Peru-specific logic"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.country_code = config.country_code
        self.include_saturdays = config.include_saturdays
        
        # Initialize holidays calendar
        if self.country_code == "PE":
            self.holidays_calendar = PeruHolidaysCalendar()
            logger.info("📅 Calendario de feriados de Perú inicializado")
        else:
            self.holidays_calendar = holidays.country_holidays(self.country_code)
            logger.info(f"📅 Calendario de feriados de {self.country_code} inicializado")
        
        logger.info(f"⚙️  Configuración días hábiles: Incluir sábados = {self.include_saturdays}")
    
    def is_business_day(self, check_date: date) -> bool:
        """
        Check if a given date is a business day.
        
        Business day = Monday-Friday (+ Saturday if included) and not a holiday
        """
        if isinstance(check_date, datetime):
            check_date = check_date.date()
        
        # Check if it's a weekend
        weekday = check_date.weekday()  # Monday = 0, Sunday = 6
        
        if weekday == 6:  # Sunday
            return False
        elif weekday == 5:  # Saturday
            return self.include_saturdays
        
        # Check if it's a holiday
        if check_date in self.holidays_calendar:
            logger.debug(f"🏖️  {check_date} es feriado: {self.holidays_calendar[check_date]}")
            return False
        
        return True
    
    def calculate_business_day_of_month(self, target_date: date) -> int:
        """
        Calculate which business day of the month the target date is.
        
        Returns:
            int: Business day number (1, 2, 3, etc.) or 0 if not a business day
        """
        if isinstance(target_date, datetime):
            target_date = target_date.date()
        
        if not self.is_business_day(target_date):
            return 0
        
        # Count business days from the start of the month
        start_of_month = target_date.replace(day=1)
        business_day_count = 0
        
        current_date = start_of_month
        while current_date <= target_date:
            if self.is_business_day(current_date):
                business_day_count += 1
            current_date += timedelta(days=1)
        
        logger.debug(f"📊 {target_date} es el día hábil #{business_day_count} del mes")
        return business_day_count
    
    def get_nth_business_day_of_month(self, year: int, month: int, n: int) -> Optional[date]:
        """
        Get the Nth business day of a given month.
        
        Args:
            year: Year
            month: Month (1-12)
            n: Which business day to find (1st, 2nd, etc.)
            
        Returns:
            date: The Nth business day, or None if not found
        """
        start_date = date(year, month, 1)
        
        # Find the last day of the month
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        end_date = next_month - timedelta(days=1)
        
        business_day_count = 0
        current_date = start_date
        
        while current_date <= end_date:
            if self.is_business_day(current_date):
                business_day_count += 1
                if business_day_count == n:
                    logger.debug(f"📅 Día hábil #{n} de {month}/{year}: {current_date}")
                    return current_date
            current_date += timedelta(days=1)
        
        logger.warning(f"⚠️  No se encontró el día hábil #{n} para {month}/{year}")
        return None
    
    def get_same_business_day_previous_month(self, target_date: date) -> Optional[date]:
        """
        Get the same business day number from the previous month.
        
        This is critical for period-over-period comparisons.
        """
        if isinstance(target_date, datetime):
            target_date = target_date.date()
        
        # Get which business day of the month this is
        business_day_number = self.calculate_business_day_of_month(target_date)
        
        if business_day_number == 0:
            logger.warning(f"⚠️  {target_date} no es un día hábil")
            return None
        
        # Calculate previous month
        if target_date.month == 1:
            prev_year = target_date.year - 1
            prev_month = 12
        else:
            prev_year = target_date.year
            prev_month = target_date.month - 1
        
        # Find the same business day number in previous month
        comparison_date = self.get_nth_business_day_of_month(prev_year, prev_month, business_day_number)
        
        if comparison_date:
            logger.info(f"🔄 Día hábil #{business_day_number}: {target_date} → {comparison_date} (mes anterior)")
        else:
            # If exact business day doesn't exist, get the last business day of previous month
            comparison_date = self.get_last_business_day_of_month(prev_year, prev_month)
            logger.info(f"🔄 Día hábil #{business_day_number} no existe en mes anterior, usando último día hábil: {comparison_date}")
        
        return comparison_date
    
    def get_last_business_day_of_month(self, year: int, month: int) -> Optional[date]:
        """Get the last business day of a given month"""
        # Find the last day of the month
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        end_date = next_month - timedelta(days=1)
        
        # Work backwards to find the last business day
        current_date = end_date
        while current_date.month == month:
            if self.is_business_day(current_date):
                return current_date
            current_date -= timedelta(days=1)
        
        return None
    
    def get_business_days_in_month(self, year: int, month: int) -> List[date]:
        """Get all business days in a given month"""
        start_date = date(year, month, 1)
        
        # Find the last day of the month
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        end_date = next_month - timedelta(days=1)
        
        business_days = []
        current_date = start_date
        
        while current_date <= end_date:
            if self.is_business_day(current_date):
                business_days.append(current_date)
            current_date += timedelta(days=1)
        
        logger.debug(f"📊 Días hábiles en {month}/{year}: {len(business_days)} días")
        return business_days
    
    def create_business_days_mapping(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Create a mapping DataFrame with business day information for a date range.
        
        This is useful for adding business day metadata to your datasets.
        """
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()
        
        logger.info(f"📅 Creando mapeo de días hábiles: {start_date} a {end_date}")
        
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        mapping_data = []
        for date_obj in date_range:
            current_date = date_obj.date()
            
            business_day_of_month = self.calculate_business_day_of_month(current_date)
            is_bday = self.is_business_day(current_date)
            same_bday_prev_month = self.get_same_business_day_previous_month(current_date) if is_bday else None
            
            mapping_data.append({
                'fecha': current_date,
                'ano': current_date.year,
                'mes': current_date.month,
                'dia': current_date.day,
                'dia_semana': current_date.weekday() + 1,  # 1=Monday, 7=Sunday
                'es_dia_habil': is_bday,
                'dia_habil_del_mes': business_day_of_month,
                'es_feriado': current_date in self.holidays_calendar,
                'nombre_feriado': self.holidays_calendar.get(current_date, ''),
                'fecha_mismo_dia_habil_mes_anterior': same_bday_prev_month
            })
        
        df_mapping = pd.DataFrame(mapping_data)
        logger.info(f"✅ Mapeo creado: {len(df_mapping)} días, {df_mapping['es_dia_habil'].sum()} días hábiles")
        
        return df_mapping
    
    def add_business_day_columns(self, df: pd.DataFrame, date_column: str = 'fecha') -> pd.DataFrame:
        """
        Add business day metadata columns to an existing DataFrame.
        
        Args:
            df: DataFrame with a date column
            date_column: Name of the date column
            
        Returns:
            DataFrame with additional business day columns
        """
        if date_column not in df.columns:
            logger.error(f"❌ Columna '{date_column}' no encontrada en DataFrame")
            return df
        
        logger.info(f"📊 Agregando columnas de días hábiles a DataFrame con {len(df)} registros")
        
        # Ensure date column is datetime
        df = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
            df[date_column] = pd.to_datetime(df[date_column])
        
        # Add business day columns
        df['dia_habil_del_mes'] = df[date_column].dt.date.apply(self.calculate_business_day_of_month)
        df['es_dia_habil'] = df[date_column].dt.date.apply(self.is_business_day)
        df['mismo_dia_habil_mes_anterior'] = df[date_column].dt.date.apply(
            self.get_same_business_day_previous_month
        )
        
        # Add day of week information
        df['dia_semana'] = df[date_column].dt.dayofweek + 1  # 1=Monday, 7=Sunday
        df['nombre_dia_semana'] = df[date_column].dt.day_name()
        
        logger.info("✅ Columnas de días hábiles agregadas exitosamente")
        return df
    
    def get_comparison_periods_info(self, target_date: date) -> Dict:
        """
        Get comprehensive information for period comparisons.
        
        This provides all the metadata needed for same-business-day comparisons.
        """
        if isinstance(target_date, datetime):
            target_date = target_date.date()
        
        business_day_num = self.calculate_business_day_of_month(target_date)
        prev_month_date = self.get_same_business_day_previous_month(target_date)
        
        # Get month information
        current_month_business_days = self.get_business_days_in_month(target_date.year, target_date.month)
        
        if target_date.month == 1:
            prev_year, prev_month = target_date.year - 1, 12
        else:
            prev_year, prev_month = target_date.year, target_date.month - 1
        
        prev_month_business_days = self.get_business_days_in_month(prev_year, prev_month)
        
        return {
            'fecha_objetivo': target_date,
            'es_dia_habil': self.is_business_day(target_date),
            'dia_habil_del_mes': business_day_num,
            'total_dias_habiles_mes_actual': len(current_month_business_days),
            'fecha_comparacion_mes_anterior': prev_month_date,
            'total_dias_habiles_mes_anterior': len(prev_month_business_days),
            'puede_comparar': prev_month_date is not None,
            'progreso_mes_actual': business_day_num / len(current_month_business_days) if current_month_business_days else 0
        }
    
    def validate_business_day_logic(self) -> Dict[str, bool]:
        """Validate that business day logic is working correctly"""
        logger.info("🧪 Validando lógica de días hábiles")
        
        validation_results = {}
        
        try:
            # Test known business day
            test_date = date(2025, 6, 19)  # Thursday
            is_bday = self.is_business_day(test_date)
            validation_results['thursday_is_business_day'] = is_bday
            logger.info(f"✅ Jueves es día hábil: {is_bday}")
            
            # Test known weekend
            sunday = date(2025, 6, 22)  # Sunday
            is_weekend = not self.is_business_day(sunday)
            validation_results['sunday_is_not_business_day'] = is_weekend
            logger.info(f"✅ Domingo NO es día hábil: {is_weekend}")
            
            # Test business day calculation
            bday_num = self.calculate_business_day_of_month(test_date)
            validation_results['business_day_calculation'] = bday_num > 0
            logger.info(f"✅ Cálculo de día hábil del mes: {bday_num}")
            
            # Test previous month comparison
            prev_month = self.get_same_business_day_previous_month(test_date)
            validation_results['previous_month_comparison'] = prev_month is not None
            logger.info(f"✅ Comparación mes anterior: {prev_month}")
            
            all_passed = all(validation_results.values())
            logger.info(f"🎯 Validación de días hábiles: {'✅ PASÓ' if all_passed else '❌ FALLÓ'}")
            
            return validation_results
            
        except Exception as e:
            logger.error(f"❌ Error en validación de días hábiles: {e}")
            return {'validation_error': False}