"""
Basic tests for FACO ETL components

Run these tests to verify the ETL implementation is working correctly.
"""

import pytest
import pandas as pd
from datetime import datetime, date
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.config import ETLConfig
from etl.business_days import BusinessDaysProcessor
from etl.transformer import CobranzaTransformer


class TestETLComponents:
    """Test core ETL components functionality"""
    
    def setup_method(self):
        """Setup test configuration"""
        self.config = ETLConfig(
            project_id="test-project",
            dataset_id="test_dataset", 
            mes_vigencia="2025-06",
            estado_vigencia="abierto",
            dry_run=True
        )
    
    def test_config_validation(self):
        """Test configuration validation"""
        # Valid config
        try:
            self.config.validate()
            assert True
        except ValueError:
            assert False, "Valid config should not raise ValueError"
        
        # Invalid config
        invalid_config = ETLConfig(
            project_id="",
            mes_vigencia="invalid-date"
        )
        
        with pytest.raises(ValueError):
            invalid_config.validate()
    
    def test_business_days_processor(self):
        """Test business days calculations"""
        processor = BusinessDaysProcessor(self.config)
        
        # Test known dates
        test_date = date(2025, 6, 19)  # Thursday
        business_day = processor.calculate_business_day_of_month(test_date)
        
        assert business_day > 0, "Business day calculation should return positive number"
        assert isinstance(business_day, int), "Business day should be integer"
        
        # Test weekend
        weekend_date = date(2025, 6, 22)  # Sunday
        is_business_day = processor._is_business_day(weekend_date)
        assert not is_business_day, "Sunday should not be a business day"
        
        # Test previous month calculation
        prev_month_date = processor.get_same_business_day_previous_month(test_date)
        assert prev_month_date is not None, "Should return a date for previous month"
        assert prev_month_date < test_date, "Previous month date should be earlier"
    
    def test_business_days_dataframe_integration(self):
        """Test business days integration with DataFrame"""
        processor = BusinessDaysProcessor(self.config)
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'FECHA_SERVICIO': [
                date(2025, 6, 19),  # Thursday
                date(2025, 6, 20),  # Friday
                date(2025, 6, 21),  # Saturday
            ],
            'metric': [100, 200, 300]
        })
        
        # Add business day metrics
        result_df = processor.add_business_day_metrics(test_df)
        
        # Validate results
        assert 'DIA_HABIL_MES' in result_df.columns
        assert 'ES_DIA_HABIL' in result_df.columns
        assert 'DIA_SEMANA' in result_df.columns
        
        # Check business day calculations
        assert result_df.loc[0, 'ES_DIA_HABIL'] == True  # Thursday
        assert result_df.loc[1, 'ES_DIA_HABIL'] == True  # Friday  
        # Saturday depends on include_saturdays setting
        
        # Check day names
        assert result_df.loc[0, 'DIA_SEMANA'] == 'Thursday'
        assert result_df.loc[1, 'DIA_SEMANA'] == 'Friday'
        assert result_df.loc[2, 'DIA_SEMANA'] == 'Saturday'
    
    def test_transformer_dimension_creation(self):
        """Test transformer dimension creation logic"""
        processor = BusinessDaysProcessor(self.config)
        transformer = CobranzaTransformer(self.config, processor)
        
        # Test cartera type extraction
        test_cases = [
            ("Cartera_Agencia_Cobranding_Gestion_Temprana_20250617.txt", "TEMPRANA"),
            ("Cartera_Agencia_Cobranding_Gestion_CF_ANN_20250617.txt", "CUOTA_FIJA_ANUAL"),
            ("Cartera_Agencia_Cobranding_Gestion_AN_20250617.txt", "ALTAS_NUEVAS"),
            ("Other_File_Name.txt", "OTRAS")
        ]
        
        for filename, expected_cartera in test_cases:
            result = transformer._extract_cartera_type(filename)
            assert result == expected_cartera, f"Failed for {filename}: expected {expected_cartera}, got {result}"
    
    def test_transformer_recovery_objective(self):
        """Test recovery objective calculation"""
        processor = BusinessDaysProcessor(self.config)
        transformer = CobranzaTransformer(self.config, processor)
        
        test_cases = [
            ("AL VCTO", 0.15),
            ("ENTRE 4 Y 15D", 0.25),
            ("TEMPRANA", 0.25),
            ("OTHER", 0.20),
            (None, 0.20)
        ]
        
        for tramo, expected_obj in test_cases:
            result = transformer._calculate_recovery_objective(tramo)
            assert result == expected_obj, f"Failed for {tramo}: expected {expected_obj}, got {result}"
    
    def test_transformer_first_time_tracking(self):
        """Test first-time tracking logic"""
        processor = BusinessDaysProcessor(self.config)
        transformer = CobranzaTransformer(self.config, processor)
        
        # Create test data with multiple interactions per client
        test_df = pd.DataFrame({
            'cliente': [1, 1, 2, 2, 1],
            'CARTERA': ['A', 'A', 'B', 'B', 'A'],
            'CANAL': ['BOT', 'BOT', 'HUMANO', 'HUMANO', 'BOT'],
            'date': pd.to_datetime([
                '2025-06-19 10:00:00',
                '2025-06-19 14:00:00', 
                '2025-06-19 11:00:00',
                '2025-06-20 09:00:00',
                '2025-06-20 15:00:00'
            ]),
            'management': ['CONTACTO_EFECTIVO', 'NO_CONTESTA', 'CONTACTO_EFECTIVO', 'CONTACTO_EFECTIVO', 'CONTACTO_EFECTIVO']
        })
        
        # Apply first-time tracking
        result_df = transformer._add_first_time_tracking(test_df, ['cliente', 'CARTERA', 'CANAL'])
        
        # Validate first-time flags
        assert 'es_primera_vez' in result_df.columns
        assert 'es_primer_contacto_efectivo' in result_df.columns
        
        # Check that first interactions are marked correctly
        # First BOT interaction for client 1 in cartera A should be marked as first
        first_interaction_mask = (
            (result_df['cliente'] == 1) & 
            (result_df['CARTERA'] == 'A') & 
            (result_df['CANAL'] == 'BOT')
        )
        first_interactions = result_df[first_interaction_mask].sort_values('date')
        assert first_interactions.iloc[0]['es_primera_vez'] == True
        assert first_interactions.iloc[1]['es_primera_vez'] == False
    
    def test_mock_data_creation(self):
        """Test mock data creation for testing without BigQuery"""
        # Create mock asignacion data
        mock_asignacion = pd.DataFrame({
            'cod_luna': [1, 2, 3],
            'cuenta': [101, 102, 103],
            'cliente': [1001, 1002, 1003],
            'telefono': [123456789, 987654321, 555666777],
            'tramo_gestion': ['AL VCTO', 'ENTRE 4 Y 15D', 'AL VCTO'],
            'negocio': ['MOVIL', 'FIJA', 'MOVIL'],
            'archivo': ['test_file.txt', 'test_file.txt', 'test_file.txt'],
            'fraccionamiento': ['NO', 'SI', 'NO']
        })
        
        # Create mock calendario data
        mock_calendario = pd.DataFrame({
            'ARCHIVO': ['test_file'],
            'FECHA_ASIGNACION': [date(2025, 6, 1)],
            'FECHA_CIERRE': [date(2025, 6, 30)],
            'FECHA_TRANDEUDA': [date(2025, 6, 1)],
            'DIAS_GESTION': [30]
        })
        
        # Test dimension creation
        processor = BusinessDaysProcessor(self.config)
        transformer = CobranzaTransformer(self.config, processor)
        
        base_dimensions = transformer._create_base_dimensions(mock_asignacion, mock_calendario)
        
        # Validate basic structure
        assert not base_dimensions.empty
        assert 'CARTERA' in base_dimensions.columns
        assert 'OBJ_RECUPERO' in base_dimensions.columns
        assert 'TEMPRANA_ALTAS_CUOTA_FRACCION' in base_dimensions.columns
        
        # Validate recovery objectives
        assert base_dimensions.loc[0, 'OBJ_RECUPERO'] == 0.15  # AL VCTO
        assert base_dimensions.loc[1, 'OBJ_RECUPERO'] == 0.25  # ENTRE 4 Y 15D
        
        # Validate fraccionamiento dimension
        assert 'FRACCIONADO' in base_dimensions.loc[1, 'TEMPRANA_ALTAS_CUOTA_FRACCION']
        assert 'FRACCIONADO' not in base_dimensions.loc[0, 'TEMPRANA_ALTAS_CUOTA_FRACCION']


def test_configuration_output_tables():
    """Test configuration output table naming"""
    config = ETLConfig(output_table_prefix="test_prefix")
    
    expected_tables = {
        'agregada': 'test_prefix_agregada',
        'comparativas': 'test_prefix_comparativas',
        'primera_vez': 'test_prefix_primera_vez',
        'base_cartera': 'test_prefix_base_cartera'
    }
    
    assert config.output_tables == expected_tables


def test_business_days_validation():
    """Test business days validation logic"""
    config = ETLConfig(country_code="PE")
    processor = BusinessDaysProcessor(config)
    
    validation_results = processor.validate_business_day_logic()
    
    # Should return results for test dates
    assert isinstance(validation_results, dict)
    assert len(validation_results) > 0
    
    # Check that each test date has required fields
    for date_str, result in validation_results.items():
        assert 'es_dia_habil' in result
        assert 'dia_habil_mes' in result
        assert 'dia_semana' in result
        assert 'es_feriado' in result


if __name__ == "__main__":
    # Run basic tests without pytest
    print("🧪 Ejecutando tests básicos de FACO ETL...")
    
    # Test 1: Configuration
    print("\n1. Testing configuration...")
    config = ETLConfig(
        project_id="test-project",
        mes_vigencia="2025-06", 
        estado_vigencia="abierto"
    )
    try:
        config.validate()
        print("   ✅ Configuration validation passed")
    except Exception as e:
        print(f"   ❌ Configuration validation failed: {e}")
    
    # Test 2: Business Days
    print("\n2. Testing business days...")
    try:
        processor = BusinessDaysProcessor(config)
        test_date = date(2025, 6, 19)
        business_day = processor.calculate_business_day_of_month(test_date)
        print(f"   ✅ Business day calculation: {test_date} is business day #{business_day}")
    except Exception as e:
        print(f"   ❌ Business days failed: {e}")
    
    # Test 3: Transformer Logic
    print("\n3. Testing transformer logic...")
    try:
        transformer = CobranzaTransformer(config, processor)
        
        # Test cartera extraction
        cartera = transformer._extract_cartera_type("Cartera_Agencia_Cobranding_Gestion_Temprana_20250617.txt")
        assert cartera == "TEMPRANA"
        
        # Test recovery objective
        obj = transformer._calculate_recovery_objective("AL VCTO")
        assert obj == 0.15
        
        print("   ✅ Transformer logic tests passed")
    except Exception as e:
        print(f"   ❌ Transformer logic failed: {e}")
    
    print("\n🎉 Tests básicos completados!")
    print("\nPara ejecutar tests completos con pytest:")
    print("   uv run pytest tests/test_basic.py -v")