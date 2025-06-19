"""
Quick validation test for FACO ETL setup
"""

def test_imports():
    """Test that all imports work correctly"""
    try:
        # Core imports
        from core.config import get_config, ETLConfig
        from core.logger import setup_logging
        from core.orchestrator import ETLOrchestrator, ETLResult
        
        print("✅ All core imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False


def test_config():
    """Test configuration with default values"""
    try:
        config = get_config()
        
        # Check required attributes
        assert hasattr(config, 'project_id')
        assert hasattr(config, 'dataset_id')
        assert hasattr(config, 'mes_vigencia')
        assert hasattr(config, 'estado_vigencia')
        
        # Check default values
        assert config.project_id == "mibot-222814"
        assert config.dataset_id == "BI_USA"
        assert config.mes_vigencia == "2025-06"
        assert config.estado_vigencia == "abierto"
        
        print("✅ Configuration validation successful")
        return True
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False


def test_orchestrator():
    """Test orchestrator initialization"""
    try:
        from core.config import get_config
        from core.orchestrator import ETLOrchestrator
        
        config = get_config()
        orchestrator = ETLOrchestrator(config)
        
        # Test that orchestrator has required methods
        assert hasattr(orchestrator, 'run')
        assert callable(orchestrator.run)
        
        print("✅ Orchestrator initialization successful")
        return True
    except Exception as e:
        print(f"❌ Orchestrator error: {e}")
        return False


def test_dry_run():
    """Test a dry run of the ETL process"""
    try:
        import sys
        from pathlib import Path
        
        # Add src to path
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        
        from core.config import get_config
        from core.orchestrator import ETLOrchestrator
        from core.logger import setup_logging
        
        # Setup minimal logging
        setup_logging("INFO")
        
        # Create config with dry run
        config = get_config()
        config.dry_run = True
        
        # Run orchestrator
        orchestrator = ETLOrchestrator(config)
        result = orchestrator.run()
        
        # Check result
        assert result.success == True
        assert result.records_processed > 0
        assert len(result.output_tables) > 0
        
        print("✅ Dry run test successful")
        print(f"   📊 Records processed: {result.records_processed:,}")
        print(f"   ⏱️  Execution time: {result.execution_time}")
        print(f"   📋 Output tables: {', '.join(result.output_tables)}")
        return True
    except Exception as e:
        print(f"❌ Dry run error: {e}")
        return False


def main():
    """Run all validation tests"""
    print("🧪 FACO ETL Validation Tests")
    print("=" * 40)
    
    tests = [
        ("Import Test", test_imports),
        ("Config Test", test_config), 
        ("Orchestrator Test", test_orchestrator),
        ("Dry Run Test", test_dry_run)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🔍 Running {test_name}...")
        if test_func():
            passed += 1
        else:
            print(f"   ❌ {test_name} failed")
    
    print("\n" + "=" * 40)
    print(f"📊 Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 All tests passed! Repository is ready to use.")
        return True
    else:
        print("⚠️  Some tests failed. Check the errors above.")
        return False


if __name__ == "__main__":
    main()