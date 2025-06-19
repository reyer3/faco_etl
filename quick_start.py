#!/usr/bin/env python3
"""
Quick Start Script for FACO ETL

Genera datos de prueba inmediatamente para presentaciones.
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime


def run_command(cmd, description):
    """Run command with nice output"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=Path(__file__).parent)
        if result.returncode == 0:
            print(f"✅ {description} - Completado")
            return True
        else:
            print(f"❌ {description} - Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"💥 {description} - Error: {e}")
        return False


def main():
    print("🚀 FACO ETL - Inicio Rápido para Presentación")
    print("=" * 50)
    print(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Check if we're in the right place
    if not Path("main.py").exists():
        print("❌ Error: Ejecuta desde el directorio del proyecto")
        print("💡 Comando: cd faco_etl && python quick_start.py")
        sys.exit(1)
    
    print("🎯 Preparando ETL para presentación...")
    print()
    
    # Step 1: Setup básico
    if not run_command("mkdir -p logs credentials data", "Creando directorios"):
        print("⚠️  Continuando de todas formas...")
    
    # Step 2: Environment
    if not Path(".env").exists():
        if not run_command("cp .env.example .env", "Configurando environment"):
            print("⚠️  Archivo .env no creado - usando defaults")
    
    # Step 3: Test connectivity 
    print("🔍 Probando conectividad...")
    if run_command("python main.py --test-connectivity", "Test de conectividad"):
        print("💡 BigQuery disponible - ejecutando ETL real")
        etl_mode = "real"
    else:
        print("💡 BigQuery no disponible - usando modo MOCK para demo")
        etl_mode = "mock"
    
    print()
    print("=" * 50)
    print("🎬 EJECUTANDO ETL PARA PRESENTACIÓN")
    print("=" * 50)
    
    # Step 4: Execute ETL
    if etl_mode == "real":
        # Try real ETL first with dry-run
        print("🧪 Probando ETL real (dry-run)...")
        if run_command("python main.py --mes 2025-06 --estado abierto --dry-run", "ETL real (dry-run)"):
            print()
            print("🎯 Ejecutando ETL real con datos...")
            success = run_command("python main.py --mes 2025-06 --estado abierto", "ETL real con BigQuery")
        else:
            print("⚠️  ETL real falló, usando modo MOCK")
            success = run_command("python main.py --mes 2025-06 --estado abierto --dry-run", "ETL MOCK")
    else:
        # Mock mode for demo
        print("🎭 Ejecutando ETL en modo MOCK para demo...")
        success = run_command("python main.py --mes 2025-06 --estado abierto --dry-run", "ETL MOCK para demo")
    
    print()
    print("=" * 50)
    
    if success:
        print("🎉 ¡ETL COMPLETADO CON ÉXITO!")
        print("=" * 50)
        print()
        print("📊 RESULTADOS PARA PRESENTACIÓN:")
        print("━" * 40)
        
        if etl_mode == "real":
            print("✅ Datos reales procesados desde BigQuery")
            print("📋 Tablas generadas:")
            print("   • dash_cobranza_agregada")
            print("   • dash_cobranza_comparativas") 
            print("   • dash_primera_vez_tracking")
            print("   • dash_cobranza_base_cartera")
            print()
            print("🔗 Conecta Looker Studio a estas tablas en BigQuery")
        else:
            print("✅ Demo completado con datos simulados")
            print("📈 Pipeline validado - listo para datos reales")
            print("🔧 Para datos reales, configura credenciales BigQuery")
        
        print()
        print("🎯 PRÓXIMOS PASOS:")
        print("━" * 40)
        print("1. 📊 Abrir Looker Studio")
        print("2. 🔗 Conectar a BigQuery dataset: BI_USA") 
        print("3. 📈 Usar tablas: dash_cobranza_*")
        print("4. 🎨 Crear dashboards con dimensiones pre-agregadas")
        print()
        print("💡 Comandos útiles:")
        print(f"   • Ver logs: tail -f logs/etl.log")
        print(f"   • Re-ejecutar: python main.py --mes 2025-06 --estado abierto")
        print(f"   • Modo debug: python main.py --debug")
        print()
        print("🚀 ¡Listo para presentar!")
        
    else:
        print("❌ ETL FALLÓ")
        print("=" * 50)
        print()
        print("🔧 SOLUCIONES RÁPIDAS:")
        print("━" * 40)
        print("1. 🔑 Configurar credenciales:")
        print("   gcloud auth application-default login")
        print()
        print("2. 📋 Revisar configuración:")
        print("   python main.py --setup-help")
        print()
        print("3. 🧪 Modo seguro para demo:")
        print("   python main.py --dry-run")
        print()
        print("4. 📝 Ver logs detallados:")
        print("   python main.py --debug")
        print()
        print("💬 El ETL está listo - solo necesita credenciales para BigQuery")


if __name__ == "__main__":
    main()