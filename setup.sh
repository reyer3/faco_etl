#!/bin/bash
# Quick setup script for FACO ETL

set -e

echo "🚀 FACO ETL - Configuración Rápida"
echo "=================================="

# Check if we're in the right directory
if [ ! -f "main.py" ]; then
    echo "❌ Error: Ejecuta este script desde el directorio raíz del proyecto"
    exit 1
fi

# Create necessary directories
echo "📁 Creando directorios..."
mkdir -p credentials
mkdir -p logs
mkdir -p data

# Check for existing .env
if [ ! -f ".env" ]; then
    echo "⚙️  Creando archivo .env..."
    cp .env.example .env
    echo "✅ Archivo .env creado. Edítalo con tus configuraciones."
else
    echo "✅ Archivo .env ya existe."
fi

# Check for credentials
if [ ! -f "credentials/key.json" ]; then
    echo ""
    echo "🔑 CONFIGURACIÓN DE CREDENCIALES"
    echo "================================"
    echo "Opciones para autenticación con Google Cloud:"
    echo ""
    echo "OPCIÓN 1 - Autenticación por línea de comandos (Recomendado):"
    echo "   gcloud auth application-default login"
    echo ""
    echo "OPCIÓN 2 - Service Account Key:"
    echo "   1. Descarga tu service account key desde Google Cloud Console"
    echo "   2. Guárdala como: credentials/key.json"
    echo "   3. export GOOGLE_APPLICATION_CREDENTIALS=\$(pwd)/credentials/key.json"
    echo ""
    echo "OPCIÓN 3 - Solo testing sin BigQuery:"
    echo "   python main.py --dry-run"
    echo ""
else
    echo "✅ Credenciales encontradas en credentials/key.json"
fi

# Check if uv is available
if command -v uv &> /dev/null; then
    echo ""
    echo "📦 UV detectado. Instalando dependencias..."
    uv sync
    echo "✅ Dependencias instaladas con UV"
    
    echo ""
    echo "🧪 Probando configuración..."
    uv run python main.py --test-connectivity
    
elif command -v pip &> /dev/null; then
    echo ""
    echo "📦 Instalando dependencias con pip..."
    pip install -r requirements.txt
    echo "✅ Dependencias instaladas con pip"
    
    echo ""
    echo "🧪 Probando configuración..."
    python main.py --test-connectivity
    
else
    echo ""
    echo "⚠️  No se encontró uv ni pip. Instala las dependencias manualmente:"
    echo "   pip install -r requirements.txt"
fi

echo ""
echo "🎉 CONFIGURACIÓN COMPLETADA"
echo "=========================="
echo ""
echo "Comandos útiles:"
echo "  # Test de conectividad"
echo "  python main.py --test-connectivity"
echo ""
echo "  # Ayuda con credenciales"
echo "  python main.py --setup-help"
echo ""
echo "  # Ejecutar ETL en modo testing"
echo "  python main.py --mes 2025-06 --estado abierto --dry-run"
echo ""
echo "  # Ejecutar ETL real (requiere credenciales)"
echo "  python main.py --mes 2025-06 --estado abierto"
echo ""
echo "¡Listo para usar! 🚀"