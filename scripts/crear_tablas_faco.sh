#!/bin/bash

# =============================================================================
# Script para crear las tablas de salida FACO ETL
# Resuelve el error: "Table faco_dash_asignacion_universo was not found"
# =============================================================================

echo "🚀 Creando tablas de salida para FACO ETL..."
echo "📍 Proyecto: mibot-222814"
echo "📍 Dataset: BI_USA"
echo ""

# Verificar que bq CLI está instalado
if ! command -v bq &> /dev/null; then
    echo "❌ Error: Google Cloud CLI (bq) no está instalado"
    echo "   Instalar desde: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Verificar autenticación
echo "🔐 Verificando autenticación..."
if ! bq ls mibot-222814:BI_USA > /dev/null 2>&1; then
    echo "❌ Error: No tienes permisos en el proyecto mibot-222814"
    echo "   Ejecuta: gcloud auth login"
    echo "   Y verifica permisos BigQuery"
    exit 1
fi

echo "✅ Autenticación verificada"
echo ""

# Crear las 4 tablas esenciales
echo "📋 Creando tablas de salida..."

# Tabla 1: Universo de Asignación
echo "1️⃣  Creando faco_dash_asignacion_universo..."
bq mk \
  --table \
  --schema \
  FECHA_ASIGNACION:DATE,CARTERA:STRING,SERVICIO:STRING,SEGMENTO_GESTION:STRING,VENCIMIENTO:DATE,ID_ARCHIVO_ASIGNACION:STRING,ZONA_GEOGRAFICA:STRING,TIPO_FRACCIONAMIENTO:STRING,OBJ_RECUPERO:FLOAT64,Q_CUENTAS_ASIGNADAS:INT64,Q_CLIENTES_ASIGNADOS:INT64,MONTO_EXIGIBLE_ASIGNADO:FLOAT64,DIAS_GESTION_DISPONIBLES:FLOAT64 \
  --time_partitioning_field FECHA_ASIGNACION \
  --clustering_fields CARTERA,SERVICIO,SEGMENTO_GESTION \
  mibot-222814:BI_USA.faco_dash_asignacion_universo

# Tabla 2: Gestión Agregada
echo "2️⃣  Creando faco_dash_gestion_agregada..."
bq mk \
  --table \
  --schema \
  FECHA_SERVICIO:DATE,CARTERA:STRING,CANAL:STRING,OPERADOR_FINAL:STRING,GRUPO_RESPUESTA:STRING,NIVEL_1:STRING,NIVEL_2:STRING,SERVICIO:STRING,SEGMENTO_GESTION:STRING,ZONA_GEOGRAFICA:STRING,Q_INTERACCIONES_TOTAL:INT64,Q_CONTACTOS_EFECTIVOS:INT64,Q_PROMESAS_DE_PAGO:INT64,MONTO_COMPROMETIDO:FLOAT64,Q_CLIENTES_UNICOS_CONTACTADOS:INT64,Q_CLIENTES_PRIMERA_VEZ_DIA:INT64,Q_CLIENTES_CON_PROMESA:INT64,EFECTIVIDAD_CONTACTO:FLOAT64,TASA_COMPROMISO:FLOAT64,MONTO_PROMEDIO_COMPROMISO:FLOAT64 \
  --time_partitioning_field FECHA_SERVICIO \
  --clustering_fields CARTERA,CANAL,OPERADOR_FINAL \
  mibot-222814:BI_USA.faco_dash_gestion_agregada

# Tabla 3: Recupero Atribuido
echo "3️⃣  Creando faco_dash_recupero_atribuido..."
bq mk \
  --table \
  --schema \
  FECHA_PAGO:DATE,CARTERA:STRING,SERVICIO:STRING,ID_ARCHIVO_ASIGNACION:STRING,CANAL_ATRIBUIDO:STRING,OPERADOR_ATRIBUIDO:STRING,FECHA_GESTION_ATRIBUIDA:DATE,FECHA_COMPROMISO:DATE,MONTO_PAGADO:FLOAT64,ES_PAGO_CON_PDP:BOOLEAN,PDP_ESTABA_VIGENTE:BOOLEAN,PAGO_ES_PUNTUAL:BOOLEAN,DIAS_ENTRE_GESTION_Y_PAGO:INT64,EFECTIVIDAD_ATRIBUCION:FLOAT64 \
  --time_partitioning_field FECHA_PAGO \
  --clustering_fields CARTERA,CANAL_ATRIBUIDO,OPERADOR_ATRIBUIDO \
  mibot-222814:BI_USA.faco_dash_recupero_atribuido

# Tabla 4: KPIs Ejecutivos
echo "4️⃣  Creando faco_dash_kpis_ejecutivos..."
bq mk \
  --table \
  --schema \
  FECHA_CALCULO:DATE,CARTERA:STRING,SERVICIO:STRING,CANAL:STRING,UNIVERSO_CUENTAS:INT64,UNIVERSO_MONTO:FLOAT64,OBJ_RECUPERO_PROMEDIO:FLOAT64,CUENTAS_CONTACTADAS:INT64,TASA_CONTACTABILIDAD:FLOAT64,EFECTIVIDAD_PROMEDIO:FLOAT64,MONTO_COMPROMETIDO:FLOAT64,TASA_COMPROMISO_PROMEDIO:FLOAT64,MONTO_RECUPERADO:FLOAT64,TASA_RECUPERACION:FLOAT64,CUMPLIMIENTO_OBJETIVO:FLOAT64 \
  --time_partitioning_field FECHA_CALCULO \
  --clustering_fields CARTERA,SERVICIO,CANAL \
  mibot-222814:BI_USA.faco_dash_kpis_ejecutivos

echo ""
echo "🎉 ¡Tablas creadas exitosamente!"
echo ""

# Verificar que se crearon
echo "🔍 Verificando tablas creadas..."
echo ""

TABLES=$(bq ls --format=csv mibot-222814:BI_USA | grep faco_dash | wc -l)

if [ "$TABLES" -eq 4 ]; then
    echo "✅ Las 4 tablas se crearon correctamente:"
    bq ls --format=table mibot-222814:BI_USA | grep faco_dash
    echo ""
    echo "🚀 Ahora puedes ejecutar tu stored procedure:"
    echo "   CALL \`mibot-222814.BI_USA.sp_faco_etl_para_looker_studio\`('2025-06-01', '2025-06-18');"
    echo ""
    echo "📊 Las tablas están optimizadas para Looker Studio con:"
    echo "   • Partitioning por fecha"
    echo "   • Clustering por dimensiones principales"
    echo "   • Tipos de datos optimizados"
else
    echo "⚠️  Solo se crearon $TABLES de 4 tablas esperadas"
    echo "   Revisa los errores arriba y reintenta"
    exit 1
fi

echo ""
echo "✨ ¡Listo para usar!"