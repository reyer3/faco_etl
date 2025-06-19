#!/bin/bash

# ==============================================================================
# FACO ETL - Script de Ejecución Rápida para Presentación
# ==============================================================================

echo "🚀 FACO ETL - Generando datos para presentación..."

# Configuración
PROJECT_ID="mibot-222814"
DATASET="BI_USA"
FECHA_INICIO="2025-05-01"  # Ajustar según tus datos
FECHA_FIN="2025-05-31"     # Ajustar según tus datos

# ==============================================================================
# PASO 1: Crear las tablas de destino (solo si no existen)
# ==============================================================================

echo "📋 Creando tablas de destino..."

# Tabla 1: Universo de Asignaciones
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
CREATE TABLE IF NOT EXISTS \`$PROJECT_ID.$DATASET.faco_dash_asignacion_universo\`
(
  FECHA_ASIGNACION DATE,
  CARTERA STRING,
  SERVICIO STRING,
  SEGMENTO_GESTION STRING,
  VENCIMIENTO DATE,
  ID_ARCHIVO_ASIGNACION STRING,
  ZONA_GEOGRAFICA STRING,
  TIPO_FRACCIONAMIENTO STRING,
  OBJ_RECUPERO FLOAT64,
  Q_CUENTAS_ASIGNADAS INT64,
  Q_CLIENTES_ASIGNADOS INT64,
  MONTO_EXIGIBLE_ASIGNADO FLOAT64,
  DIAS_GESTION_DISPONIBLES FLOAT64
)
PARTITION BY FECHA_ASIGNACION
CLUSTER BY CARTERA, SERVICIO, SEGMENTO_GESTION;
EOF

# Tabla 2: Gestión Agregada
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
CREATE TABLE IF NOT EXISTS \`$PROJECT_ID.$DATASET.faco_dash_gestion_agregada\`
(
  FECHA_SERVICIO DATE,
  CARTERA STRING,
  CANAL STRING,
  OPERADOR_FINAL STRING,
  GRUPO_RESPUESTA STRING,
  NIVEL_1 STRING,
  NIVEL_2 STRING,
  SERVICIO STRING,
  SEGMENTO_GESTION STRING,
  ZONA_GEOGRAFICA STRING,
  Q_INTERACCIONES_TOTAL INT64,
  Q_CONTACTOS_EFECTIVOS INT64,
  Q_PROMESAS_DE_PAGO INT64,
  MONTO_COMPROMETIDO FLOAT64,
  Q_CLIENTES_UNICOS_CONTACTADOS INT64,
  Q_CLIENTES_PRIMERA_VEZ_DIA INT64,
  Q_CLIENTES_CON_PROMESA INT64,
  EFECTIVIDAD_CONTACTO FLOAT64,
  TASA_COMPROMISO FLOAT64,
  MONTO_PROMEDIO_COMPROMISO FLOAT64
)
PARTITION BY FECHA_SERVICIO
CLUSTER BY CARTERA, CANAL, OPERADOR_FINAL;
EOF

# Tabla 3: Recupero Atribuido
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
CREATE TABLE IF NOT EXISTS \`$PROJECT_ID.$DATASET.faco_dash_recupero_atribuido\`
(
  FECHA_PAGO DATE,
  CARTERA STRING,
  SERVICIO STRING,
  ID_ARCHIVO_ASIGNACION STRING,
  CANAL_ATRIBUIDO STRING,
  OPERADOR_ATRIBUIDO STRING,
  FECHA_GESTION_ATRIBUIDA DATE,
  FECHA_COMPROMISO DATE,
  MONTO_PAGADO FLOAT64,
  ES_PAGO_CON_PDP BOOL,
  PDP_ESTABA_VIGENTE BOOL,
  PAGO_ES_PUNTUAL BOOL,
  DIAS_ENTRE_GESTION_Y_PAGO INT64,
  EFECTIVIDAD_ATRIBUCION FLOAT64
)
PARTITION BY FECHA_PAGO
CLUSTER BY CARTERA, CANAL_ATRIBUIDO, OPERADOR_ATRIBUIDO;
EOF

# Tabla 4: KPIs Ejecutivos
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
CREATE TABLE IF NOT EXISTS \`$PROJECT_ID.$DATASET.faco_dash_kpis_ejecutivos\`
(
  FECHA_CALCULO DATE,
  CARTERA STRING,
  SERVICIO STRING,
  CANAL STRING,
  UNIVERSO_CUENTAS INT64,
  UNIVERSO_MONTO FLOAT64,
  OBJ_RECUPERO_PROMEDIO FLOAT64,
  CUENTAS_CONTACTADAS INT64,
  TASA_CONTACTABILIDAD FLOAT64,
  EFECTIVIDAD_PROMEDIO FLOAT64,
  MONTO_COMPROMETIDO FLOAT64,
  TASA_COMPROMISO_PROMEDIO FLOAT64,
  MONTO_RECUPERADO FLOAT64,
  TASA_RECUPERACION FLOAT64,
  CUMPLIMIENTO_OBJETIVO FLOAT64
)
PARTITION BY FECHA_CALCULO
CLUSTER BY CARTERA, SERVICIO, CANAL;
EOF

echo "✅ Tablas creadas exitosamente"

# ==============================================================================
# PASO 2: Ejecutar el Stored Procedure
# ==============================================================================

echo "⚡ Ejecutando ETL para período $FECHA_INICIO a $FECHA_FIN..."

bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
CALL \`$PROJECT_ID.$DATASET.sp_faco_etl_para_looker_studio\`('$FECHA_INICIO', '$FECHA_FIN');
EOF

# ==============================================================================
# PASO 3: Verificar resultados
# ==============================================================================

echo "🔍 Verificando resultados..."

echo "📊 Tabla 1 - Universo de Asignaciones:"
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
SELECT 
  COUNT(*) as total_registros,
  MIN(FECHA_ASIGNACION) as fecha_min,
  MAX(FECHA_ASIGNACION) as fecha_max,
  COUNT(DISTINCT CARTERA) as carteras_unicas,
  SUM(Q_CUENTAS_ASIGNADAS) as total_cuentas,
  SUM(MONTO_EXIGIBLE_ASIGNADO) as total_monto
FROM \`$PROJECT_ID.$DATASET.faco_dash_asignacion_universo\`
WHERE FECHA_ASIGNACION BETWEEN '$FECHA_INICIO' AND '$FECHA_FIN';
EOF

echo "📞 Tabla 2 - Gestión Agregada:"
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
SELECT 
  COUNT(*) as total_registros,
  MIN(FECHA_SERVICIO) as fecha_min,
  MAX(FECHA_SERVICIO) as fecha_max,
  COUNT(DISTINCT CANAL) as canales_unicos,
  SUM(Q_INTERACCIONES_TOTAL) as total_interacciones,
  SUM(Q_CLIENTES_UNICOS_CONTACTADOS) as total_clientes_contactados
FROM \`$PROJECT_ID.$DATASET.faco_dash_gestion_agregada\`
WHERE FECHA_SERVICIO BETWEEN '$FECHA_INICIO' AND '$FECHA_FIN';
EOF

echo "💰 Tabla 3 - Recupero Atribuido:"
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
SELECT 
  COUNT(*) as total_registros,
  MIN(FECHA_PAGO) as fecha_min,
  MAX(FECHA_PAGO) as fecha_max,
  SUM(MONTO_PAGADO) as total_recuperado,
  COUNT(CASE WHEN ES_PAGO_CON_PDP THEN 1 END) as pagos_con_pdp,
  AVG(EFECTIVIDAD_ATRIBUCION) as efectividad_promedio
FROM \`$PROJECT_ID.$DATASET.faco_dash_recupero_atribuido\`
WHERE FECHA_PAGO BETWEEN '$FECHA_INICIO' AND '$FECHA_FIN';
EOF

echo "📈 Tabla 4 - KPIs Ejecutivos:"
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
SELECT 
  COUNT(*) as total_registros,
  COUNT(DISTINCT CARTERA) as carteras,
  COUNT(DISTINCT CANAL) as canales,
  AVG(TASA_CONTACTABILIDAD) as contactabilidad_promedio,
  AVG(TASA_RECUPERACION) as recuperacion_promedio,
  AVG(CUMPLIMIENTO_OBJETIVO) as cumplimiento_promedio
FROM \`$PROJECT_ID.$DATASET.faco_dash_kpis_ejecutivos\`
WHERE FECHA_CALCULO BETWEEN '$FECHA_INICIO' AND '$FECHA_FIN';
EOF

# ==============================================================================
# PASO 4: Generar queries de muestra para Looker Studio
# ==============================================================================

echo "📋 Generando queries de muestra para Looker Studio..."

cat > looker_studio_queries.sql <<EOF
-- ==============================================================================
-- QUERIES DE MUESTRA PARA LOOKER STUDIO - FACO ETL
-- ==============================================================================

-- Query 1: KPIs Principales por Cartera y Canal
SELECT 
  k.CARTERA,
  k.CANAL,
  k.UNIVERSO_CUENTAS,
  k.CUENTAS_CONTACTADAS,
  k.TASA_CONTACTABILIDAD,
  k.EFECTIVIDAD_PROMEDIO,
  k.MONTO_RECUPERADO,
  k.TASA_RECUPERACION,
  k.CUMPLIMIENTO_OBJETIVO
FROM \`$PROJECT_ID.$DATASET.faco_dash_kpis_ejecutivos\` k
WHERE k.FECHA_CALCULO BETWEEN '$FECHA_INICIO' AND '$FECHA_FIN'
ORDER BY k.CARTERA, k.CANAL;

-- Query 2: Evolución Diaria de Gestión
SELECT 
  g.FECHA_SERVICIO,
  g.CARTERA,
  g.CANAL,
  SUM(g.Q_INTERACCIONES_TOTAL) as TOTAL_INTERACCIONES,
  SUM(g.Q_CLIENTES_UNICOS_CONTACTADOS) as CLIENTES_CONTACTADOS,
  AVG(g.EFECTIVIDAD_CONTACTO) as EFECTIVIDAD_PROMEDIO,
  SUM(g.MONTO_COMPROMETIDO) as MONTO_COMPROMETIDO
FROM \`$PROJECT_ID.$DATASET.faco_dash_gestion_agregada\` g
WHERE g.FECHA_SERVICIO BETWEEN '$FECHA_INICIO' AND '$FECHA_FIN'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Query 3: Atribución de Recupero
SELECT 
  r.CARTERA,
  r.CANAL_ATRIBUIDO,
  COUNT(*) as CANTIDAD_PAGOS,
  SUM(r.MONTO_PAGADO) as MONTO_TOTAL_RECUPERADO,
  COUNT(CASE WHEN r.ES_PAGO_CON_PDP THEN 1 END) as PAGOS_CON_PDP,
  COUNT(CASE WHEN r.PDP_ESTABA_VIGENTE THEN 1 END) as PAGOS_PDP_VIGENTE,
  AVG(r.EFECTIVIDAD_ATRIBUCION) as EFECTIVIDAD_ATRIBUCION_PROMEDIO
FROM \`$PROJECT_ID.$DATASET.faco_dash_recupero_atribuido\` r
WHERE r.FECHA_PAGO BETWEEN '$FECHA_INICIO' AND '$FECHA_FIN'
GROUP BY 1, 2
ORDER BY 3 DESC;

-- Query 4: Top Operadores por Efectividad
SELECT 
  g.OPERADOR_FINAL,
  g.CANAL,
  SUM(g.Q_INTERACCIONES_TOTAL) as TOTAL_INTERACCIONES,
  SUM(g.Q_CONTACTOS_EFECTIVOS) as CONTACTOS_EFECTIVOS,
  AVG(g.EFECTIVIDAD_CONTACTO) as EFECTIVIDAD_PROMEDIO,
  SUM(g.Q_PROMESAS_DE_PAGO) as PROMESAS_TOTALES,
  SUM(g.MONTO_COMPROMETIDO) as MONTO_COMPROMETIDO
FROM \`$PROJECT_ID.$DATASET.faco_dash_gestion_agregada\` g
WHERE g.FECHA_SERVICIO BETWEEN '$FECHA_INICIO' AND '$FECHA_FIN'
  AND g.OPERADOR_FINAL != 'SISTEMA_BOT'
GROUP BY 1, 2
HAVING SUM(g.Q_INTERACCIONES_TOTAL) >= 100  -- Filtrar operadores con actividad significativa
ORDER BY AVG(g.EFECTIVIDAD_CONTACTO) DESC
LIMIT 20;

EOF

echo "✅ Queries de muestra creadas en: looker_studio_queries.sql"

# ==============================================================================
# RESUMEN FINAL
# ==============================================================================

echo ""
echo "🎉 ¡FACO ETL ejecutado exitosamente!"
echo ""
echo "📊 Tablas generadas para Looker Studio:"
echo "   1. faco_dash_asignacion_universo    - Universo de cuentas asignadas"
echo "   2. faco_dash_gestion_agregada       - Métricas de gestión diaria"
echo "   3. faco_dash_recupero_atribuido     - Atribución de pagos a gestiones"
echo "   4. faco_dash_kpis_ejecutivos        - KPIs consolidados ejecutivos"
echo ""
echo "🔗 Para conectar Looker Studio:"
echo "   - Usa BigQuery como fuente de datos"
echo "   - Proyecto: $PROJECT_ID"
echo "   - Dataset: $DATASET"
echo "   - Tablas: faco_dash_*"
echo ""
echo "⚡ Próximos pasos:"
echo "   1. Conecta Looker Studio a las tablas faco_dash_*"
echo "   2. Usa las queries de looker_studio_queries.sql como base"
echo "   3. Crea filtros por CARTERA, CANAL, FECHA_*"
echo "   4. ¡Presenta tus KPIs!"
echo ""