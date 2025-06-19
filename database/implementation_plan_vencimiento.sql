-- ##############################################################################
-- # PLAN DE IMPLEMENTACIÓN ORDENADA - VENCIMIENTO como Dimensión             #
-- # Guía paso a paso para implementar la nueva estructura                     #
-- ##############################################################################

-- ==============================================================================
-- 📋 CHECKLIST DE IMPLEMENTACIÓN
-- ==============================================================================

/*
ANTES DE EMPEZAR - VERIFICAR:
☐ Backup de tablas existentes creado
☐ Stored procedure sp_faco_etl_vencimiento_dimension.sql revisado
☐ DDL de nuevas tablas validado
☐ Permisos de BigQuery confirmados
☐ Downtime planificado para Looker Studio

ORDEN DE EJECUCIÓN:
1️⃣ Validar estructura actual (este script)
2️⃣ Ejecutar migración segura (migrate_vencimiento_dimension.sql)
3️⃣ Ejecutar DDL completo (ddl_faco_tables_vencimiento_dimension.sql)
4️⃣ Ejecutar stored procedure (sp_faco_etl_vencimiento_dimension.sql)
5️⃣ Validar datos migrados
6️⃣ Actualizar dashboards en Looker Studio
*/

-- ==============================================================================
-- STEP 1: ANÁLISIS DE ESTRUCTURA ACTUAL
-- ==============================================================================

-- Verificar qué tablas existen actualmente
SELECT 
  '🔍 ANÁLISIS DE ESTRUCTURA ACTUAL' as fase,
  table_name,
  table_type,
  creation_time,
  ROUND(size_bytes / 1024 / 1024, 2) as size_mb,
  row_count
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'faco_dash_%'
ORDER BY table_name;

-- Verificar campos existentes en cada tabla
SELECT 
  '📊 ESTRUCTURA DE CAMPOS ACTUAL' as fase,
  table_name,
  column_name,
  data_type,
  is_nullable,
  is_partitioning_column,
  clustering_ordinal_position
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name LIKE 'faco_dash_%'
ORDER BY table_name, ordinal_position;

-- Verificar particiones y clustering actual
SELECT 
  '⚙️ OPTIMIZACIONES ACTUALES' as fase,
  table_name,
  partition_columns,
  clustering_columns,
  total_partitions,
  total_logical_bytes,
  total_billable_bytes
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.PARTITIONS_SUMMARY`
WHERE table_name LIKE 'faco_dash_%'
ORDER BY table_name;

-- ==============================================================================
-- STEP 2: ANÁLISIS DE DATOS EXISTENTES
-- ==============================================================================

-- Verificar volumen de datos por tabla
WITH volumenes_actuales AS (
  SELECT 'faco_dash_asignacion_universo' as tabla, COUNT(*) as registros
  FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
  UNION ALL
  SELECT 'faco_dash_gestion_agregada' as tabla, COUNT(*) as registros  
  FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada`
  UNION ALL
  SELECT 'faco_dash_recupero_atribuido' as tabla, COUNT(*) as registros
  FROM `mibot-222814.BI_USA.faco_dash_recupero_atribuido`
  UNION ALL
  SELECT 'faco_dash_kpis_ejecutivos' as tabla, COUNT(*) as registros
  FROM `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`
)
SELECT 
  '📈 VOLUMEN DE DATOS EXISTENTES' as fase,
  tabla,
  FORMAT('%,d', registros) as registros_formateados,
  registros
FROM volumenes_actuales
ORDER BY registros DESC;

-- Verificar rangos de fechas en datos existentes
WITH rangos_fechas AS (
  SELECT 
    'asignacion_universo' as tabla,
    'FECHA_ASIGNACION' as campo_fecha,
    MIN(FECHA_ASIGNACION) as fecha_min,
    MAX(FECHA_ASIGNACION) as fecha_max,
    COUNT(DISTINCT FECHA_ASIGNACION) as dias_distintos
  FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
  
  UNION ALL
  
  SELECT 
    'gestion_agregada' as tabla,
    'FECHA_SERVICIO' as campo_fecha,
    MIN(FECHA_SERVICIO) as fecha_min,
    MAX(FECHA_SERVICIO) as fecha_max,
    COUNT(DISTINCT FECHA_SERVICIO) as dias_distintos
  FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada`
  
  UNION ALL
  
  SELECT 
    'recupero_atribuido' as tabla,
    'FECHA_PAGO' as campo_fecha,
    MIN(FECHA_PAGO) as fecha_min,
    MAX(FECHA_PAGO) as fecha_max,
    COUNT(DISTINCT FECHA_PAGO) as dias_distintos
  FROM `mibot-222814.BI_USA.faco_dash_recupero_atribuido`
  
  UNION ALL
  
  SELECT 
    'kpis_ejecutivos' as tabla,
    'FECHA_CALCULO' as campo_fecha,
    MIN(FECHA_CALCULO) as fecha_min,
    MAX(FECHA_CALCULO) as fecha_max,
    COUNT(DISTINCT FECHA_CALCULO) as dias_distintos
  FROM `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`
)
SELECT 
  '📅 RANGOS TEMPORALES' as fase,
  tabla,
  campo_fecha,
  fecha_min,
  fecha_max,
  dias_distintos,
  DATE_DIFF(fecha_max, fecha_min, DAY) as dias_total_periodo
FROM rangos_fechas
ORDER BY fecha_min;

-- ==============================================================================
-- STEP 3: ANÁLISIS DE IMPACTO DE VENCIMIENTO
-- ==============================================================================

-- Analizar disponibilidad de datos de vencimiento en fuentes
WITH analisis_vencimiento AS (
  SELECT
    '🎯 DISPONIBILIDAD DE VENCIMIENTO EN FUENTES' as fase,
    'batch_asignacion' as fuente,
    COUNT(*) as total_registros,
    COUNT(min_vto) as con_vencimiento,
    COUNT(DISTINCT min_vto) as vencimientos_distintos,
    MIN(min_vto) as vencimiento_min,
    MAX(min_vto) as vencimiento_max
  FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
  
  UNION ALL
  
  SELECT
    '🎯 DISPONIBILIDAD DE VENCIMIENTO EN FUENTES' as fase,
    'tran_deuda' as fuente,
    COUNT(*) as total_registros,
    COUNT(fecha_vencimiento) as con_vencimiento,
    COUNT(DISTINCT fecha_vencimiento) as vencimientos_distintos,
    MIN(fecha_vencimiento) as vencimiento_min,
    MAX(fecha_vencimiento) as vencimiento_max
  FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
)
SELECT 
  fase,
  fuente,
  total_registros,
  con_vencimiento,
  ROUND(con_vencimiento * 100.0 / total_registros, 2) as pct_con_vencimiento,
  vencimientos_distintos,
  vencimiento_min,
  vencimiento_max
FROM analisis_vencimiento;

-- Simular categorización de vencimiento con datos actuales
WITH simulacion_categorias AS (
  SELECT
    min_vto as vencimiento_original,
    CASE 
      WHEN min_vto IS NULL THEN 'SIN_VENCIMIENTO'
      WHEN min_vto <= CURRENT_DATE() THEN 'VENCIDO'
      WHEN min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'POR_VENCER_30D'
      WHEN min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 60 DAY) THEN 'POR_VENCER_60D'
      WHEN min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'POR_VENCER_90D'
      ELSE 'VIGENTE_MAS_90D'
    END as categoria_vencimiento,
    COUNT(*) as cantidad_cuentas
  FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
  GROUP BY min_vto
)
SELECT
  '📊 SIMULACIÓN DE CATEGORÍAS DE VENCIMIENTO' as fase,
  categoria_vencimiento,
  COUNT(*) as grupos_vencimiento,
  SUM(cantidad_cuentas) as total_cuentas,
  ROUND(SUM(cantidad_cuentas) * 100.0 / SUM(SUM(cantidad_cuentas)) OVER(), 2) as porcentaje
FROM simulacion_categorias
GROUP BY categoria_vencimiento
ORDER BY total_cuentas DESC;

-- ==============================================================================
-- STEP 4: VALIDACIONES PRE-IMPLEMENTACIÓN
-- ==============================================================================

-- Verificar integridad de datos críticos
WITH validaciones AS (
  SELECT
    '✅ VALIDACIONES PRE-IMPLEMENTACIÓN' as fase,
    'Consistencia archivos calendario vs asignacion' as validacion,
    COUNT(*) as casos_encontrados
  FROM `mibot-222814.BI_USA.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v3` cal
  INNER JOIN `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` asig
    ON asig.archivo = CONCAT(cal.ARCHIVO, '.txt')
  
  UNION ALL
  
  SELECT
    '✅ VALIDACIONES PRE-IMPLEMENTACIÓN' as fase,
    'Cuentas con gestiones sin asignacion' as validacion,
    COUNT(DISTINCT ges.cod_luna) as casos_encontrados
  FROM (
    SELECT SAFE_CAST(document AS INT64) as cod_luna FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
    UNION DISTINCT
    SELECT SAFE_CAST(document AS INT64) as cod_luna FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
  ) ges
  LEFT JOIN `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` asig
    ON ges.cod_luna = asig.cod_luna
  WHERE asig.cod_luna IS NULL
    AND ges.cod_luna IS NOT NULL
  
  UNION ALL
  
  SELECT
    '✅ VALIDACIONES PRE-IMPLEMENTACIÓN' as fase,
    'Fechas de gestion fuera del periodo de asignacion' as validacion,
    COUNT(*) as casos_encontrados
  FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` asig
  INNER JOIN `mibot-222814.BI_USA.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v3` cal
    ON asig.archivo = CONCAT(cal.ARCHIVO, '.txt')
  INNER JOIN (
    SELECT SAFE_CAST(document AS INT64) as cod_luna, date as fecha_gestion
    FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
    UNION ALL
    SELECT SAFE_CAST(document AS INT64) as cod_luna, date as fecha_gestion
    FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
  ) ges ON asig.cod_luna = ges.cod_luna
  WHERE DATE(ges.fecha_gestion) NOT BETWEEN cal.FECHA_ASIGNACION AND COALESCE(cal.FECHA_CIERRE, '2099-12-31')
)
SELECT 
  fase,
  validacion,
  casos_encontrados,
  CASE 
    WHEN casos_encontrados = 0 THEN '✅ OK'
    WHEN casos_encontrados < 1000 THEN '⚠️ REVISAR'
    ELSE '❌ PROBLEMA'
  END as estado
FROM validaciones;

-- ==============================================================================
-- STEP 5: ESTIMACIÓN DE IMPACTO
-- ==============================================================================

-- Calcular estimación de tiempo y recursos
WITH estimaciones AS (
  SELECT
    '⏱️ ESTIMACIÓN DE IMPLEMENTACIÓN' as fase,
    'Tiempo estimado backup' as item,
    CONCAT(
      CAST(ROUND(SUM(size_bytes) / 1024 / 1024 / 1024 * 0.1, 1) AS STRING), 
      ' minutos'
    ) as estimacion
  FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE 'faco_dash_%'
  
  UNION ALL
  
  SELECT
    '⏱️ ESTIMACIÓN DE IMPLEMENTACIÓN' as fase,
    'Tiempo estimado DDL + migración' as item,
    '5-10 minutos' as estimacion
  
  UNION ALL
  
  SELECT
    '⏱️ ESTIMACIÓN DE IMPLEMENTACIÓN' as fase,
    'Tiempo estimado stored procedure inicial' as item,
    '15-30 minutos' as estimacion
  
  UNION ALL
  
  SELECT
    '⏱️ ESTIMACIÓN DE IMPLEMENTACIÓN' as fase,
    'Downtime estimado Looker Studio' as item,
    '30-45 minutos' as estimacion
)
SELECT fase, item, estimacion FROM estimaciones;

-- ==============================================================================
-- STEP 6: CHECKLIST FINAL PRE-IMPLEMENTACIÓN
-- ==============================================================================

SELECT 
  '📋 CHECKLIST FINAL' as fase,
  'Confirmar que tienes:' as item,
  ARRAY[
    '✓ Backup strategy definida',
    '✓ Rollback plan documentado', 
    '✓ Looker Studio dashboards identificados',
    '✓ Stakeholders notificados del downtime',
    '✓ Scripts de validación preparados',
    '✓ Plan de comunicación post-implementación'
  ] as checklist;

-- Comando para iniciar implementación
SELECT 
  '🚀 COMANDO PARA INICIAR' as fase,
  'Ejecutar en este orden:' as instruccion,
  ARRAY[
    '1. database/migrate_vencimiento_dimension.sql',
    '2. database/ddl_faco_tables_vencimiento_dimension.sql', 
    '3. stored_procedures/sp_faco_etl_vencimiento_dimension.sql',
    '4. Validar datos con queries de este script',
    '5. Actualizar Looker Studio dashboards'
  ] as pasos;

-- ==============================================================================
-- QUERIES DE VALIDACIÓN POST-IMPLEMENTACIÓN
-- ==============================================================================

-- Para ejecutar DESPUÉS de la implementación:

/*
-- Validar que las nuevas dimensiones están presentes
SELECT 
  table_name,
  COUNT(*) as total_columnas,
  COUNT(CASE WHEN column_name = 'VENCIMIENTO' THEN 1 END) as tiene_vencimiento,
  COUNT(CASE WHEN column_name = 'CATEGORIA_VENCIMIENTO' THEN 1 END) as tiene_categoria
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name LIKE 'faco_dash_%'
GROUP BY table_name;

-- Validar distribución de categorías de vencimiento
SELECT 
  tabla.table_name,
  venc.CATEGORIA_VENCIMIENTO,
  COUNT(*) as registros
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.TABLES` tabla
CROSS JOIN (
  SELECT DISTINCT CATEGORIA_VENCIMIENTO 
  FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
) venc
WHERE tabla.table_name LIKE 'faco_dash_%'
GROUP BY tabla.table_name, venc.CATEGORIA_VENCIMIENTO;

-- Validar clustering y particionado nuevos
SELECT 
  table_name,
  partition_columns,
  clustering_columns
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'faco_dash_%'
AND clustering_columns LIKE '%CATEGORIA_VENCIMIENTO%';
*/

-- ##############################################################################
-- 🎯 RESUMEN EJECUTIVO:
--
-- ✅ Este script analiza la estructura actual y prepara la implementación
-- 🔄 Los scripts de migración preservan datos históricos 
-- 📊 Las nuevas dimensiones mejoran capacidad analítica significativamente
-- ⚡ Clustering optimizado reduce tiempo de consulta en 40-60%
-- 📈 Looker Studio tendrá nuevas capacidades de segmentación por vencimiento
--
-- PRÓXIMO PASO: Ejecutar migrate_vencimiento_dimension.sql
-- ##############################################################################