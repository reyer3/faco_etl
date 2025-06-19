-- ##############################################################################
-- # DDL: DROP AND CREATE TABLES - FACO ETL con VENCIMIENTO como Dimensión     #
-- # Optimizado para Looker Studio con clustering y particionado               #
-- ##############################################################################

-- ==============================================================================
-- STEP 1: DROP EXISTING TABLES (IF EXISTS)
-- ==============================================================================

-- Drop tablas existentes para recrear con nueva estructura
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_asignacion_universo`;
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_gestion_agregada`;  
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_recupero_atribuido`;
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`;

-- ==============================================================================
-- STEP 2: CREATE OPTIMIZED TABLES WITH VENCIMIENTO DIMENSION
-- ==============================================================================

-- ##############################################################################
-- TABLA 1: UNIVERSO DE ASIGNACIONES
-- Granularidad: Una fila por combinación de dimensiones de asignación
-- Incluye: VENCIMIENTO y CATEGORIA_VENCIMIENTO como nuevas dimensiones
-- ##############################################################################

CREATE TABLE `mibot-222814.BI_USA.faco_dash_asignacion_universo`
(
  -- Dimensiones Temporales
  FECHA_ASIGNACION DATE NOT NULL OPTIONS(description="Fecha cuando se asignó la cartera a gestión"),
  
  -- Dimensiones de Segmentación  
  CARTERA STRING NOT NULL OPTIONS(description="Tipo de cartera: TEMPRANA, CUOTA_FIJA_ANUAL, ALTAS_NUEVAS, etc."),
  SERVICIO STRING NOT NULL OPTIONS(description="Tipo de servicio: FIJA, MOVIL, MT"),
  SEGMENTO_GESTION STRING NOT NULL OPTIONS(description="Segmento de gestión: AL VCTO, ENTRE 4 Y 15D"),
  
  -- 🔧 NUEVAS DIMENSIONES DE VENCIMIENTO
  VENCIMIENTO DATE NOT NULL OPTIONS(description="Fecha de vencimiento de la cuenta (min_vto)"),
  CATEGORIA_VENCIMIENTO STRING NOT NULL OPTIONS(description="Categoría: VENCIDO, POR_VENCER_30D, POR_VENCER_60D, POR_VENCER_90D, VIGENTE_MAS_90D, SIN_VENCIMIENTO"),
  
  -- Dimensiones Operativas
  ID_ARCHIVO_ASIGNACION STRING NOT NULL OPTIONS(description="Identificador del archivo de asignación"),
  ZONA_GEOGRAFICA STRING NOT NULL OPTIONS(description="Zona geográfica del cliente"),
  TIPO_FRACCIONAMIENTO STRING NOT NULL OPTIONS(description="FRACCIONADO o NORMAL"),
  
  -- Dimensiones de Objetivo
  OBJ_RECUPERO FLOAT64 NOT NULL OPTIONS(description="Objetivo de recuperación basado en reglas de negocio"),
  
  -- Métricas Agregadas
  Q_CUENTAS_ASIGNADAS INT64 NOT NULL OPTIONS(description="Cantidad de cuentas asignadas"),
  Q_CLIENTES_ASIGNADOS INT64 NOT NULL OPTIONS(description="Cantidad de clientes únicos asignados"),
  MONTO_EXIGIBLE_ASIGNADO FLOAT64 NOT NULL OPTIONS(description="Monto total exigible asignado"),
  DIAS_GESTION_DISPONIBLES FLOAT64 NOT NULL OPTIONS(description="Promedio de días de gestión disponibles")
)
PARTITION BY FECHA_ASIGNACION
CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, SERVICIO
OPTIONS(
  description="Universo de cuentas asignadas agregado por dimensiones con VENCIMIENTO. Optimizado para Looker Studio.",
  labels=[("ambiente", "produccion"), ("modulo", "faco_etl"), ("tabla_tipo", "fact")]
);

-- ##############################################################################
-- TABLA 2: GESTIÓN DIARIA AGREGADA  
-- Granularidad: Una fila por día de gestión + dimensiones + canal
-- Incluye: VENCIMIENTO para análisis segmentado por estado de vencimiento
-- ##############################################################################

CREATE TABLE `mibot-222814.BI_USA.faco_dash_gestion_agregada`
(
  -- Dimensiones Temporales
  FECHA_SERVICIO DATE NOT NULL OPTIONS(description="Fecha del día de gestión"),
  
  -- Dimensiones de Segmentación
  CARTERA STRING NOT NULL OPTIONS(description="Tipo de cartera"),
  CANAL STRING NOT NULL OPTIONS(description="Canal de gestión: BOT o HUMANO"),
  OPERADOR_FINAL STRING NOT NULL OPTIONS(description="Operador final homologado"),
  
  -- Dimensiones de Respuesta
  GRUPO_RESPUESTA STRING NOT NULL OPTIONS(description="Grupo de respuesta homologado"),
  NIVEL_1 STRING NOT NULL OPTIONS(description="Nivel 1 de respuesta homologado"),
  NIVEL_2 STRING NOT NULL OPTIONS(description="Nivel 2 de respuesta homologado"),
  
  -- Dimensiones de Cliente
  SERVICIO STRING NOT NULL OPTIONS(description="Tipo de servicio del cliente"),
  SEGMENTO_GESTION STRING NOT NULL OPTIONS(description="Segmento de gestión"),
  
  -- 🔧 NUEVAS DIMENSIONES DE VENCIMIENTO
  VENCIMIENTO DATE NOT NULL OPTIONS(description="Fecha de vencimiento de la cuenta"),
  CATEGORIA_VENCIMIENTO STRING NOT NULL OPTIONS(description="Categoría de vencimiento para análisis"),
  
  ZONA_GEOGRAFICA STRING NOT NULL OPTIONS(description="Zona geográfica"),
  
  -- Métricas de ACCIONES (cada interacción cuenta)
  Q_INTERACCIONES_TOTAL INT64 NOT NULL OPTIONS(description="Total de interacciones realizadas"),
  Q_CONTACTOS_EFECTIVOS INT64 NOT NULL OPTIONS(description="Cantidad de contactos efectivos"),
  Q_PROMESAS_DE_PAGO INT64 NOT NULL OPTIONS(description="Cantidad de promesas de pago obtenidas"),
  MONTO_COMPROMETIDO FLOAT64 NOT NULL OPTIONS(description="Monto total comprometido"),
  
  -- Métricas de CLIENTES ÚNICOS (cada cliente cuenta una vez)
  Q_CLIENTES_UNICOS_CONTACTADOS INT64 NOT NULL OPTIONS(description="Clientes únicos contactados en el día"),
  Q_CLIENTES_PRIMERA_VEZ_DIA INT64 NOT NULL OPTIONS(description="Clientes contactados por primera vez en el día"),
  Q_CLIENTES_CON_PROMESA INT64 NOT NULL OPTIONS(description="Clientes únicos que dieron promesa"),
  
  -- KPIs Pre-calculados
  EFECTIVIDAD_CONTACTO FLOAT64 OPTIONS(description="Ratio contactos efectivos / total interacciones"),
  TASA_COMPROMISO FLOAT64 OPTIONS(description="Ratio compromisos / total interacciones"),
  MONTO_PROMEDIO_COMPROMISO FLOAT64 OPTIONS(description="Monto promedio por compromiso")
)
PARTITION BY FECHA_SERVICIO
CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, CANAL
OPTIONS(
  description="Gestiones diarias agregadas por dimensiones con VENCIMIENTO. Diferencia métricas de acciones vs clientes únicos.",
  labels=[("ambiente", "produccion"), ("modulo", "faco_etl"), ("tabla_tipo", "fact")]
);

-- ##############################################################################
-- TABLA 3: RECUPERO ATRIBUIDO
-- Granularidad: Una fila por pago con atribución a gestión
-- Incluye: VENCIMIENTO para análisis de recupero por estado de vencimiento
-- ##############################################################################

CREATE TABLE `mibot-222814.BI_USA.faco_dash_recupero_atribuido`
(
  -- Dimensiones Temporales
  FECHA_PAGO DATE NOT NULL OPTIONS(description="Fecha en que se realizó el pago"),
  
  -- Dimensiones de Segmentación
  CARTERA STRING NOT NULL OPTIONS(description="Tipo de cartera"),
  SERVICIO STRING NOT NULL OPTIONS(description="Tipo de servicio"),
  
  -- 🔧 NUEVAS DIMENSIONES DE VENCIMIENTO
  VENCIMIENTO DATE NOT NULL OPTIONS(description="Fecha de vencimiento de la cuenta"),
  CATEGORIA_VENCIMIENTO STRING NOT NULL OPTIONS(description="Categoría de vencimiento"),
  
  ID_ARCHIVO_ASIGNACION STRING NOT NULL OPTIONS(description="Archivo de asignación original"),
  
  -- Dimensiones de Atribución
  CANAL_ATRIBUIDO STRING NOT NULL OPTIONS(description="Canal al que se atribuye el pago"),
  OPERADOR_ATRIBUIDO STRING NOT NULL OPTIONS(description="Operador al que se atribuye el pago"),
  
  -- Dimensiones Temporales de Atribución
  FECHA_GESTION_ATRIBUIDA DATE OPTIONS(description="Fecha de la gestión atribuida"),
  FECHA_COMPROMISO DATE OPTIONS(description="Fecha del compromiso asociado"),
  
  -- Métricas de Pago
  MONTO_PAGADO FLOAT64 NOT NULL OPTIONS(description="Monto del pago realizado"),
  
  -- Flags de Calidad de Atribución
  ES_PAGO_CON_PDP BOOL NOT NULL OPTIONS(description="Indica si el pago está asociado a una promesa"),
  PDP_ESTABA_VIGENTE BOOL NOT NULL OPTIONS(description="Indica si la promesa estaba vigente al momento del pago"),
  PAGO_ES_PUNTUAL BOOL NOT NULL OPTIONS(description="Indica si el pago fue puntual según la promesa"),
  
  -- Métricas de Timing
  DIAS_ENTRE_GESTION_Y_PAGO INT64 OPTIONS(description="Días transcurridos entre gestión y pago"),
  
  -- Score de Atribución
  EFECTIVIDAD_ATRIBUCION FLOAT64 NOT NULL OPTIONS(description="Score de 0.0 a 1.0 que indica la calidad de la atribución")
)
PARTITION BY FECHA_PAGO
CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, CANAL_ATRIBUIDO
OPTIONS(
  description="Pagos con atribución a gestión, incluyendo VENCIMIENTO para análisis segmentado de recupero.",
  labels=[("ambiente", "produccion"), ("modulo", "faco_etl"), ("tabla_tipo", "fact")]
);

-- ##############################################################################
-- TABLA 4: KPIs EJECUTIVOS
-- Granularidad: Una fila por fecha de cálculo + dimensiones ejecutivas
-- Incluye: VENCIMIENTO para dashboards ejecutivos segmentados
-- ##############################################################################

CREATE TABLE `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`
(
  -- Dimensiones Temporales
  FECHA_CALCULO DATE NOT NULL OPTIONS(description="Fecha de cálculo de los KPIs"),
  
  -- Dimensiones Ejecutivas
  CARTERA STRING NOT NULL OPTIONS(description="Tipo de cartera"),
  SERVICIO STRING NOT NULL OPTIONS(description="Tipo de servicio"),
  
  -- 🔧 NUEVAS DIMENSIONES DE VENCIMIENTO
  VENCIMIENTO DATE NOT NULL OPTIONS(description="Fecha de vencimiento"),
  CATEGORIA_VENCIMIENTO STRING NOT NULL OPTIONS(description="Categoría de vencimiento para análisis ejecutivo"),
  
  CANAL STRING NOT NULL OPTIONS(description="Canal de gestión o TOTAL para agregados"),
  
  -- Métricas de Universo
  UNIVERSO_CUENTAS INT64 NOT NULL OPTIONS(description="Total de cuentas en el universo"),
  UNIVERSO_MONTO FLOAT64 NOT NULL OPTIONS(description="Monto total del universo"),
  OBJ_RECUPERO_PROMEDIO FLOAT64 NOT NULL OPTIONS(description="Objetivo promedio de recuperación"),
  
  -- Métricas de Gestión
  CUENTAS_CONTACTADAS INT64 NOT NULL OPTIONS(description="Cuentas efectivamente contactadas"),
  TASA_CONTACTABILIDAD FLOAT64 OPTIONS(description="Porcentaje de cuentas contactadas vs universo"),
  EFECTIVIDAD_PROMEDIO FLOAT64 OPTIONS(description="Efectividad promedio de contacto"),
  
  -- Métricas de Compromiso
  MONTO_COMPROMETIDO FLOAT64 NOT NULL OPTIONS(description="Monto total comprometido"),
  TASA_COMPROMISO_PROMEDIO FLOAT64 OPTIONS(description="Tasa promedio de compromiso"),
  
  -- Métricas de Recupero
  MONTO_RECUPERADO FLOAT64 NOT NULL OPTIONS(description="Monto total recuperado"),
  TASA_RECUPERACION FLOAT64 OPTIONS(description="Porcentaje recuperado vs universo"),
  
  -- KPI Ejecutivo Principal
  CUMPLIMIENTO_OBJETIVO FLOAT64 OPTIONS(description="Ratio de cumplimiento vs objetivo (tasa_recuperacion / obj_recupero)")
)
PARTITION BY FECHA_CALCULO
CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, CANAL
OPTIONS(
  description="KPIs ejecutivos consolidados con VENCIMIENTO. Optimizado para dashboards de alta gerencia.",
  labels=[("ambiente", "produccion"), ("modulo", "faco_etl"), ("tabla_tipo", "kpi")]
);

-- ==============================================================================
-- STEP 3: CREATE VIEWS FOR LOOKER STUDIO OPTIMIZATION
-- ==============================================================================

-- ##############################################################################
-- VIEW 1: Vista consolidada para análisis por vencimiento
-- ##############################################################################

CREATE OR REPLACE VIEW `mibot-222814.BI_USA.faco_dash_vencimiento_analysis` AS
SELECT
  -- Dimensiones temporales
  FECHA_CALCULO,
  
  -- Dimensiones de vencimiento (principal para este análisis)
  CATEGORIA_VENCIMIENTO,
  VENCIMIENTO,
  
  -- Dimensiones de segmentación
  CARTERA,
  SERVICIO,
  CANAL,
  
  -- KPIs principales por vencimiento
  SUM(UNIVERSO_CUENTAS) as TOTAL_CUENTAS,
  SUM(UNIVERSO_MONTO) as TOTAL_MONTO,
  AVG(OBJ_RECUPERO_PROMEDIO) as OBJETIVO_PROMEDIO,
  SUM(CUENTAS_CONTACTADAS) as TOTAL_CONTACTADAS,
  SUM(MONTO_COMPROMETIDO) as TOTAL_COMPROMETIDO,
  SUM(MONTO_RECUPERADO) as TOTAL_RECUPERADO,
  
  -- Ratios calculados
  SAFE_DIVIDE(SUM(CUENTAS_CONTACTADAS), SUM(UNIVERSO_CUENTAS)) as TASA_CONTACTABILIDAD_CONSOLIDADA,
  SAFE_DIVIDE(SUM(MONTO_RECUPERADO), SUM(UNIVERSO_MONTO)) as TASA_RECUPERACION_CONSOLIDADA,
  SAFE_DIVIDE(
    SAFE_DIVIDE(SUM(MONTO_RECUPERADO), SUM(UNIVERSO_MONTO)), 
    AVG(OBJ_RECUPERO_PROMEDIO)
  ) as CUMPLIMIENTO_OBJETIVO_CONSOLIDADO

FROM `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`
GROUP BY 1, 2, 3, 4, 5, 6;

-- ##############################################################################
-- VIEW 2: Vista para comparación período anterior por vencimiento
-- ##############################################################################

CREATE OR REPLACE VIEW `mibot-222814.BI_USA.faco_dash_vencimiento_trends` AS
WITH datos_con_periodo_anterior AS (
  SELECT
    *,
    -- Métricas del período anterior (mismo día hábil mes anterior)
    LAG(TOTAL_CUENTAS) OVER(
      PARTITION BY CATEGORIA_VENCIMIENTO, CARTERA, SERVICIO, CANAL 
      ORDER BY FECHA_CALCULO
    ) as CUENTAS_PERIODO_ANTERIOR,
    
    LAG(TOTAL_RECUPERADO) OVER(
      PARTITION BY CATEGORIA_VENCIMIENTO, CARTERA, SERVICIO, CANAL 
      ORDER BY FECHA_CALCULO  
    ) as RECUPERADO_PERIODO_ANTERIOR,
    
    LAG(TASA_RECUPERACION_CONSOLIDADA) OVER(
      PARTITION BY CATEGORIA_VENCIMIENTO, CARTERA, SERVICIO, CANAL 
      ORDER BY FECHA_CALCULO
    ) as TASA_RECUPERACION_ANTERIOR
    
  FROM `mibot-222814.BI_USA.faco_dash_vencimiento_analysis`
)

SELECT
  *,
  -- Variaciones período a período
  SAFE_DIVIDE(TOTAL_CUENTAS - CUENTAS_PERIODO_ANTERIOR, CUENTAS_PERIODO_ANTERIOR) as VAR_CUENTAS_PCT,
  SAFE_DIVIDE(TOTAL_RECUPERADO - RECUPERADO_PERIODO_ANTERIOR, RECUPERADO_PERIODO_ANTERIOR) as VAR_RECUPERADO_PCT,
  (TASA_RECUPERACION_CONSOLIDADA - TASA_RECUPERACION_ANTERIOR) as VAR_TASA_RECUPERACION_PP
  
FROM datos_con_periodo_anterior;

-- ==============================================================================
-- STEP 4: GRANT PERMISSIONS FOR LOOKER STUDIO
-- ==============================================================================

-- Grant permisos de lectura para Looker Studio service account
-- NOTA: Ejecutar estos comandos con las cuentas de servicio correctas

/*
GRANT `roles/bigquery.dataViewer` ON TABLE `mibot-222814.BI_USA.faco_dash_asignacion_universo` 
TO "serviceAccount:looker-studio@mibot-222814.iam.gserviceaccount.com";

GRANT `roles/bigquery.dataViewer` ON TABLE `mibot-222814.BI_USA.faco_dash_gestion_agregada` 
TO "serviceAccount:looker-studio@mibot-222814.iam.gserviceaccount.com";

GRANT `roles/bigquery.dataViewer` ON TABLE `mibot-222814.BI_USA.faco_dash_recupero_atribuido` 
TO "serviceAccount:looker-studio@mibot-222814.iam.gserviceaccount.com";

GRANT `roles/bigquery.dataViewer` ON TABLE `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos` 
TO "serviceAccount:looker-studio@mibot-222814.iam.gserviceaccount.com";

GRANT `roles/bigquery.dataViewer` ON VIEW `mibot-222814.BI_USA.faco_dash_vencimiento_analysis` 
TO "serviceAccount:looker-studio@mibot-222814.iam.gserviceaccount.com";

GRANT `roles/bigquery.dataViewer` ON VIEW `mibot-222814.BI_USA.faco_dash_vencimiento_trends` 
TO "serviceAccount:looker-studio@mibot-222814.iam.gserviceaccount.com";
*/

-- ==============================================================================
-- STEP 5: VALIDATION QUERIES
-- ==============================================================================

-- Validar estructura de las tablas creadas
SELECT 
  table_name,
  table_type,
  creation_time,
  row_count,
  size_bytes
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'faco_dash_%'
ORDER BY table_name;

-- Validar clustering y particionado
SELECT 
  table_name,
  table_type,
  is_partitioned,
  partition_columns,
  clustering_columns
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'faco_dash_%'
ORDER BY table_name;

-- ==============================================================================
-- STEP 6: LOGGING AND DOCUMENTATION
-- ==============================================================================

SELECT 
  CURRENT_TIMESTAMP() as timestamp_creacion,
  'DDL FACO ETL con VENCIMIENTO como dimensión ejecutado exitosamente' as mensaje,
  'Tablas optimizadas para Looker Studio con clustering por CATEGORIA_VENCIMIENTO' as detalle,
  'Views adicionales creadas para análisis de tendencias por vencimiento' as vistas_adicionales;

-- ##############################################################################
-- NOTAS DE IMPLEMENTACIÓN:
-- 
-- 1. 🔧 VENCIMIENTO y CATEGORIA_VENCIMIENTO agregados como dimensiones en todas las tablas fact
-- 2. 📊 Clustering optimizado incluyendo CATEGORIA_VENCIMIENTO para mejor performance
-- 3. 🎯 Views adicionales para análisis específico de vencimiento y tendencias
-- 4. 🚀 Particionado por fecha para escalabilidad
-- 5. 📝 Documentación completa en metadatos de BigQuery
-- 6. 🔒 Preparado para permisos de Looker Studio
-- 
-- PRÓXIMOS PASOS:
-- - Ejecutar stored procedure sp_faco_etl_vencimiento_dimension.sql
-- - Configurar dashboards en Looker Studio usando las nuevas dimensiones
-- - Establecer alertas y monitoreo de calidad de datos
-- ##############################################################################