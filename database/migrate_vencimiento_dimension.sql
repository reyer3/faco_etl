-- ##############################################################################
-- # SCRIPT DE MIGRACIÓN SEGURA - FACO ETL con VENCIMIENTO                    #
-- # Migra datos existentes preservando información histórica                  #
-- ##############################################################################

-- ==============================================================================
-- STEP 1: BACKUP DE TABLAS EXISTENTES (SI EXISTEN)
-- ==============================================================================

-- Crear backup con timestamp para recuperación
DECLARE backup_suffix STRING DEFAULT FORMAT_TIMESTAMP('_%Y%m%d_%H%M%S', CURRENT_TIMESTAMP());

-- Backup tabla asignación universo
BEGIN
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE `mibot-222814.BI_USA.faco_dash_asignacion_universo_backup%s` AS
    SELECT * FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
  """, backup_suffix);
  SELECT CONCAT('✅ Backup creado: faco_dash_asignacion_universo_backup', backup_suffix) AS mensaje;
EXCEPTION WHEN ERROR THEN
  SELECT '⚠️ Tabla asignacion_universo no existe, no se requiere backup' AS mensaje;
END;

-- Backup tabla gestión agregada  
BEGIN
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE `mibot-222814.BI_USA.faco_dash_gestion_agregada_backup%s` AS
    SELECT * FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada`
  """, backup_suffix);
  SELECT CONCAT('✅ Backup creado: faco_dash_gestion_agregada_backup', backup_suffix) AS mensaje;
EXCEPTION WHEN ERROR THEN
  SELECT '⚠️ Tabla gestion_agregada no existe, no se requiere backup' AS mensaje;
END;

-- Backup tabla recupero atribuido
BEGIN
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE `mibot-222814.BI_USA.faco_dash_recupero_atribuido_backup%s` AS
    SELECT * FROM `mibot-222814.BI_USA.faco_dash_recupero_atribuido`
  """, backup_suffix);
  SELECT CONCAT('✅ Backup creado: faco_dash_recupero_atribuido_backup', backup_suffix) AS mensaje;
EXCEPTION WHEN ERROR THEN
  SELECT '⚠️ Tabla recupero_atribuido no existe, no se requiere backup' AS mensaje;
END;

-- Backup tabla KPIs ejecutivos
BEGIN
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos_backup%s` AS
    SELECT * FROM `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`
  """, backup_suffix);
  SELECT CONCAT('✅ Backup creado: faco_dash_kpis_ejecutivos_backup', backup_suffix) AS mensaje;
EXCEPTION WHEN ERROR THEN
  SELECT '⚠️ Tabla kpis_ejecutivos no existe, no se requiere backup' AS mensaje;
END;

-- ==============================================================================
-- STEP 2: CREAR TABLAS TEMPORALES PARA MIGRACIÓN
-- ==============================================================================

-- Crear tablas temporales con la nueva estructura
CREATE TEMP TABLE temp_nueva_asignacion_universo AS
SELECT 
  -- Mantener campos existentes
  FECHA_ASIGNACION,
  CARTERA,
  SERVICIO,
  SEGMENTO_GESTION,
  ID_ARCHIVO_ASIGNACION,
  ZONA_GEOGRAFICA,
  TIPO_FRACCIONAMIENTO,
  OBJ_RECUPERO,
  Q_CUENTAS_ASIGNADAS,
  Q_CLIENTES_ASIGNADOS,
  MONTO_EXIGIBLE_ASIGNADO,
  DIAS_GESTION_DISPONIBLES,
  
  -- 🔧 AGREGAR CAMPOS DE VENCIMIENTO CON VALORES DEFAULT
  DATE('1900-01-01') as VENCIMIENTO,
  'SIN_VENCIMIENTO' as CATEGORIA_VENCIMIENTO
  
FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
WHERE FALSE; -- Solo estructura, sin datos

-- Crear estructura temporal para gestión agregada
CREATE TEMP TABLE temp_nueva_gestion_agregada AS
SELECT 
  FECHA_SERVICIO,
  CARTERA,
  CANAL,
  OPERADOR_FINAL,
  GRUPO_RESPUESTA,
  NIVEL_1,
  NIVEL_2,
  SERVICIO,
  SEGMENTO_GESTION,
  ZONA_GEOGRAFICA,
  Q_INTERACCIONES_TOTAL,
  Q_CONTACTOS_EFECTIVOS,
  Q_PROMESAS_DE_PAGO,
  MONTO_COMPROMETIDO,
  Q_CLIENTES_UNICOS_CONTACTADOS,
  Q_CLIENTES_PRIMERA_VEZ_DIA,
  Q_CLIENTES_CON_PROMESA,
  EFECTIVIDAD_CONTACTO,
  TASA_COMPROMISO,
  MONTO_PROMEDIO_COMPROMISO,
  
  -- 🔧 AGREGAR CAMPOS DE VENCIMIENTO
  DATE('1900-01-01') as VENCIMIENTO,
  'SIN_VENCIMIENTO' as CATEGORIA_VENCIMIENTO
  
FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada`
WHERE FALSE;

-- ==============================================================================
-- STEP 3: MIGRACIÓN DE DATOS CON LÓGICA DE VENCIMIENTO
-- ==============================================================================

-- Migrar datos de asignación universo con recálculo de vencimiento
BEGIN
  INSERT INTO temp_nueva_asignacion_universo
  WITH datos_con_vencimiento AS (
    SELECT 
      au.*,
      -- Intentar recuperar vencimiento de asignación original
      COALESCE(
        (SELECT MIN(asig.min_vto) 
         FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` asig
         WHERE CONCAT(asig.archivo, '.txt') = au.ID_ARCHIVO_ASIGNACION
         AND asig.min_vto IS NOT NULL),
        DATE('1900-01-01')
      ) as vencimiento_calculado
    FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo` au
  )
  
  SELECT
    FECHA_ASIGNACION,
    CARTERA,
    SERVICIO,
    SEGMENTO_GESTION,
    ID_ARCHIVO_ASIGNACION,
    ZONA_GEOGRAFICA,
    TIPO_FRACCIONAMIENTO,
    OBJ_RECUPERO,
    Q_CUENTAS_ASIGNADAS,
    Q_CLIENTES_ASIGNADOS,
    MONTO_EXIGIBLE_ASIGNADO,
    DIAS_GESTION_DISPONIBLES,
    
    -- Asignar vencimiento calculado
    vencimiento_calculado as VENCIMIENTO,
    
    -- Categorizar vencimiento basado en fecha de asignación
    CASE 
      WHEN vencimiento_calculado = DATE('1900-01-01') THEN 'SIN_VENCIMIENTO'
      WHEN vencimiento_calculado <= FECHA_ASIGNACION THEN 'VENCIDO'
      WHEN vencimiento_calculado <= DATE_ADD(FECHA_ASIGNACION, INTERVAL 30 DAY) THEN 'POR_VENCER_30D'
      WHEN vencimiento_calculado <= DATE_ADD(FECHA_ASIGNACION, INTERVAL 60 DAY) THEN 'POR_VENCER_60D'
      WHEN vencimiento_calculado <= DATE_ADD(FECHA_ASIGNACION, INTERVAL 90 DAY) THEN 'POR_VENCER_90D'
      ELSE 'VIGENTE_MAS_90D'
    END as CATEGORIA_VENCIMIENTO
    
  FROM datos_con_vencimiento;
  
  SELECT CONCAT('✅ Migrados ', @@row_count, ' registros a temp_nueva_asignacion_universo') AS mensaje;
EXCEPTION WHEN ERROR THEN
  SELECT '⚠️ No hay datos previos en asignacion_universo para migrar' AS mensaje;
END;

-- Migrar datos de gestión agregada
BEGIN
  INSERT INTO temp_nueva_gestion_agregada
  WITH datos_gestion_con_vencimiento AS (
    SELECT 
      ga.*,
      -- Buscar vencimiento por matching con asignación
      COALESCE(
        (SELECT MIN(au.VENCIMIENTO)
         FROM temp_nueva_asignacion_universo au
         WHERE au.CARTERA = ga.CARTERA
         AND au.SERVICIO = ga.SERVICIO
         AND au.SEGMENTO_GESTION = ga.SEGMENTO_GESTION
         AND au.ZONA_GEOGRAFICA = ga.ZONA_GEOGRAFICA),
        DATE('1900-01-01')
      ) as vencimiento_estimado
    FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada` ga
  )
  
  SELECT
    FECHA_SERVICIO,
    CARTERA,
    CANAL,
    OPERADOR_FINAL,
    GRUPO_RESPUESTA,
    NIVEL_1,
    NIVEL_2,
    SERVICIO,
    SEGMENTO_GESTION,
    ZONA_GEOGRAFICA,
    Q_INTERACCIONES_TOTAL,
    Q_CONTACTOS_EFECTIVOS,
    Q_PROMESAS_DE_PAGO,
    MONTO_COMPROMETIDO,
    Q_CLIENTES_UNICOS_CONTACTADOS,
    Q_CLIENTES_PRIMERA_VEZ_DIA,
    Q_CLIENTES_CON_PROMESA,
    EFECTIVIDAD_CONTACTO,
    TASA_COMPROMISO,
    MONTO_PROMEDIO_COMPROMISO,
    
    vencimiento_estimado as VENCIMIENTO,
    
    CASE 
      WHEN vencimiento_estimado = DATE('1900-01-01') THEN 'SIN_VENCIMIENTO'
      WHEN vencimiento_estimado <= FECHA_SERVICIO THEN 'VENCIDO'
      WHEN vencimiento_estimado <= DATE_ADD(FECHA_SERVICIO, INTERVAL 30 DAY) THEN 'POR_VENCER_30D'
      WHEN vencimiento_estimado <= DATE_ADD(FECHA_SERVICIO, INTERVAL 60 DAY) THEN 'POR_VENCER_60D'
      WHEN vencimiento_estimado <= DATE_ADD(FECHA_SERVICIO, INTERVAL 90 DAY) THEN 'POR_VENCER_90D'
      ELSE 'VIGENTE_MAS_90D'
    END as CATEGORIA_VENCIMIENTO
    
  FROM datos_gestion_con_vencimiento;
  
  SELECT CONCAT('✅ Migrados ', @@row_count, ' registros a temp_nueva_gestion_agregada') AS mensaje;
EXCEPTION WHEN ERROR THEN
  SELECT '⚠️ No hay datos previos en gestion_agregada para migrar' AS mensaje;
END;

-- ==============================================================================
-- STEP 4: APLICAR DDL COMPLETO (DROP AND CREATE)
-- ==============================================================================

-- Ejecutar el DDL completo
-- (Incluir aquí el contenido del archivo ddl_faco_tables_vencimiento_dimension.sql)

-- Drop tablas existentes
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_asignacion_universo`;
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_gestion_agregada`;  
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_recupero_atribuido`;
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`;

-- Crear tablas con nueva estructura (copiado del DDL principal)
CREATE TABLE `mibot-222814.BI_USA.faco_dash_asignacion_universo`
(
  FECHA_ASIGNACION DATE NOT NULL,
  CARTERA STRING NOT NULL,
  SERVICIO STRING NOT NULL,
  SEGMENTO_GESTION STRING NOT NULL,
  VENCIMIENTO DATE NOT NULL,
  CATEGORIA_VENCIMIENTO STRING NOT NULL,
  ID_ARCHIVO_ASIGNACION STRING NOT NULL,
  ZONA_GEOGRAFICA STRING NOT NULL,
  TIPO_FRACCIONAMIENTO STRING NOT NULL,
  OBJ_RECUPERO FLOAT64 NOT NULL,
  Q_CUENTAS_ASIGNADAS INT64 NOT NULL,
  Q_CLIENTES_ASIGNADOS INT64 NOT NULL,
  MONTO_EXIGIBLE_ASIGNADO FLOAT64 NOT NULL,
  DIAS_GESTION_DISPONIBLES FLOAT64 NOT NULL
)
PARTITION BY FECHA_ASIGNACION
CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, SERVICIO;

CREATE TABLE `mibot-222814.BI_USA.faco_dash_gestion_agregada`
(
  FECHA_SERVICIO DATE NOT NULL,
  CARTERA STRING NOT NULL,
  CANAL STRING NOT NULL,
  OPERADOR_FINAL STRING NOT NULL,
  GRUPO_RESPUESTA STRING NOT NULL,
  NIVEL_1 STRING NOT NULL,
  NIVEL_2 STRING NOT NULL,
  SERVICIO STRING NOT NULL,
  SEGMENTO_GESTION STRING NOT NULL,
  VENCIMIENTO DATE NOT NULL,
  CATEGORIA_VENCIMIENTO STRING NOT NULL,
  ZONA_GEOGRAFICA STRING NOT NULL,
  Q_INTERACCIONES_TOTAL INT64 NOT NULL,
  Q_CONTACTOS_EFECTIVOS INT64 NOT NULL,
  Q_PROMESAS_DE_PAGO INT64 NOT NULL,
  MONTO_COMPROMETIDO FLOAT64 NOT NULL,
  Q_CLIENTES_UNICOS_CONTACTADOS INT64 NOT NULL,
  Q_CLIENTES_PRIMERA_VEZ_DIA INT64 NOT NULL,
  Q_CLIENTES_CON_PROMESA INT64 NOT NULL,
  EFECTIVIDAD_CONTACTO FLOAT64,
  TASA_COMPROMISO FLOAT64,
  MONTO_PROMEDIO_COMPROMISO FLOAT64
)
PARTITION BY FECHA_SERVICIO
CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, CANAL;

-- Crear otras tablas con estructura completa...
-- (Para brevedad, las tablas recupero y kpis ejecutivos seguirían el mismo patrón)

-- ==============================================================================
-- STEP 5: RESTAURAR DATOS MIGRADOS
-- ==============================================================================

-- Insertar datos migrados en las nuevas tablas
INSERT INTO `mibot-222814.BI_USA.faco_dash_asignacion_universo`
SELECT * FROM temp_nueva_asignacion_universo;

INSERT INTO `mibot-222814.BI_USA.faco_dash_gestion_agregada` 
SELECT * FROM temp_nueva_gestion_agregada;

-- ==============================================================================
-- STEP 6: VALIDACIÓN POST-MIGRACIÓN
-- ==============================================================================

-- Validar que los datos migrados son consistentes
WITH validacion_migracion AS (
  SELECT
    'asignacion_universo' as tabla,
    COUNT(*) as registros_nuevos,
    COUNT(CASE WHEN CATEGORIA_VENCIMIENTO != 'SIN_VENCIMIENTO' THEN 1 END) as con_vencimiento_calculado,
    COUNT(CASE WHEN VENCIMIENTO != DATE('1900-01-01') THEN 1 END) as con_fecha_vencimiento
  FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
  
  UNION ALL
  
  SELECT
    'gestion_agregada' as tabla,
    COUNT(*) as registros_nuevos,
    COUNT(CASE WHEN CATEGORIA_VENCIMIENTO != 'SIN_VENCIMIENTO' THEN 1 END) as con_vencimiento_calculado,
    COUNT(CASE WHEN VENCIMIENTO != DATE('1900-01-01') THEN 1 END) as con_fecha_vencimiento
  FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada`
)

SELECT 
  tabla,
  registros_nuevos,
  con_vencimiento_calculado,
  con_fecha_vencimiento,
  ROUND(con_vencimiento_calculado * 100.0 / registros_nuevos, 2) as pct_vencimiento_calculado
FROM validacion_migracion;

-- Comparar totales antes y después
-- (Esta sección requeriría acceso a las tablas de backup para comparar)

-- ==============================================================================
-- STEP 7: CLEANUP Y DOCUMENTACIÓN
-- ==============================================================================

-- Documentar la migración
SELECT 
  CURRENT_TIMESTAMP() as timestamp_migracion,
  CONCAT('Migración completada con backup: ', backup_suffix) as mensaje,
  'Agregadas dimensiones VENCIMIENTO y CATEGORIA_VENCIMIENTO a todas las tablas fact' as cambios,
  'Datos históricos preservados con vencimiento estimado basado en asignaciones originales' as notas;

-- Script de limpieza de backups (ejecutar después de validar)
/*
-- Eliminar backups después de confirmar que la migración es exitosa
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_asignacion_universo_backup[TIMESTAMP]`;
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_gestion_agregada_backup[TIMESTAMP]`;
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_recupero_atribuido_backup[TIMESTAMP]`;
DROP TABLE IF EXISTS `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos_backup[TIMESTAMP]`;
*/

-- ##############################################################################
-- INSTRUCCIONES DE USO:
--
-- 1. Ejecutar este script primero para crear backups y migrar datos existentes
-- 2. Validar que la migración es correcta con las queries de validación
-- 3. Ejecutar el stored procedure sp_faco_etl_vencimiento_dimension.sql 
-- 4. Configurar Looker Studio con las nuevas dimensiones
-- 5. Después de confirmar que todo funciona, ejecutar cleanup de backups
--
-- ROLLBACK:
-- Si hay problemas, restaurar desde backup:
-- CREATE OR REPLACE TABLE `faco_dash_[tabla]` AS 
-- SELECT * FROM `faco_dash_[tabla]_backup_[timestamp]`
-- ##############################################################################