-- =============================================================================
-- DDLs para Tablas de Salida - FACO ETL
-- Tablas optimizadas para Looker Studio con clustering y partitioning
-- Basadas en el stored procedure: sp_faco_etl_looker_optimized.sql
-- =============================================================================

-- =============================================================================
-- TABLA 1: UNIVERSO DE ASIGNACIÓN
-- Contiene el universo base de todas las cuentas asignadas por período
-- =============================================================================
CREATE OR REPLACE TABLE `mibot-222814.BI_USA.faco_dash_asignacion_universo`
(
  -- DIMENSIONES TEMPORALES
  FECHA_ASIGNACION DATE NOT NULL,
  
  -- DIMENSIONES DE NEGOCIO
  CARTERA STRING NOT NULL,              -- TEMPRANA, CUOTA_FIJA_ANUAL, ALTAS_NUEVAS, COBRANDING, OTRAS
  SERVICIO STRING NOT NULL,             -- FIJA, MOVIL, MT
  SEGMENTO_GESTION STRING NOT NULL,     -- AL VCTO, ENTRE 4 Y 15D
  VENCIMIENTO DATE,                     -- Fecha de vencimiento
  ID_ARCHIVO_ASIGNACION STRING NOT NULL,-- Identificador del archivo de asignación
  ZONA_GEOGRAFICA STRING,               -- Zona geográfica
  TIPO_FRACCIONAMIENTO STRING,          -- FRACCIONADO, NORMAL
  
  -- OBJETIVO DE NEGOCIO
  OBJ_RECUPERO FLOAT64,                 -- Objetivo de recupero (0.15, 0.20, 0.25)
  
  -- MÉTRICAS AGREGADAS
  Q_CUENTAS_ASIGNADAS INT64,            -- Cantidad de cuentas asignadas
  Q_CLIENTES_ASIGNADOS INT64,           -- Cantidad de clientes únicos asignados
  MONTO_EXIGIBLE_ASIGNADO FLOAT64,      -- Monto total exigible asignado
  DIAS_GESTION_DISPONIBLES FLOAT64      -- Promedio de días de gestión disponibles
)
PARTITION BY FECHA_ASIGNACION
CLUSTER BY CARTERA, SERVICIO, SEGMENTO_GESTION;

-- =============================================================================
-- TABLA 2: GESTIÓN DIARIA AGREGADA
-- Contiene las métricas de gestión agregadas por día y dimensiones
-- =============================================================================
CREATE OR REPLACE TABLE `mibot-222814.BI_USA.faco_dash_gestion_agregada`
(
  -- DIMENSIONES TEMPORALES
  FECHA_SERVICIO DATE NOT NULL,
  
  -- DIMENSIONES DE NEGOCIO
  CARTERA STRING NOT NULL,
  CANAL STRING NOT NULL,                -- BOT, HUMANO
  OPERADOR_FINAL STRING,                -- Nombre del operador o SISTEMA_BOT
  GRUPO_RESPUESTA STRING,               -- Respuesta homologada
  NIVEL_1 STRING,                       -- Nivel 1 de la respuesta
  NIVEL_2 STRING,                       -- Nivel 2 de la respuesta
  SERVICIO STRING,                      -- FIJA, MOVIL, MT
  SEGMENTO_GESTION STRING,              -- AL VCTO, ENTRE 4 Y 15D
  ZONA_GEOGRAFICA STRING,               -- Zona geográfica
  
  -- MÉTRICAS DE ACCIONES (cada interacción cuenta)
  Q_INTERACCIONES_TOTAL INT64,          -- Total de interacciones realizadas
  Q_CONTACTOS_EFECTIVOS INT64,          -- Cantidad de contactos efectivos
  Q_PROMESAS_DE_PAGO INT64,             -- Cantidad de promesas de pago obtenidas
  MONTO_COMPROMETIDO FLOAT64,           -- Monto total comprometido
  
  -- MÉTRICAS DE CLIENTES ÚNICOS (cada cliente cuenta una vez)
  Q_CLIENTES_UNICOS_CONTACTADOS INT64,  -- Clientes únicos contactados en el día
  Q_CLIENTES_PRIMERA_VEZ_DIA INT64,     -- Clientes contactados por primera vez en el día
  Q_CLIENTES_CON_PROMESA INT64,         -- Clientes únicos que dieron promesa
  
  -- KPIS CALCULADOS
  EFECTIVIDAD_CONTACTO FLOAT64,         -- Ratio de contactos efectivos / total interacciones
  TASA_COMPROMISO FLOAT64,              -- Ratio de promesas / total interacciones
  MONTO_PROMEDIO_COMPROMISO FLOAT64     -- Monto promedio por promesa
)
PARTITION BY FECHA_SERVICIO
CLUSTER BY CARTERA, CANAL, OPERADOR_FINAL;

-- =============================================================================
-- TABLA 3: RECUPERO ATRIBUIDO
-- Contiene los pagos con atribución a la gestión que los generó
-- =============================================================================
CREATE OR REPLACE TABLE `mibot-222814.BI_USA.faco_dash_recupero_atribuido`
(
  -- DIMENSIONES TEMPORALES
  FECHA_PAGO DATE NOT NULL,
  FECHA_GESTION_ATRIBUIDA DATE,         -- Fecha de la gestión atribuida al pago
  FECHA_COMPROMISO DATE,                -- Fecha del compromiso de pago
  
  -- DIMENSIONES DE NEGOCIO
  CARTERA STRING NOT NULL,
  SERVICIO STRING,
  ID_ARCHIVO_ASIGNACION STRING,
  
  -- ATRIBUCIÓN DE GESTIÓN
  CANAL_ATRIBUIDO STRING,               -- Canal que se atribuye el pago
  OPERADOR_ATRIBUIDO STRING,            -- Operador que se atribuye el pago
  
  -- MÉTRICAS DE PAGO
  MONTO_PAGADO FLOAT64,                 -- Monto efectivamente pagado
  
  -- FLAGS DE CALIDAD DE ATRIBUCIÓN
  ES_PAGO_CON_PDP BOOLEAN,              -- Si el pago tiene promesa previa
  PDP_ESTABA_VIGENTE BOOLEAN,           -- Si la promesa estaba vigente al momento del pago
  PAGO_ES_PUNTUAL BOOLEAN,              -- Si el pago fue exactamente en la fecha prometida
  
  -- MÉTRICAS DE TIEMPO
  DIAS_ENTRE_GESTION_Y_PAGO INT64,      -- Días transcurridos entre gestión y pago
  
  -- SCORE DE EFECTIVIDAD
  EFECTIVIDAD_ATRIBUCION FLOAT64        -- Score de 0 a 1 de qué tan atribuible es el pago
)
PARTITION BY FECHA_PAGO
CLUSTER BY CARTERA, CANAL_ATRIBUIDO, OPERADOR_ATRIBUIDO;

-- =============================================================================
-- TABLA 4: KPIS EJECUTIVOS CONSOLIDADOS
-- Contiene los KPIs ejecutivos agregados por dimensiones principales
-- =============================================================================
CREATE OR REPLACE TABLE `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`
(
  -- DIMENSIONES TEMPORALES
  FECHA_CALCULO DATE NOT NULL,          -- Fecha de referencia del cálculo
  
  -- DIMENSIONES DE NEGOCIO
  CARTERA STRING NOT NULL,
  SERVICIO STRING,
  CANAL STRING,                         -- BOT, HUMANO, TOTAL
  
  -- MÉTRICAS DE UNIVERSO BASE
  UNIVERSO_CUENTAS INT64,               -- Total de cuentas en el universo
  UNIVERSO_MONTO FLOAT64,               -- Monto total exigible del universo
  OBJ_RECUPERO_PROMEDIO FLOAT64,        -- Objetivo de recupero promedio
  
  -- MÉTRICAS DE GESTIÓN
  CUENTAS_CONTACTADAS INT64,            -- Cuentas que fueron contactadas
  TASA_CONTACTABILIDAD FLOAT64,         -- % de cuentas contactadas vs universo
  EFECTIVIDAD_PROMEDIO FLOAT64,         -- Efectividad promedio de contacto
  
  -- MÉTRICAS DE COMPROMISO
  MONTO_COMPROMETIDO FLOAT64,           -- Monto total comprometido
  TASA_COMPROMISO_PROMEDIO FLOAT64,     -- Tasa promedio de compromiso
  
  -- MÉTRICAS DE RECUPERO
  MONTO_RECUPERADO FLOAT64,             -- Monto total recuperado
  TASA_RECUPERACION FLOAT64,            -- % recuperado vs universo
  CUMPLIMIENTO_OBJETIVO FLOAT64         -- % de cumplimiento vs objetivo
)
PARTITION BY FECHA_CALCULO
CLUSTER BY CARTERA, SERVICIO, CANAL;

-- =============================================================================
-- TABLA AUXILIAR: TRACKING DE PRIMERA VEZ POR CLIENTE
-- Para el ETL Python - tracking granular de primera vez por dimensión
-- =============================================================================
CREATE OR REPLACE TABLE `mibot-222814.BI_USA.faco_dash_primera_vez_tracking`
(
  -- IDENTIFICADORES
  cliente INT64 NOT NULL,
  
  -- DIMENSIONES DE TRACKING
  CARTERA STRING NOT NULL,
  CANAL STRING NOT NULL,
  GRUPO_RESPUESTA STRING,
  
  -- EVENTOS DE PRIMERA VEZ
  FECHA_PRIMERA_VEZ_CONTACTADO DATE,    -- Primera vez contactado en esta dimensión
  FECHA_PRIMERA_VEZ_EFECTIVO DATE,      -- Primera vez contacto efectivo
  FECHA_PRIMERA_VEZ_COMPROMISO DATE,    -- Primera vez que dio compromiso
  
  -- CONTADORES ACUMULADOS
  TOTAL_CONTACTOS_ACUMULADOS INT64,     -- Total de contactos hasta la fecha
  TOTAL_EFECTIVOS_ACUMULADOS INT64,     -- Total de contactos efectivos
  TOTAL_COMPROMISOS_ACUMULADOS INT64,   -- Total de compromisos dados
  
  -- METADATA
  FECHA_ULTIMA_ACTUALIZACION TIMESTAMP  -- Cuándo se actualizó por última vez
)
PARTITION BY FECHA_PRIMERA_VEZ_CONTACTADO
CLUSTER BY CARTERA, CANAL, cliente;

-- =============================================================================
-- TABLA AUXILIAR: DÍAS HÁBILES
-- Para cálculos de comparativas periodo anterior
-- =============================================================================
CREATE OR REPLACE TABLE `mibot-222814.BI_USA.faco_dash_dias_habiles`
(
  -- DIMENSIONES TEMPORALES
  FECHA DATE NOT NULL,
  AÑO INT64 NOT NULL,
  MES INT64 NOT NULL,
  DIA INT64 NOT NULL,
  
  -- INFORMACIÓN DE DÍAS HÁBILES
  ES_DIA_HABIL BOOLEAN NOT NULL,
  DIA_HABIL_DEL_MES INT64,              -- Qué número de día hábil es en el mes (1, 2, 3...)
  DIA_HABIL_DEL_AÑO INT64,              -- Qué número de día hábil es en el año
  
  -- METADATA
  NOMBRE_DIA_SEMANA STRING,             -- Lunes, Martes, etc.
  ES_FERIADO BOOLEAN,                   -- Si es feriado nacional
  NOMBRE_FERIADO STRING,                -- Nombre del feriado si aplica
  
  -- CAMPOS DE COMPARATIVA
  FECHA_MISMO_DIA_HABIL_MES_ANTERIOR DATE, -- Misma posición de día hábil del mes anterior
  FECHA_MISMO_DIA_HABIL_AÑO_ANTERIOR DATE  -- Misma posición de día hábil del año anterior
)
PARTITION BY FECHA
CLUSTER BY AÑO, MES, ES_DIA_HABIL;

-- =============================================================================
-- VIEWS OPTIMIZADAS PARA LOOKER STUDIO
-- =============================================================================

-- View consolidada para análisis ejecutivo
CREATE OR REPLACE VIEW `mibot-222814.BI_USA.faco_dash_resumen_ejecutivo` AS
SELECT 
  k.FECHA_CALCULO,
  k.CARTERA,
  k.SERVICIO,
  k.CANAL,
  
  -- Métricas principales
  k.UNIVERSO_CUENTAS,
  k.UNIVERSO_MONTO,
  k.CUENTAS_CONTACTADAS,
  k.TASA_CONTACTABILIDAD,
  k.MONTO_COMPROMETIDO,
  k.MONTO_RECUPERADO,
  k.TASA_RECUPERACION,
  k.CUMPLIMIENTO_OBJETIVO,
  
  -- Comparativa con objetivo
  CASE 
    WHEN k.CUMPLIMIENTO_OBJETIVO >= 1.0 THEN '🟢 SUPERADO'
    WHEN k.CUMPLIMIENTO_OBJETIVO >= 0.8 THEN '🟡 EN RANGO'
    ELSE '🔴 BAJO OBJETIVO'
  END AS STATUS_OBJETIVO,
  
  -- Información de días hábiles para comparativas
  dh.DIA_HABIL_DEL_MES,
  dh.FECHA_MISMO_DIA_HABIL_MES_ANTERIOR
  
FROM `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos` k
LEFT JOIN `mibot-222814.BI_USA.faco_dash_dias_habiles` dh 
  ON k.FECHA_CALCULO = dh.FECHA;

-- View para análisis operativo detallado
CREATE OR REPLACE VIEW `mibot-222814.BI_USA.faco_dash_operativo_detalle` AS
SELECT 
  g.FECHA_SERVICIO,
  g.CARTERA,
  g.CANAL,
  g.OPERADOR_FINAL,
  g.GRUPO_RESPUESTA,
  
  -- Métricas operativas
  g.Q_INTERACCIONES_TOTAL,
  g.Q_CONTACTOS_EFECTIVOS,
  g.Q_CLIENTES_UNICOS_CONTACTADOS,
  g.EFECTIVIDAD_CONTACTO,
  g.Q_PROMESAS_DE_PAGO,
  g.MONTO_COMPROMETIDO,
  g.TASA_COMPROMISO,
  
  -- Productividad por operador
  SAFE_DIVIDE(g.Q_INTERACCIONES_TOTAL, 8) AS INTERACCIONES_POR_HORA,
  SAFE_DIVIDE(g.MONTO_COMPROMETIDO, NULLIF(g.Q_PROMESAS_DE_PAGO, 0)) AS MONTO_PROMEDIO_POR_PROMESA,
  
  -- Información de días hábiles
  dh.DIA_HABIL_DEL_MES
  
FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada` g
LEFT JOIN `mibot-222814.BI_USA.faco_dash_dias_habiles` dh 
  ON g.FECHA_SERVICIO = dh.FECHA;

-- =============================================================================
-- SCRIPT DE INICIALIZACIÓN DE DÍAS HÁBILES
-- Ejecutar una vez para poblar la tabla de días hábiles
-- =============================================================================

-- Poblar días hábiles para 2025 (expandir según necesidad)
INSERT INTO `mibot-222814.BI_USA.faco_dash_dias_habiles`
WITH fechas_base AS (
  SELECT DATE_ADD('2025-01-01', INTERVAL day_offset DAY) AS fecha
  FROM UNNEST(GENERATE_ARRAY(0, 364)) AS day_offset  -- 365 días de 2025
),
feriados_peru AS (
  SELECT fecha_feriado, nombre_feriado FROM (
    SELECT '2025-01-01' AS fecha_feriado, 'Año Nuevo' AS nombre_feriado UNION ALL
    SELECT '2025-05-01', 'Día del Trabajador' UNION ALL
    SELECT '2025-07-28', 'Fiestas Patrias - Independencia' UNION ALL
    SELECT '2025-07-29', 'Fiestas Patrias - Proclamación' UNION ALL
    SELECT '2025-12-25', 'Navidad'
    -- Agregar más feriados según necesidad
  )
),
dias_procesados AS (
  SELECT 
    f.fecha,
    EXTRACT(YEAR FROM f.fecha) AS año,
    EXTRACT(MONTH FROM f.fecha) AS mes,
    EXTRACT(DAY FROM f.fecha) AS dia,
    EXTRACT(DAYOFWEEK FROM f.fecha) AS dia_semana,
    FORMAT_DATE('%A', f.fecha) AS nombre_dia_semana,
    fp.nombre_feriado IS NOT NULL AS es_feriado,
    fp.nombre_feriado,
    
    -- Es día hábil si no es sábado (7), domingo (1), ni feriado
    (EXTRACT(DAYOFWEEK FROM f.fecha) NOT IN (1, 7) 
     AND fp.nombre_feriado IS NULL) AS es_dia_habil
     
  FROM fechas_base f
  LEFT JOIN feriados_peru fp ON f.fecha = PARSE_DATE('%Y-%m-%d', fp.fecha_feriado)
),
con_numeracion_dias_habiles AS (
  SELECT 
    *,
    -- Numerar días hábiles del mes
    CASE WHEN es_dia_habil THEN 
      ROW_NUMBER() OVER(
        PARTITION BY año, mes, es_dia_habil 
        ORDER BY fecha
      )
    END AS dia_habil_del_mes,
    
    -- Numerar días hábiles del año
    CASE WHEN es_dia_habil THEN 
      ROW_NUMBER() OVER(
        PARTITION BY año, es_dia_habil 
        ORDER BY fecha
      )
    END AS dia_habil_del_año
    
  FROM dias_procesados
)
SELECT 
  fecha,
  año,
  mes,
  dia,
  es_dia_habil,
  dia_habil_del_mes,
  dia_habil_del_año,
  nombre_dia_semana,
  es_feriado,
  nombre_feriado,
  
  -- Calcular fecha del mismo día hábil del mes anterior
  LAG(fecha, 1) OVER(
    PARTITION BY dia_habil_del_mes, es_dia_habil 
    ORDER BY año, mes
  ) AS fecha_mismo_dia_habil_mes_anterior,
  
  -- Calcular fecha del mismo día hábil del año anterior
  LAG(fecha, 1) OVER(
    PARTITION BY dia_habil_del_año, es_dia_habil 
    ORDER BY año
  ) AS fecha_mismo_dia_habil_año_anterior
  
FROM con_numeracion_dias_habiles
WHERE es_dia_habil = TRUE OR es_feriado = TRUE;

-- =============================================================================
-- ÍNDICES Y OPTIMIZACIONES ADICIONALES
-- =============================================================================

-- Las tablas ya están optimizadas con:
-- 1. Partitioning por fecha para mejor performance temporal
-- 2. Clustering por dimensiones más consultadas en Looker Studio
-- 3. Types optimizados (INT64 para contadores, FLOAT64 para ratios)
-- 4. Views pre-calculadas para análisis comunes

-- =============================================================================
-- GRANTS Y PERMISOS (Ejecutar con usuario admin)
-- =============================================================================

-- GRANT SELECT ON `mibot-222814.BI_USA.faco_dash_*` TO 'looker-studio@domain.com';
-- GRANT SELECT ON `mibot-222814.BI_USA.faco_dash_*` TO 'analytics-team@domain.com';

-- =============================================================================
-- VERIFICACIÓN DE ESTRUCTURA
-- =============================================================================

SELECT 
  table_name,
  ddl
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.TABLES` 
WHERE table_name LIKE 'faco_dash_%'
ORDER BY table_name;