-- ##############################################################################
-- # Stored Procedure FINAL con VENCIMIENTO como Dimensión - FACO ETL          #
-- # Incluye VENCIMIENTO en todas las tablas fact para análisis segmentado     #
-- ##############################################################################

CREATE OR REPLACE PROCEDURE `mibot-222814.BI_USA.sp_faco_etl_para_looker_studio`(
  -- PARÁMETROS DE ENTRADA
  IN p_fecha_inicio DATE, -- Fecha de inicio del rango a procesar
  IN p_fecha_fin DATE     -- Fecha de fin del rango a procesar
)
BEGIN

  -- ==================================================================
  -- 1. CONFIGURACIÓN Y DECLARACIÓN DE VARIABLES
  -- ==================================================================
  
  -- Tablas de destino finales optimizadas para Looker Studio
  DECLARE v_tabla_asignacion STRING DEFAULT 'mibot-222814.BI_USA.faco_dash_asignacion_universo';
  DECLARE v_tabla_gestion STRING DEFAULT 'mibot-222814.BI_USA.faco_dash_gestion_agregada';
  DECLARE v_tabla_recupero STRING DEFAULT 'mibot-222814.BI_USA.faco_dash_recupero_atribuido';
  DECLARE v_tabla_kpis STRING DEFAULT 'mibot-222814.BI_USA.faco_dash_kpis_ejecutivos';

  -- Variables para logging y control
  DECLARE v_inicio_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_registros_procesados INT64 DEFAULT 0;

  -- ==================================================================
  -- 2. IDEMPOTENCIA: LIMPIEZA DE DATOS DEL PERÍODO
  -- ==================================================================
  
  -- Log inicio del proceso
  SELECT CONCAT('🚀 FACO ETL iniciado para período: ', CAST(p_fecha_inicio AS STRING), ' a ', CAST(p_fecha_fin AS STRING)) AS mensaje;
  
  -- Limpieza idempotente (con manejo de errores si las tablas no existen)
  BEGIN
    EXECUTE IMMEDIATE FORMAT("DELETE FROM `%s` WHERE FECHA_ASIGNACION BETWEEN @fecha_inicio AND @fecha_fin", v_tabla_asignacion) 
      USING p_fecha_inicio AS fecha_inicio, p_fecha_fin AS fecha_fin;
  EXCEPTION WHEN ERROR THEN
    SELECT 'Tabla asignacion_universo no existe aún, se creará' AS mensaje;
  END;
  
  BEGIN
    EXECUTE IMMEDIATE FORMAT("DELETE FROM `%s` WHERE FECHA_SERVICIO BETWEEN @fecha_inicio AND @fecha_fin", v_tabla_gestion) 
      USING p_fecha_inicio AS fecha_inicio, p_fecha_fin AS fecha_fin;
  EXCEPTION WHEN ERROR THEN
    SELECT 'Tabla gestion_agregada no existe aún, se creará' AS mensaje;
  END;
  
  BEGIN
    EXECUTE IMMEDIATE FORMAT("DELETE FROM `%s` WHERE FECHA_PAGO BETWEEN @fecha_inicio AND @fecha_fin", v_tabla_recupero) 
      USING p_fecha_inicio AS fecha_inicio, p_fecha_fin AS fecha_fin;
  EXCEPTION WHEN ERROR THEN
    SELECT 'Tabla recupero_atribuido no existe aún, se creará' AS mensaje;
  END;
    
  BEGIN
    EXECUTE IMMEDIATE FORMAT("DELETE FROM `%s` WHERE FECHA_CALCULO BETWEEN @fecha_inicio AND @fecha_fin", v_tabla_kpis) 
      USING p_fecha_inicio AS fecha_inicio, p_fecha_fin AS fecha_fin;
  EXCEPTION WHEN ERROR THEN
    SELECT 'Tabla kpis_ejecutivos no existe aún, se creará' AS mensaje;
  END;

  -- ==================================================================
  -- 3. TABLAS TEMPORALES BASE CON LÓGICA DE NEGOCIO
  -- ==================================================================
  
  -- 3a. Universo de Asignaciones con Dimensiones de Looker Studio (VALORES NO NULL)
  CREATE TEMP TABLE temp_asignaciones_universo AS
  SELECT
    asig.cod_luna, 
    asig.cuenta, 
    asig.cliente,
    asig.telefono,
    COALESCE(asig.negocio, 'SIN_SERVICIO') AS SERVICIO,
    COALESCE(asig.tramo_gestion, 'SIN_SEGMENTO') AS SEGMENTO_GESTION,
    COALESCE(asig.zona, 'SIN_ZONA') AS ZONA_GEOGRAFICA,
    COALESCE(asig.min_vto, DATE('1900-01-01')) AS VENCIMIENTO,
    
    -- 🔧 NUEVA DIMENSIÓN: Categorización de vencimiento para análisis
    CASE 
      WHEN asig.min_vto IS NULL THEN 'SIN_VENCIMIENTO'
      WHEN asig.min_vto <= CURRENT_DATE() THEN 'VENCIDO'
      WHEN asig.min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'POR_VENCER_30D'
      WHEN asig.min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 60 DAY) THEN 'POR_VENCER_60D'
      WHEN asig.min_vto <= DATE_ADD(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'POR_VENCER_90D'
      ELSE 'VIGENTE_MAS_90D'
    END AS CATEGORIA_VENCIMIENTO,
    
    -- Fechas del calendario
    cal.FECHA_ASIGNACION,
    cal.FECHA_CIERRE, 
    cal.FECHA_TRANDEUDA,
    cal.DIAS_GESTION,
    COALESCE(cal.ARCHIVO, 'SIN_ARCHIVO') AS ID_ARCHIVO_ASIGNACION,
    
    -- Dimensiones derivadas para Looker Studio
    CASE 
      WHEN CONTAINS_SUBSTR(UPPER(COALESCE(asig.archivo, '')), 'TEMPRANA') THEN 'TEMPRANA' 
      WHEN CONTAINS_SUBSTR(UPPER(COALESCE(asig.archivo, '')), 'CF_ANN') THEN 'CUOTA_FIJA_ANUAL' 
      WHEN CONTAINS_SUBSTR(UPPER(COALESCE(asig.archivo, '')), '_AN_') THEN 'ALTAS_NUEVAS' 
      WHEN CONTAINS_SUBSTR(UPPER(COALESCE(asig.archivo, '')), 'COBRANDING') THEN 'COBRANDING' 
      ELSE 'OTRAS' 
    END AS CARTERA,
    
    -- Objetivo de recupero basado en reglas de negocio
    CASE 
      WHEN COALESCE(asig.tramo_gestion, '') = 'AL VCTO' THEN 0.15
      WHEN COALESCE(asig.tramo_gestion, '') = 'ENTRE 4 Y 15D' THEN 0.25
      WHEN CONTAINS_SUBSTR(UPPER(COALESCE(asig.archivo, '')), 'TEMPRANA') THEN 0.20
      ELSE 0.20
    END AS OBJ_RECUPERO,
    
    -- Flags de fraccionamiento
    CASE WHEN COALESCE(asig.fraccionamiento, 'NO') = 'SI' THEN 'FRACCIONADO' ELSE 'NORMAL' END AS TIPO_FRACCIONAMIENTO
    
  FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` AS asig
  INNER JOIN `mibot-222814.BI_USA.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v3` AS cal 
    ON asig.archivo = CONCAT(cal.ARCHIVO, '.txt');

  -- 3b. Deuda Completa con Fechas Construidas Correctamente
  CREATE TEMP TABLE temp_deuda_completa AS
  WITH deuda_con_fecha AS (
    SELECT 
      cod_cuenta, 
      nro_documento, 
      monto_exigible,
      archivo,
      creado_el,
      -- Extraer fecha del nombre del archivo TRAN_DEUDA_DDMM
      CASE 
        WHEN REGEXP_CONTAINS(archivo, r'TRAN_DEUDA_(\\d{4})') THEN
          SAFE.PARSE_DATE('%Y-%m-%d', 
            CONCAT(
              CAST(EXTRACT(YEAR FROM creado_el) AS STRING), '-',
              SUBSTR(REGEXP_EXTRACT(archivo, r'TRAN_DEUDA_(\\d{4})'), 3, 2), '-',
              SUBSTR(REGEXP_EXTRACT(archivo, r'TRAN_DEUDA_(\\d{4})'), 1, 2)
            )
          )
        ELSE DATE(creado_el)
      END AS fecha_deuda_construida
    FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
  )
  SELECT *
  FROM deuda_con_fecha
  WHERE fecha_deuda_construida IS NOT NULL;

  -- 3c. Gestiones Homologadas y Unificadas (BOT + HUMANO)
  CREATE TEMP TABLE temp_gestiones_homologadas AS
  WITH gestiones_bot AS (
    SELECT 
      SAFE_CAST(document AS INT64) AS cod_luna,
      date AS fecha_gestion,
      COALESCE(management, 'SIN_MANAGEMENT') AS management,
      '' AS sub_management,
      COALESCE(compromiso, '') AS compromiso,
      'SISTEMA_BOT' AS nombre_agente,
      CAST(0 AS FLOAT64) AS monto_compromiso,
      CAST(NULL AS DATE) AS fecha_compromiso,
      'BOT' AS canal_origen
    FROM `mibot-222814.BI_USA.voicebot_P3fV4dWNeMkN5RJMhV8e`
    WHERE SAFE_CAST(document AS INT64) IS NOT NULL
      AND DATE(date) >= '2025-01-01' -- Filtrar fechas erróneas
  ),
  
  gestiones_humano AS (
    SELECT 
      SAFE_CAST(document AS INT64) AS cod_luna,
      date AS fecha_gestion,
      COALESCE(management, 'SIN_MANAGEMENT') AS management,
      '' AS sub_management,
      '' AS compromiso,
      COALESCE(nombre_agente, 'SIN_AGENTE') AS nombre_agente,
      CAST(COALESCE(monto_compromiso, 0) AS FLOAT64) AS monto_compromiso,
      CAST(fecha_compromiso AS DATE) AS fecha_compromiso,
      'HUMANO' AS canal_origen
    FROM `mibot-222814.BI_USA.mibotair_P3fV4dWNeMkN5RJMhV8e`
    WHERE SAFE_CAST(document AS INT64) IS NOT NULL
  ),
  
  gestiones_unificadas AS (
    SELECT * FROM gestiones_bot
    UNION ALL
    SELECT * FROM gestiones_humano
  )
  
  SELECT
    g.cod_luna,
    g.fecha_gestion,
    g.monto_compromiso,
    g.fecha_compromiso,
    g.canal_origen AS CANAL,
    
    -- Operador final con homologación
    CASE 
      WHEN g.canal_origen = 'BOT' THEN 'SISTEMA_BOT'
      WHEN g.canal_origen = 'HUMANO' THEN COALESCE(h_user.usuario_homologado, g.nombre_agente, 'SIN_AGENTE')
      ELSE 'NO_IDENTIFICADO'
    END AS OPERADOR_FINAL,
    
    -- Homologación de respuestas
    COALESCE(
      CASE
        WHEN g.canal_origen = 'BOT' THEN h_bot.contactabilidad_homologada
        WHEN g.canal_origen = 'HUMANO' THEN h_call.contactabilidad
      END,
      g.management,
      'NO_IDENTIFICADO'
    ) AS GRUPO_RESPUESTA,
    
    -- Niveles de respuesta homologados
    COALESCE(
      CASE
        WHEN g.canal_origen = 'BOT' THEN h_bot.n1_homologado
        WHEN g.canal_origen = 'HUMANO' THEN h_call.n_1
      END,
      'SIN_N1'
    ) AS NIVEL_1,
    
    COALESCE(
      CASE
        WHEN g.canal_origen = 'BOT' THEN h_bot.n2_homologado
        WHEN g.canal_origen = 'HUMANO' THEN h_call.n_2
      END,
      'SIN_N2'
    ) AS NIVEL_2,
    
    -- Flag de compromiso/PDP
    CASE
      WHEN g.canal_origen = 'BOT' THEN COALESCE(h_bot.es_pdp_homologado, 0)
      WHEN g.canal_origen = 'HUMANO' THEN 
        CASE WHEN UPPER(COALESCE(h_call.pdp, '')) = 'SI' THEN 1 ELSE 0 END
      ELSE 0
    END AS es_compromiso,
    
    -- Tipo de contacto efectivo
    CASE 
      WHEN UPPER(g.management) LIKE '%CONTACTO_EFECTIVO%' OR UPPER(g.management) LIKE '%EFECTIVO%' THEN 1
      ELSE 0 
    END AS es_contacto_efectivo
    
  FROM gestiones_unificadas g
  
  -- Homologación BOT
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_voicebot` AS h_bot 
    ON g.canal_origen = 'BOT' 
    AND COALESCE(g.management, '') = h_bot.bot_management 
    AND COALESCE(g.sub_management, '') = h_bot.bot_sub_management 
    AND COALESCE(g.compromiso, '') = h_bot.bot_compromiso
  
  -- Homologación HUMANO  
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_v2` AS h_call 
    ON g.canal_origen = 'HUMANO' 
    AND COALESCE(g.management, '') = h_call.management
  
  -- Homologación USUARIOS
  LEFT JOIN `mibot-222814.BI_USA.homologacion_P3fV4dWNeMkN5RJMhV8e_usuarios` AS h_user 
    ON g.canal_origen = 'HUMANO' 
    AND g.nombre_agente = h_user.usuario;

  -- ==================================================================
  -- 4. CREACIÓN DE TABLAS FACT CON VENCIMIENTO COMO DIMENSIÓN
  -- ==================================================================
  
  -- 🔧 ACTUALIZADO: Tabla asignacion_universo con CATEGORIA_VENCIMIENTO
  CREATE TABLE IF NOT EXISTS `mibot-222814.BI_USA.faco_dash_asignacion_universo`
  (
    FECHA_ASIGNACION DATE NOT NULL,
    CARTERA STRING NOT NULL,
    SERVICIO STRING NOT NULL,
    SEGMENTO_GESTION STRING NOT NULL,
    VENCIMIENTO DATE NOT NULL,
    CATEGORIA_VENCIMIENTO STRING NOT NULL,         -- 🔧 NUEVA DIMENSIÓN
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
  CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, SERVICIO;  -- 🔧 CLUSTERING actualizado

  -- 🔧 ACTUALIZADO: Tabla gestion_agregada con VENCIMIENTO y CATEGORIA_VENCIMIENTO
  CREATE TABLE IF NOT EXISTS `mibot-222814.BI_USA.faco_dash_gestion_agregada`
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
    VENCIMIENTO DATE NOT NULL,                     -- 🔧 NUEVA DIMENSIÓN
    CATEGORIA_VENCIMIENTO STRING NOT NULL,         -- 🔧 NUEVA DIMENSIÓN
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
  CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, CANAL;     -- 🔧 CLUSTERING actualizado

  -- 🔧 ACTUALIZADO: Tabla recupero_atribuido con VENCIMIENTO
  CREATE TABLE IF NOT EXISTS `mibot-222814.BI_USA.faco_dash_recupero_atribuido`
  (
    FECHA_PAGO DATE NOT NULL,
    CARTERA STRING NOT NULL,
    SERVICIO STRING NOT NULL,
    VENCIMIENTO DATE NOT NULL,                     -- 🔧 NUEVA DIMENSIÓN
    CATEGORIA_VENCIMIENTO STRING NOT NULL,         -- 🔧 NUEVA DIMENSIÓN
    ID_ARCHIVO_ASIGNACION STRING NOT NULL,
    CANAL_ATRIBUIDO STRING NOT NULL,
    OPERADOR_ATRIBUIDO STRING NOT NULL,
    FECHA_GESTION_ATRIBUIDA DATE,
    FECHA_COMPROMISO DATE,
    MONTO_PAGADO FLOAT64 NOT NULL,
    ES_PAGO_CON_PDP BOOL NOT NULL,
    PDP_ESTABA_VIGENTE BOOL NOT NULL,
    PAGO_ES_PUNTUAL BOOL NOT NULL,
    DIAS_ENTRE_GESTION_Y_PAGO INT64,
    EFECTIVIDAD_ATRIBUCION FLOAT64 NOT NULL
  )
  PARTITION BY FECHA_PAGO
  CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, CANAL_ATRIBUIDO;  -- 🔧 CLUSTERING actualizado

  -- 🔧 ACTUALIZADO: Tabla kpis_ejecutivos con VENCIMIENTO
  CREATE TABLE IF NOT EXISTS `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`
  (
    FECHA_CALCULO DATE NOT NULL,
    CARTERA STRING NOT NULL,
    SERVICIO STRING NOT NULL,
    VENCIMIENTO DATE NOT NULL,                     -- 🔧 NUEVA DIMENSIÓN  
    CATEGORIA_VENCIMIENTO STRING NOT NULL,         -- 🔧 NUEVA DIMENSIÓN
    CANAL STRING NOT NULL,
    UNIVERSO_CUENTAS INT64 NOT NULL,
    UNIVERSO_MONTO FLOAT64 NOT NULL,
    OBJ_RECUPERO_PROMEDIO FLOAT64 NOT NULL,
    CUENTAS_CONTACTADAS INT64 NOT NULL,
    TASA_CONTACTABILIDAD FLOAT64,
    EFECTIVIDAD_PROMEDIO FLOAT64,
    MONTO_COMPROMETIDO FLOAT64 NOT NULL,
    TASA_COMPROMISO_PROMEDIO FLOAT64,
    MONTO_RECUPERADO FLOAT64 NOT NULL,
    TASA_RECUPERACION FLOAT64,
    CUMPLIMIENTO_OBJETIVO FLOAT64
  )
  PARTITION BY FECHA_CALCULO
  CLUSTER BY CARTERA, CATEGORIA_VENCIMIENTO, CANAL;           -- 🔧 CLUSTERING actualizado

  -- ==================================================================
  -- 5. TABLA FACT 1: UNIVERSO DE ASIGNACIONES CON VENCIMIENTO
  -- ==================================================================
  
  INSERT INTO `mibot-222814.BI_USA.faco_dash_asignacion_universo`
  (
    FECHA_ASIGNACION, CARTERA, SERVICIO, SEGMENTO_GESTION, VENCIMIENTO, CATEGORIA_VENCIMIENTO,
    ID_ARCHIVO_ASIGNACION, ZONA_GEOGRAFICA, TIPO_FRACCIONAMIENTO, OBJ_RECUPERO,
    Q_CUENTAS_ASIGNADAS, Q_CLIENTES_ASIGNADOS, MONTO_EXIGIBLE_ASIGNADO, DIAS_GESTION_DISPONIBLES
  )
  SELECT
    asig.FECHA_ASIGNACION,
    asig.CARTERA,
    asig.SERVICIO,
    asig.SEGMENTO_GESTION,
    asig.VENCIMIENTO,
    asig.CATEGORIA_VENCIMIENTO,                    -- 🔧 NUEVA DIMENSIÓN INCLUIDA
    asig.ID_ARCHIVO_ASIGNACION,
    asig.ZONA_GEOGRAFICA,
    asig.TIPO_FRACCIONAMIENTO,
    AVG(asig.OBJ_RECUPERO) AS OBJ_RECUPERO,
    
    -- Métricas agregadas
    COUNT(DISTINCT asig.cuenta) AS Q_CUENTAS_ASIGNADAS,
    COUNT(DISTINCT asig.cliente) AS Q_CLIENTES_ASIGNADOS,
    SUM(COALESCE(deuda.monto_exigible, 0)) AS MONTO_EXIGIBLE_ASIGNADO,
    AVG(asig.DIAS_GESTION) AS DIAS_GESTION_DISPONIBLES
    
  FROM temp_asignaciones_universo AS asig
  LEFT JOIN temp_deuda_completa AS deuda 
    ON CAST(asig.cuenta AS STRING) = deuda.cod_cuenta 
    AND asig.FECHA_TRANDEUDA = deuda.fecha_deuda_construida
  WHERE asig.FECHA_ASIGNACION BETWEEN p_fecha_inicio AND p_fecha_fin
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;              -- 🔧 GROUP BY actualizado con VENCIMIENTO

  SET v_registros_procesados = @@row_count;
  SELECT CONCAT('✅ Tabla asignación_universo poblada: ', CAST(v_registros_procesados AS STRING), ' registros') AS mensaje;

  -- ==================================================================
  -- 6. TABLA FACT 2: GESTIÓN DIARIA AGREGADA CON VENCIMIENTO
  -- ==================================================================
  
  INSERT INTO `mibot-222814.BI_USA.faco_dash_gestion_agregada`
  (
    FECHA_SERVICIO, CARTERA, CANAL, OPERADOR_FINAL, GRUPO_RESPUESTA, NIVEL_1, NIVEL_2,
    SERVICIO, SEGMENTO_GESTION, VENCIMIENTO, CATEGORIA_VENCIMIENTO, ZONA_GEOGRAFICA,
    
    -- Métricas de ACCIONES (cada interacción cuenta)
    Q_INTERACCIONES_TOTAL, Q_CONTACTOS_EFECTIVOS, Q_PROMESAS_DE_PAGO, MONTO_COMPROMETIDO,
    
    -- Métricas de CLIENTES ÚNICOS (cada cliente cuenta una vez)
    Q_CLIENTES_UNICOS_CONTACTADOS, Q_CLIENTES_PRIMERA_VEZ_DIA, Q_CLIENTES_CON_PROMESA,
    
    -- KPIs Calculados
    EFECTIVIDAD_CONTACTO, TASA_COMPROMISO, MONTO_PROMEDIO_COMPROMISO
  )
  WITH gestiones_en_contexto AS (
    SELECT
      DATE(ges.fecha_gestion) AS FECHA_SERVICIO,
      asig.CARTERA,
      ges.CANAL,
      ges.OPERADOR_FINAL,
      ges.GRUPO_RESPUESTA,
      ges.NIVEL_1,
      ges.NIVEL_2,
      asig.SERVICIO,
      asig.SEGMENTO_GESTION,
      asig.VENCIMIENTO,                            -- 🔧 NUEVA DIMENSIÓN
      asig.CATEGORIA_VENCIMIENTO,                  -- 🔧 NUEVA DIMENSIÓN
      asig.ZONA_GEOGRAFICA,
      
      -- Información de la gestión
      asig.cliente,
      ges.es_contacto_efectivo,
      ges.es_compromiso,
      ges.monto_compromiso,
      
      -- Flag de primera vez por día (para métricas de clientes únicos)
      ROW_NUMBER() OVER(
        PARTITION BY asig.cliente, DATE(ges.fecha_gestion) 
        ORDER BY ges.fecha_gestion
      ) = 1 AS es_primera_gestion_del_dia
      
    FROM temp_asignaciones_universo AS asig
    INNER JOIN temp_gestiones_homologadas AS ges 
      ON asig.cod_luna = ges.cod_luna
    WHERE DATE(ges.fecha_gestion) BETWEEN p_fecha_inicio AND p_fecha_fin
      AND DATE(ges.fecha_gestion) BETWEEN asig.FECHA_ASIGNACION AND COALESCE(asig.FECHA_CIERRE, '2099-12-31')
  )
  
  SELECT
    FECHA_SERVICIO, CARTERA, CANAL, OPERADOR_FINAL, GRUPO_RESPUESTA, NIVEL_1, NIVEL_2,
    SERVICIO, SEGMENTO_GESTION, VENCIMIENTO, CATEGORIA_VENCIMIENTO, ZONA_GEOGRAFICA,
    
    -- Métricas de ACCIONES
    COUNT(*) AS Q_INTERACCIONES_TOTAL,
    SUM(es_contacto_efectivo) AS Q_CONTACTOS_EFECTIVOS,
    SUM(es_compromiso) AS Q_PROMESAS_DE_PAGO,
    SUM(monto_compromiso) AS MONTO_COMPROMETIDO,
    
    -- Métricas de CLIENTES ÚNICOS
    SUM(CASE WHEN es_primera_gestion_del_dia THEN 1 ELSE 0 END) AS Q_CLIENTES_UNICOS_CONTACTADOS,
    SUM(CASE WHEN es_primera_gestion_del_dia THEN 1 ELSE 0 END) AS Q_CLIENTES_PRIMERA_VEZ_DIA,
    COUNT(DISTINCT CASE WHEN es_compromiso = 1 THEN cliente END) AS Q_CLIENTES_CON_PROMESA,
    
    -- KPIs Calculados
    SAFE_DIVIDE(SUM(es_contacto_efectivo), COUNT(*)) AS EFECTIVIDAD_CONTACTO,
    SAFE_DIVIDE(SUM(es_compromiso), COUNT(*)) AS TASA_COMPROMISO,
    SAFE_DIVIDE(SUM(monto_compromiso), NULLIF(SUM(es_compromiso), 0)) AS MONTO_PROMEDIO_COMPROMISO
    
  FROM gestiones_en_contexto
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;  -- 🔧 GROUP BY actualizado con VENCIMIENTO

  SET v_registros_procesados = @@row_count;
  SELECT CONCAT('✅ Tabla gestión_agregada poblada: ', CAST(v_registros_procesados AS STRING), ' registros') AS mensaje;

  -- ==================================================================
  -- 7. TABLA FACT 3: RECUPERO ATRIBUIDO CON VENCIMIENTO
  -- ==================================================================
  
  INSERT INTO `mibot-222814.BI_USA.faco_dash_recupero_atribuido`
  (
    FECHA_PAGO, CARTERA, SERVICIO, VENCIMIENTO, CATEGORIA_VENCIMIENTO, ID_ARCHIVO_ASIGNACION,
    CANAL_ATRIBUIDO, OPERADOR_ATRIBUIDO, FECHA_GESTION_ATRIBUIDA, FECHA_COMPROMISO,
    MONTO_PAGADO, ES_PAGO_CON_PDP, PDP_ESTABA_VIGENTE, PAGO_ES_PUNTUAL,
    DIAS_ENTRE_GESTION_Y_PAGO, EFECTIVIDAD_ATRIBUCION
  )
  WITH pagos_con_contexto AS (
    SELECT
      p.fecha_pago,
      p.monto_cancelado,
      p.nro_documento,
      asig.CARTERA,
      asig.SERVICIO,
      asig.VENCIMIENTO,                            -- 🔧 NUEVA DIMENSIÓN
      asig.CATEGORIA_VENCIMIENTO,                  -- 🔧 NUEVA DIMENSIÓN
      asig.ID_ARCHIVO_ASIGNACION,
      asig.cod_luna,
      
      -- Obtener la última asignación para cada pago
      ROW_NUMBER() OVER(
        PARTITION BY p.nro_documento 
        ORDER BY asig.FECHA_ASIGNACION DESC
      ) AS rn_ultima_asignacion
      
    FROM `mibot-222814.BI_USA.batch_P3fV4dWNeMkN5RJMhV8e_pagos` p
    INNER JOIN temp_deuda_completa deuda 
      ON p.nro_documento = deuda.nro_documento
    INNER JOIN temp_asignaciones_universo asig 
      ON CAST(asig.cuenta AS STRING) = deuda.cod_cuenta
    WHERE DATE(p.fecha_pago) BETWEEN p_fecha_inicio AND p_fecha_fin
  ),
  
  pagos_con_gestion_atribuida AS (
    SELECT
      p.*,
      ges.fecha_gestion,
      ges.fecha_compromiso,
      ges.CANAL,
      ges.OPERADOR_FINAL,
      ges.es_compromiso,
      ges.monto_compromiso,
      
      -- Obtener la última gestión antes del pago
      ROW_NUMBER() OVER(
        PARTITION BY p.nro_documento 
        ORDER BY ges.fecha_gestion DESC
      ) AS rn_ultima_gestion
      
    FROM pagos_con_contexto p
    LEFT JOIN temp_gestiones_homologadas ges 
      ON p.cod_luna = ges.cod_luna 
      AND DATE(ges.fecha_gestion) <= DATE(p.fecha_pago)
    WHERE p.rn_ultima_asignacion = 1
  )
  
  SELECT
    DATE(fecha_pago) AS FECHA_PAGO,
    CARTERA,
    SERVICIO,
    VENCIMIENTO,                                   -- 🔧 NUEVA DIMENSIÓN
    CATEGORIA_VENCIMIENTO,                         -- 🔧 NUEVA DIMENSIÓN
    ID_ARCHIVO_ASIGNACION,
    COALESCE(CANAL, 'SIN_GESTION_PREVIA') AS CANAL_ATRIBUIDO,
    COALESCE(OPERADOR_FINAL, 'SIN_GESTION_PREVIA') AS OPERADOR_ATRIBUIDO,
    DATE(fecha_gestion) AS FECHA_GESTION_ATRIBUIDA,
    DATE(fecha_compromiso) AS FECHA_COMPROMISO,
    monto_cancelado AS MONTO_PAGADO,
    
    -- Flags de atribución
    (es_compromiso = 1) AS ES_PAGO_CON_PDP,
    (es_compromiso = 1 AND fecha_compromiso IS NOT NULL 
     AND DATE(fecha_pago) BETWEEN DATE(fecha_compromiso) 
     AND DATE_ADD(DATE(fecha_compromiso), INTERVAL 7 DAY)) AS PDP_ESTABA_VIGENTE,
    (es_compromiso = 1 AND fecha_compromiso IS NOT NULL 
     AND DATE(fecha_pago) = DATE(fecha_compromiso)) AS PAGO_ES_PUNTUAL,
    
    -- Métricas de tiempo
    CASE 
      WHEN fecha_gestion IS NOT NULL THEN 
        DATE_DIFF(DATE(fecha_pago), DATE(fecha_gestion), DAY)
      ELSE NULL 
    END AS DIAS_ENTRE_GESTION_Y_PAGO,
    
    -- Score de efectividad de atribución
    CASE
      WHEN es_compromiso = 1 AND DATE(fecha_pago) = DATE(fecha_compromiso) THEN 1.0  -- Pago puntual
      WHEN es_compromiso = 1 AND DATE(fecha_pago) <= DATE_ADD(DATE(fecha_compromiso), INTERVAL 3 DAY) THEN 0.8  -- Pago dentro de 3 días
      WHEN es_compromiso = 1 AND DATE(fecha_pago) <= DATE_ADD(DATE(fecha_compromiso), INTERVAL 7 DAY) THEN 0.6  -- Pago dentro de semana
      WHEN fecha_gestion IS NOT NULL AND DATE_DIFF(DATE(fecha_pago), DATE(fecha_gestion), DAY) <= 7 THEN 0.4  -- Pago dentro de semana post-gestión
      WHEN fecha_gestion IS NOT NULL THEN 0.2  -- Hay gestión previa
      ELSE 0.0  -- Sin gestión atribuible
    END AS EFECTIVIDAD_ATRIBUCION
    
  FROM pagos_con_gestion_atribuida
  WHERE rn_ultima_gestion = 1 OR rn_ultima_gestion IS NULL;

  SET v_registros_procesados = @@row_count;
  SELECT CONCAT('✅ Tabla recupero_atribuido poblada: ', CAST(v_registros_procesados AS STRING), ' registros') AS mensaje;

  -- ==================================================================
  -- 8. TABLA FACT 4: KPIs EJECUTIVOS CON VENCIMIENTO
  -- ==================================================================
  
  INSERT INTO `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`
  (
    FECHA_CALCULO, CARTERA, SERVICIO, VENCIMIENTO, CATEGORIA_VENCIMIENTO, CANAL,
    UNIVERSO_CUENTAS, UNIVERSO_MONTO, OBJ_RECUPERO_PROMEDIO,
    CUENTAS_CONTACTADAS, TASA_CONTACTABILIDAD, EFECTIVIDAD_PROMEDIO,
    MONTO_COMPROMETIDO, TASA_COMPROMISO_PROMEDIO,
    MONTO_RECUPERADO, TASA_RECUPERACION, CUMPLIMIENTO_OBJETIVO
  )
  WITH kpis_por_dimension AS (
    SELECT
      p_fecha_inicio AS FECHA_CALCULO,  -- Fecha de referencia del cálculo
      COALESCE(u.CARTERA, g.CARTERA, r.CARTERA, 'SIN_CARTERA') AS CARTERA,
      COALESCE(u.SERVICIO, g.SERVICIO, r.SERVICIO, 'SIN_SERVICIO') AS SERVICIO,
      COALESCE(u.VENCIMIENTO, g.VENCIMIENTO, r.VENCIMIENTO, DATE('1900-01-01')) AS VENCIMIENTO,         -- 🔧 NUEVA DIMENSIÓN
      COALESCE(u.CATEGORIA_VENCIMIENTO, g.CATEGORIA_VENCIMIENTO, r.CATEGORIA_VENCIMIENTO, 'SIN_CATEGORIA') AS CATEGORIA_VENCIMIENTO,  -- 🔧 NUEVA DIMENSIÓN
      COALESCE(g.CANAL, r.CANAL_ATRIBUIDO, 'TOTAL') AS CANAL,
      
      -- Métricas de universo
      SUM(COALESCE(u.Q_CUENTAS_ASIGNADAS, 0)) AS UNIVERSO_CUENTAS,
      SUM(COALESCE(u.MONTO_EXIGIBLE_ASIGNADO, 0)) AS UNIVERSO_MONTO,
      AVG(COALESCE(u.OBJ_RECUPERO, 0.20)) AS OBJ_RECUPERO_PROMEDIO,
      
      -- Métricas de gestión
      SUM(COALESCE(g.Q_CLIENTES_UNICOS_CONTACTADOS, 0)) AS CUENTAS_CONTACTADAS,
      SUM(COALESCE(g.Q_INTERACCIONES_TOTAL, 0)) AS TOTAL_INTERACCIONES,
      AVG(COALESCE(g.EFECTIVIDAD_CONTACTO, 0)) AS EFECTIVIDAD_PROMEDIO,
      SUM(COALESCE(g.MONTO_COMPROMETIDO, 0)) AS MONTO_COMPROMETIDO,
      AVG(COALESCE(g.TASA_COMPROMISO, 0)) AS TASA_COMPROMISO_PROMEDIO,
      
      -- Métricas de recupero
      SUM(COALESCE(r.MONTO_PAGADO, 0)) AS MONTO_RECUPERADO
      
    FROM 
      `mibot-222814.BI_USA.faco_dash_asignacion_universo` u
    FULL OUTER JOIN 
      `mibot-222814.BI_USA.faco_dash_gestion_agregada` g
      ON u.CARTERA = g.CARTERA 
      AND u.SERVICIO = g.SERVICIO 
      AND u.CATEGORIA_VENCIMIENTO = g.CATEGORIA_VENCIMIENTO  -- 🔧 JOIN actualizado
    FULL OUTER JOIN 
      `mibot-222814.BI_USA.faco_dash_recupero_atribuido` r
      ON COALESCE(u.CARTERA, g.CARTERA) = r.CARTERA 
      AND COALESCE(u.SERVICIO, g.SERVICIO) = r.SERVICIO
      AND COALESCE(u.CATEGORIA_VENCIMIENTO, g.CATEGORIA_VENCIMIENTO) = r.CATEGORIA_VENCIMIENTO  -- 🔧 JOIN actualizado
    WHERE 
      u.FECHA_ASIGNACION BETWEEN p_fecha_inicio AND p_fecha_fin
      OR g.FECHA_SERVICIO BETWEEN p_fecha_inicio AND p_fecha_fin
      OR r.FECHA_PAGO BETWEEN p_fecha_inicio AND p_fecha_fin
    GROUP BY 1, 2, 3, 4, 5, 6                     -- 🔧 GROUP BY actualizado con VENCIMIENTO
  )
  
  SELECT
    FECHA_CALCULO,
    CARTERA,
    SERVICIO,
    VENCIMIENTO,                                   -- 🔧 NUEVA DIMENSIÓN
    CATEGORIA_VENCIMIENTO,                         -- 🔧 NUEVA DIMENSIÓN
    CANAL,
    UNIVERSO_CUENTAS,
    UNIVERSO_MONTO,
    OBJ_RECUPERO_PROMEDIO,
    CUENTAS_CONTACTADAS,
    
    -- KPIs calculados
    SAFE_DIVIDE(CUENTAS_CONTACTADAS, NULLIF(UNIVERSO_CUENTAS, 0)) AS TASA_CONTACTABILIDAD,
    EFECTIVIDAD_PROMEDIO,
    MONTO_COMPROMETIDO,
    TASA_COMPROMISO_PROMEDIO,
    MONTO_RECUPERADO,
    SAFE_DIVIDE(MONTO_RECUPERADO, NULLIF(UNIVERSO_MONTO, 0)) AS TASA_RECUPERACION,
    SAFE_DIVIDE(
      SAFE_DIVIDE(MONTO_RECUPERADO, NULLIF(UNIVERSO_MONTO, 0)), 
      NULLIF(OBJ_RECUPERO_PROMEDIO, 0)
    ) AS CUMPLIMIENTO_OBJETIVO
    
  FROM kpis_por_dimension
  WHERE UNIVERSO_CUENTAS > 0 OR CUENTAS_CONTACTADAS > 0 OR MONTO_RECUPERADO > 0;

  -- ==================================================================
  -- 9. LOGGING FINAL Y ESTADÍSTICAS CON VENCIMIENTO
  -- ==================================================================
  
  SET v_registros_procesados = @@row_count;
  SELECT CONCAT('✅ Tabla kpis_ejecutivos poblada: ', CAST(v_registros_procesados AS STRING), ' registros') AS mensaje;
  
  -- Log final del proceso con estadísticas por vencimiento
  SELECT 
    CONCAT('🎉 FACO ETL completado exitosamente en ', 
           CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), v_inicio_proceso, SECOND) AS STRING), 
           ' segundos') AS mensaje_final;
           
  -- Estadísticas de vencimiento para validación
  SELECT 
    '📊 Distribución por Categoría de Vencimiento:' AS titulo,
    CATEGORIA_VENCIMIENTO,
    COUNT(*) as registros,
    SUM(Q_CUENTAS_ASIGNADAS) as total_cuentas,
    SUM(MONTO_EXIGIBLE_ASIGNADO) as total_monto
  FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
  WHERE FECHA_ASIGNACION BETWEEN p_fecha_inicio AND p_fecha_fin
  GROUP BY CATEGORIA_VENCIMIENTO
  ORDER BY total_monto DESC;

END;