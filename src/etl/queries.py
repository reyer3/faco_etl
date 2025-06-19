"""
Central repository for BigQuery SQL queries used in the FACO ETL.
Usa parámetros de consulta (@param) para seguridad y rendimiento.
"""

QUERIES = {
    'get_calendario': """
        SELECT
            ARCHIVO, cant_cod_luna_unique, cant_registros_archivo,
            FECHA_ASIGNACION, FECHA_TRANDEUDA, FECHA_CIERRE, VENCIMIENTO,
            DIAS_GESTION, DIAS_PARA_CIERRE, ESTADO
        FROM `{dataset}.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v4`
        WHERE DATE_TRUNC(FECHA_ASIGNACION, MONTH) = DATE(@mes_vigencia)
        AND UPPER(ESTADO) = UPPER(@estado_vigencia)
        ORDER BY FECHA_ASIGNACION DESC
    """,
    'get_asignacion': """
        SELECT
            t1.cod_luna, t1.cuenta, t1.cliente, t1.telefono, t1.dni, t1.tramo_gestion,
            t1.negocio, t1.zona, t1.archivo, t1.min_vto, t1.fraccionamiento,
            t1.cuota_fracc_act, t1.rango_renta, t1.decil_contacto, t1.decil_pago,
            t1.tipo_linea, DATE(t1.creado_el) as fecha_carga
        FROM `{dataset}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion` AS t1
        JOIN UNNEST(@archivos) AS archivo_param ON t1.archivo = archivo_param
    """,
    'get_gestiones_bot': """
        SELECT
            SAFE_CAST(t1.document AS INT64) as cod_luna, t1.date, t1.management,
            t1.sub_management, t1.compromiso, t1.fecha_compromiso, t1.duracion,
            t1.phone, t1.campaign_name, t1.origin, t1.weight
        FROM `{dataset}.voicebot_P3fV4dWNeMkN5RJMhV8e` AS t1
        JOIN UNNEST(@cod_lunas) AS cod_luna_param ON SAFE_CAST(t1.document AS INT64) = cod_luna_param
        WHERE DATE(t1.date) BETWEEN DATE(@fecha_inicio) AND DATE(@fecha_fin)
        AND SAFE_CAST(t1.document AS INT64) IS NOT NULL
        ORDER BY t1.date DESC
    """,
    'get_gestiones_humano': """
        SELECT
            SAFE_CAST(t1.document AS INT64) as cod_luna, t1.date, t1.management,
            t1.sub_management, t1.n1, t1.n2, t1.n3, t1.monto_compromiso, t1.fecha_compromiso,
            t1.nombre_agente, t1.correo_agente, t1.phone, t1.duracion, t1.campaign_name,
            t1.origin, t1.weight
        FROM `{dataset}.mibotair_P3fV4dWNeMkN5RJMhV8e` AS t1
        JOIN UNNEST(@cod_lunas) AS cod_luna_param ON SAFE_CAST(t1.document AS INT64) = cod_luna_param
        WHERE DATE(t1.date) BETWEEN DATE(@fecha_inicio) AND DATE(@fecha_fin)
        AND SAFE_CAST(t1.document AS INT64) IS NOT NULL
        ORDER BY t1.date DESC
    """,
    'get_all_trandeuda_files': """
        SELECT DISTINCT archivo
        FROM `{dataset}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
    """,
    'get_trandeuda_data': """
        SELECT
            t1.cod_cuenta, t1.nro_documento, t1.fecha_vencimiento,
            t1.monto_exigible, t1.archivo
        FROM `{dataset}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda` AS t1
        JOIN UNNEST(@archivos) AS archivo_param ON t1.archivo = archivo_param
    """,
    'get_pagos_by_nro_documento': """
        SELECT
            cod_sistema, nro_documento, monto_cancelado,
            fecha_pago, archivo
        FROM `{dataset}.batch_P3fV4dWNeMkN5RJMhV8e_pagos` AS t1
        JOIN UNNEST(@nros_documento) AS doc_param ON t1.nro_documento = doc_param
    """
}