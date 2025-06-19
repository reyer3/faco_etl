# src/etl/queries.py
"""
Central repository for BigQuery SQL queries used in the FACO ETL.
Separating SQL from Python logic improves maintainability.
"""

QUERIES = {
    'get_calendario': """
        SELECT
            ARCHIVO,
            cant_cod_luna_unique,
            cant_registros_archivo,
            FECHA_ASIGNACION,
            FECHA_TRANDEUDA,
            FECHA_CIERRE,
            VENCIMIENTO,
            DIAS_GESTION,
            DIAS_PARA_CIERRE,
            ESTADO
        FROM `{dataset}.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v4`
        WHERE DATE_TRUNC(FECHA_ASIGNACION, MONTH) = DATE('{mes_vigencia}-01')
        AND UPPER(ESTADO) = UPPER('{estado_vigencia}')
        ORDER BY FECHA_ASIGNACION DESC
    """,
    'get_asignacion': """
        SELECT
            cod_luna, cuenta, cliente, telefono, dni, tramo_gestion,
            negocio, zona, archivo, min_vto, fraccionamiento,
            cuota_fracc_act, rango_renta, decil_contacto, decil_pago,
            tipo_linea, DATE(creado_el) as fecha_carga
        FROM `{dataset}.batch_P3fV4dWNeMkN5RJMhV8e_asignacion`
        WHERE archivo IN UNNEST({archivos})
    """,
    'get_gestiones_bot': """
        SELECT
            SAFE_CAST(document AS INT64) as cod_luna, date, management,
            sub_management, compromiso, fecha_compromiso, duracion,
            phone, campaign_name, origin, weight
        FROM `{dataset}.voicebot_P3fV4dWNeMkN5RJMhV8e`
        WHERE SAFE_CAST(document AS INT64) IN UNNEST({cod_lunas})
        AND DATE(date) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND SAFE_CAST(document AS INT64) IS NOT NULL
        ORDER BY date DESC
    """,
    'get_gestiones_humano': """
        SELECT
            SAFE_CAST(document AS INT64) as cod_luna, date, management,
            sub_management, n1, n2, n3, monto_compromiso, fecha_compromiso,
            nombre_agente, correo_agente, phone, duracion, campaign_name,
            origin, weight
        FROM `{dataset}.mibotair_P3fV4dWNeMkN5RJMhV8e`
        WHERE SAFE_CAST(document AS INT64) IN UNNEST({cod_lunas})
        AND DATE(date) BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND SAFE_CAST(document AS INT64) IS NOT NULL
        ORDER BY date DESC
    """,
    'get_all_trandeuda_files': """
        SELECT DISTINCT archivo
        FROM `{dataset}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
        WHERE STARTS_WITH(archivo, 'TRAN_DEUDA_') -- Pre-filtro para eficiencia
    """,
    'get_trandeuda_data': """
        SELECT
            cod_cuenta, nro_documento, fecha_vencimiento,
            monto_exigible, archivo
        FROM `{dataset}.batch_P3fV4dWNeMkN5RJMhV8e_tran_deuda`
        WHERE archivo IN UNNEST({archivos})
    """,
    'get_pagos_data': """
        SELECT
            cod_sistema, nro_documento, monto_cancelado,
            fecha_pago, archivo
        FROM `{dataset}.batch_P3fV4dWNeMkN5RJMhV8e_pagos`
        WHERE DATE_TRUNC(fecha_pago, MONTH) = DATE('{mes_vigencia}-01')
    """
}