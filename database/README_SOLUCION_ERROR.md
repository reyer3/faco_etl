# 🚨 Solución al Error: "Table faco_dash_asignacion_universo was not found"

## 📋 **Problema Identificado**

Tu stored procedure `sp_faco_etl_para_looker_studio` está intentando insertar datos en **4 tablas que no existen**:

1. ❌ `faco_dash_asignacion_universo`
2. ❌ `faco_dash_gestion_agregada` 
3. ❌ `faco_dash_recupero_atribuido`
4. ❌ `faco_dash_kpis_ejecutivos`

## ✅ **Solución Inmediata**

### **Paso 1: Crear las Tablas de Salida**

Ejecuta este script en BigQuery **ANTES** de correr tu stored procedure:

```sql
-- Copiar y pegar el contenido completo de:
-- database/crear_tablas_esenciales.sql
```

O descarga directamente desde:
🔗 [crear_tablas_esenciales.sql](./crear_tablas_esenciales.sql)

### **Paso 2: Ejecutar en BigQuery Console**

```sql
-- 1. Abrir BigQuery Console
-- 2. Seleccionar proyecto: mibot-222814
-- 3. Pegar y ejecutar el script completo
-- 4. Verificar que se crearon las 4 tablas
```

### **Paso 3: Verificar Creación**

```sql
SELECT 
  table_name,
  creation_time,
  table_type
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.TABLES` 
WHERE table_name LIKE 'faco_dash_%'
ORDER BY table_name;
```

Deberías ver estas 4 tablas:
- ✅ `faco_dash_asignacion_universo`
- ✅ `faco_dash_gestion_agregada`
- ✅ `faco_dash_kpis_ejecutivos` 
- ✅ `faco_dash_recupero_atribuido`

### **Paso 4: Probar el Stored Procedure**

```sql
-- Ahora sí debería funcionar
CALL `mibot-222814.BI_USA.sp_faco_etl_para_looker_studio`('2025-06-01', '2025-06-18');
```

## 🏗️ **Estructura de las Tablas Creadas**

### **1. faco_dash_asignacion_universo**
- **Propósito**: Universo base de cuentas asignadas
- **Partición**: Por `FECHA_ASIGNACION`
- **Clustering**: Por `CARTERA`, `SERVICIO`, `SEGMENTO_GESTION`
- **Uso en Looker**: Filtros de cartera y métricas de universo

### **2. faco_dash_gestion_agregada**
- **Propósito**: Métricas diarias de gestión por operador
- **Partición**: Por `FECHA_SERVICIO`
- **Clustering**: Por `CARTERA`, `CANAL`, `OPERADOR_FINAL`
- **Uso en Looker**: Dashboards operativos y efectividad

### **3. faco_dash_recupero_atribuido**
- **Propósito**: Pagos con atribución a gestión
- **Partición**: Por `FECHA_PAGO`
- **Clustering**: Por `CARTERA`, `CANAL_ATRIBUIDO`, `OPERADOR_ATRIBUIDO`
- **Uso en Looker**: Análisis de recupero y efectividad

### **4. faco_dash_kpis_ejecutivos**
- **Propósito**: KPIs consolidados para ejecutivos
- **Partición**: Por `FECHA_CALCULO`
- **Clustering**: Por `CARTERA`, `SERVICIO`, `CANAL`
- **Uso en Looker**: Dashboards ejecutivos y cumplimiento

## 🔧 **Optimizaciones para Looker Studio**

Las tablas están optimizadas con:

✅ **Partitioning por fecha** → Consultas más rápidas por período
✅ **Clustering por dimensiones** → Filtros eficientes en Looker
✅ **Tipos de datos optimizados** → Menor uso de memoria
✅ **Campos calculados incluidos** → Menos cálculos en tiempo real

## 🚀 **Próximos Pasos**

1. **Ejecutar DDLs** → Crear las 4 tablas
2. **Probar Stored Procedure** → Verificar que funciona
3. **Poblar con datos históricos** → Ejecutar para períodos pasados
4. **Conectar Looker Studio** → Crear dashboards sobre estas tablas
5. **Implementar ETL Python** → Para automatización futura

## 🆘 **Troubleshooting**

### **Error: "Permission denied"**
```sql
-- Verificar permisos BigQuery
SELECT 
  effective_labels,
  creation_time 
FROM `mibot-222814.BI_USA.__TABLES__` 
LIMIT 1;
```

### **Error: "Dataset not found"**
```sql
-- Verificar que el dataset existe
SELECT schema_name 
FROM `mibot-222814.INFORMATION_SCHEMA.SCHEMATA` 
WHERE schema_name = 'BI_USA';
```

### **Verificar contenido después de SP**
```sql
-- Verificar que el SP pobló las tablas
SELECT 
  'asignacion_universo' as tabla, COUNT(*) as registros 
FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
UNION ALL
SELECT 
  'gestion_agregada', COUNT(*) 
FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada`
UNION ALL
SELECT 
  'recupero_atribuido', COUNT(*) 
FROM `mibot-222814.BI_USA.faco_dash_recupero_atribuido`
UNION ALL
SELECT 
  'kpis_ejecutivos', COUNT(*) 
FROM `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`;
```

## 📞 **¿Necesitas Ayuda?**

Si sigues teniendo problemas:

1. **Verifica permisos** de tu usuario en BigQuery
2. **Copia exactamente** el script `crear_tablas_esenciales.sql`
3. **Ejecuta tabla por tabla** si hay errores en el script completo
4. **Revisa logs** del stored procedure para errores específicos

Una vez creadas las tablas, tu stored procedure debería funcionar perfectamente. 🎉