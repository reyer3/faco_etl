# 🗃️ Database Scripts - FACO ETL con VENCIMIENTO

Esta carpeta contiene todos los scripts necesarios para implementar **VENCIMIENTO** como nueva dimensión en las tablas FACO ETL.

## 📁 Estructura de Archivos

```
database/
├── 📋 implementation_plan_vencimiento.sql      # Plan de implementación y análisis
├── 🔄 migrate_vencimiento_dimension.sql        # Migración segura de datos existentes  
├── 🗃️ ddl_faco_tables_vencimiento_dimension.sql # DDL: Drop and Create tablas
└── 📖 README.md                                # Esta documentación
```

## 🎯 ¿Qué Cambia con VENCIMIENTO?

### **Nuevas Dimensiones Agregadas:**
- **`VENCIMIENTO`**: Fecha exacta de vencimiento (campo `min_vto`)
- **`CATEGORIA_VENCIMIENTO`**: Categorización para análisis:
  - `VENCIDO`: Ya venció
  - `POR_VENCER_30D`: Vence en 30 días
  - `POR_VENCER_60D`: Vence en 60 días  
  - `POR_VENCER_90D`: Vence en 90 días
  - `VIGENTE_MAS_90D`: Vence en más de 90 días
  - `SIN_VENCIMIENTO`: Sin fecha de vencimiento

### **Tablas Impactadas:**
- ✅ `faco_dash_asignacion_universo`
- ✅ `faco_dash_gestion_agregada` 
- ✅ `faco_dash_recupero_atribuido`
- ✅ `faco_dash_kpis_ejecutivos`

### **Optimizaciones:**
- 🚀 **Clustering actualizado** incluyendo `CATEGORIA_VENCIMIENTO`
- 📊 **Nuevas vistas** para análisis de tendencias por vencimiento
- ⚡ **Performance mejorada** para queries segmentadas

## 🚀 Implementación Paso a Paso

### **1️⃣ Análisis Pre-Implementación**
```sql
-- Ejecutar para evaluar estado actual
psql -f database/implementation_plan_vencimiento.sql
```
**Propósito:** Analiza estructura actual, volumen de datos y estima impacto.

### **2️⃣ Migración Segura** 
```sql
-- Crear backups y migrar datos existentes
psql -f database/migrate_vencimiento_dimension.sql
```
**Propósito:** Preserva datos históricos y los migra con dimensiones de vencimiento.

### **3️⃣ Recrear Estructura**
```sql  
-- Drop and create con nueva estructura
psql -f database/ddl_faco_tables_vencimiento_dimension.sql
```
**Propósito:** Crea tablas optimizadas con clustering por vencimiento.

### **4️⃣ Ejecutar ETL Actualizado**
```sql
-- Stored procedure con nueva lógica
CALL `mibot-222814.BI_USA.sp_faco_etl_para_looker_studio`('2025-06-01', '2025-06-30');
```
**Propósito:** Ejecuta ETL con las nuevas dimensiones de vencimiento.

### **5️⃣ Validación**
```sql
-- Validar implementación exitosa
SELECT 
  CATEGORIA_VENCIMIENTO,
  COUNT(*) as cuentas,
  SUM(MONTO_EXIGIBLE_ASIGNADO) as monto_total
FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
GROUP BY CATEGORIA_VENCIMIENTO
ORDER BY monto_total DESC;
```

## 📊 Nuevas Capacidades en Looker Studio

### **Dashboards Ejecutivos Mejorados:**
- 🎯 **Segmentación por estado de vencimiento**
- 📈 **Análisis de cartera por madurez** 
- ⚠️ **Alertas de vencimientos próximos**
- 📊 **Comparativas período anterior por vencimiento**

### **KPIs Adicionales:**
- **Tasa de contactabilidad por vencimiento**
- **Efectividad de gestión por madurez de cartera**
- **Recuperación prioritaria de vencidos**
- **Proyección de vencimientos por categoría**

### **Nuevas Vistas Disponibles:**
```sql
-- Vista consolidada por vencimiento
SELECT * FROM `mibot-222814.BI_USA.faco_dash_vencimiento_analysis`;

-- Tendencias período a período
SELECT * FROM `mibot-222814.BI_USA.faco_dash_vencimiento_trends`;
```

## ⚠️ Consideraciones Importantes

### **Downtime Estimado:**
- ⏱️ **Backup:** 2-5 minutos
- 🔄 **Migración:** 5-10 minutos  
- 🗃️ **DDL:** 2-3 minutos
- 📊 **ETL inicial:** 15-30 minutos
- **Total:** ~30-45 minutos

### **Rollback Plan:**
Si hay problemas, restaurar desde backup:
```sql
-- Restaurar tabla desde backup
CREATE OR REPLACE TABLE `mibot-222814.BI_USA.faco_dash_asignacion_universo` AS
SELECT * FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo_backup_[TIMESTAMP]`;
```

### **Validaciones Post-Implementación:**
```sql
-- Verificar estructura
SELECT table_name, column_name 
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.COLUMNS`
WHERE column_name IN ('VENCIMIENTO', 'CATEGORIA_VENCIMIENTO')
AND table_name LIKE 'faco_dash_%';

-- Verificar distribución de datos
SELECT 
  CATEGORIA_VENCIMIENTO,
  COUNT(*) as registros,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje
FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`
GROUP BY CATEGORIA_VENCIMIENTO;
```

## 🔧 Troubleshooting

### **Error: Tabla no existe**
```
SOLUTION: Ejecutar scripts en orden correcto (implementación → DDL → ETL)
```

### **Error: Datos históricos perdidos**
```
SOLUTION: Restaurar desde backup y revisar script de migración
```

### **Error: Performance lenta en Looker Studio**
```
SOLUTION: Verificar que clustering incluya CATEGORIA_VENCIMIENTO
```

### **Datos de vencimiento NULL/incorrectos**
```sql
-- Diagnóstico
SELECT 
  COUNT(*) as total,
  COUNT(VENCIMIENTO) as con_vencimiento,
  COUNT(CASE WHEN CATEGORIA_VENCIMIENTO = 'SIN_VENCIMIENTO' THEN 1 END) as sin_categoria
FROM `mibot-222814.BI_USA.faco_dash_asignacion_universo`;
```

## 📞 Soporte

Para problemas o dudas:
1. **Revisar logs** del stored procedure
2. **Validar** con queries de diagnóstico
3. **Consultar** este README
4. **Rollback** si es necesario

---

## 🎉 Beneficios Post-Implementación

✅ **Análisis más granular** por estado de vencimiento  
✅ **Dashboards más informativos** con nueva segmentación  
✅ **Performance mejorada** con clustering optimizado  
✅ **Capacidad de predicción** de vencimientos  
✅ **Alertas proactivas** para gestión preventiva  

**¡La implementación de VENCIMIENTO como dimensión transforma la capacidad analítica de gestión de cobranza!** 🚀