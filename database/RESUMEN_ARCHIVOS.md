# 📋 Resumen: Archivos para DDLs de Tablas de Salida

## 🚨 **Tu Error Actual**
```
Not found: Table mibot-222814:BI_USA.faco_dash_asignacion_universo was not found in location us-east1
```

## ✅ **Soluciones Disponibles**

### **🏃‍♂️ Opción 1: RÁPIDA - Script Ejecutable**
```bash
# Descargar y ejecutar script
curl -o crear_tablas.sh https://raw.githubusercontent.com/reyer3/faco_etl/main/scripts/crear_tablas_faco.sh
chmod +x crear_tablas.sh
./crear_tablas.sh
```

### **📋 Opción 2: MANUAL - BigQuery Console**
1. Abrir BigQuery Console
2. Copiar contenido de: [`database/crear_tablas_esenciales.sql`](database/crear_tablas_esenciales.sql)
3. Pegar y ejecutar en BigQuery
4. Verificar que se crearon las 4 tablas

### **🔧 Opción 3: COMPLETA - Todas las Tablas**
Si también quieres las tablas auxiliares para el ETL Python:
- Usar: [`database/ddl_tablas_salida.sql`](database/ddl_tablas_salida.sql)

## 📁 **Archivos Creados en el Repo**

```
database/
├── 📋 crear_tablas_esenciales.sql    # ⭐ USAR ESTE para resolver tu error
├── 📋 ddl_tablas_salida.sql          # Versión completa con tablas auxiliares
├── 🚨 README_SOLUCION_ERROR.md       # Guía detallada paso a paso
└── 🔧 crear_tablas_faco.sh           # Script ejecutable automático
```

## ✨ **Después de Crear las Tablas**

Tu stored procedure debería funcionar:

```sql
-- Esto ya NO debería dar error
CALL `mibot-222814.BI_USA.sp_faco_etl_para_looker_studio`('2025-06-01', '2025-06-18');
```

## 🏗️ **Tablas que se Crearán**

1. **`faco_dash_asignacion_universo`** - Universo de cuentas asignadas
2. **`faco_dash_gestion_agregada`** - Métricas diarias de gestión  
3. **`faco_dash_recupero_atribuido`** - Pagos con atribución
4. **`faco_dash_kpis_ejecutivos`** - KPIs consolidados

Todas optimizadas para **Looker Studio** con:
- ✅ Partitioning por fecha
- ✅ Clustering por dimensiones principales
- ✅ Tipos de datos optimizados

## 🆘 **Si Sigues Teniendo Problemas**

1. **Verificar permisos**: `bq ls mibot-222814:BI_USA`
2. **Verificar autenticación**: `gcloud auth list`
3. **Revisar dataset**: `bq ls mibot-222814:BI_USA | grep faco_dash`

## 📞 **Próximos Pasos**

1. ✅ Crear tablas usando cualquiera de las opciones arriba
2. ✅ Probar tu stored procedure  
3. ✅ Conectar Looker Studio a las nuevas tablas
4. 🚀 Implementar ETL Python para automatización futura

---

**¡Tu stored procedure debería funcionar perfectamente después de crear estas tablas!** 🎉