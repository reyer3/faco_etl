# 🚀 FACO ETL - GUÍA RÁPIDA PARA PRESENTACIÓN

## ⚡ Setup Inmediato (2 minutos)

### 1. **Clonar y configurar credenciales**:
```bash
git clone https://github.com/reyer3/faco_etl.git
cd faco_etl

# Autenticarse con Google Cloud
gcloud auth application-default login
```

### 2. **Instalar dependencias**:
```bash
pip install -r requirements.txt
```

### 3. **Configurar variables** (opcional):
```bash
cp .env.example .env
# Editar .env si necesitas cambiar proyecto/dataset
```

## 🎯 **Para tu PRESENTACIÓN - Comandos Clave**

### **Verificar conectividad rápida**:
```bash
python main.py --test-connectivity
```

### **Resumen ejecutivo para presentación**:
```bash
python main.py --quick-summary --mes 2025-06 --estado abierto
```
> ✨ **PERFECTO para presentaciones** - Te da métricas resumidas listas para mostrar

### **Extraer y procesar datos reales**:
```bash
# Modo seguro (sin escribir a BigQuery)
python main.py --dry-run --mes 2025-06 --estado abierto

# Procesamiento completo
python main.py --mes 2025-06 --estado abierto
```

## 📊 **Cambios Implementados para tu Presentación**

### ✅ **Problemas Resueltos**:
1. **Error de credenciales**: Ahora detecta automáticamente credenciales locales/Docker
2. **Tabla calendario v4**: Actualizada con nuevos campos (`cant_cod_luna_unique`, etc.)
3. **Queries centralizadas**: Todas las consultas están en `src/etl/queries.py`
4. **Validación temporal**: Gestiones filtradas por período del calendario

### ✅ **Nuevas Funcionalidades**:
- `--quick-summary`: Métricas ejecutivas en 30 segundos
- `--test-connectivity`: Validación rápida sin procesamiento
- `--dry-run`: Procesamiento completo sin escribir a BigQuery
- Logging estructurado con emojis para fácil lectura

## 🎯 **Flujo Recomendado para tu Presentación**

### **1. Validar Setup (30 segundos)**:
```bash
python main.py --test-connectivity
```

### **2. Obtener Métricas Ejecutivas (1 minuto)**:
```bash
python main.py --quick-summary --mes 2025-06 --estado abierto
```

### **3. Procesar Datos si Necesario (5-15 minutos)**:
```bash
python main.py --dry-run --mes 2025-06 --estado abierto
```

## 📈 **Output para Presentación**

El comando `--quick-summary` te dará:

```
📈 DATOS DISPONIBLES PARA PRESENTACIÓN:
==================================================
📅 Período: 2025-06-01 → 2025-06-30
📁 Archivos de cartera: 12
⏰ Días de gestión disponibles: 30
📊 Estado del período: ABIERTO

👥 Total cuentas asignadas: 127,450
🏢 Cuentas únicas: 125,330
📱 Teléfonos únicos: 118,220

📊 Distribución por tramo:
   • AL VCTO: 45,230 (35.5%)
   • ENTRE 4 Y 15D: 82,220 (64.5%)

📊 Distribución por negocio:
   • MOVIL: 78,450 (61.5%)
   • FIJA: 48,890 (38.4%)
   • MT: 110 (0.1%)
==================================================
✅ Datos listos para ETL y dashboards en Looker Studio
```

## 🚨 **Solución Rápida a Problemas Comunes**

### **Error de credenciales**:
```bash
# Opción 1: Autenticación rápida
gcloud auth application-default login

# Opción 2: Ver ayuda detallada
python main.py --setup-help
```

### **Sin datos para el período**:
```bash
# Probar diferentes meses/estados
python main.py --quick-summary --mes 2025-05 --estado finalizado
python main.py --quick-summary --mes 2025-06 --estado abierto
```

### **Debug detallado**:
```bash
python main.py --debug --quick-summary --mes 2025-06
```

## 📋 **Tabla calendario v4 - Nuevos Campos**

Ahora usando:
```sql
SELECT 
    ARCHIVO,
    cant_cod_luna_unique,      -- Cantidad de cod_lunas únicos 
    cant_registros_archivo,    -- Total registros en archivo
    FECHA_ASIGNACION,
    FECHA_TRANDEUDA, 
    FECHA_CIERRE,
    VENCIMIENTO,
    DIAS_GESTION,
    DIAS_PARA_CIERRE,
    ESTADO
FROM `BI_USA.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v4`
```

## 🎉 **Para tu Presentación**

1. **Ejecutar**: `python main.py --quick-summary --mes 2025-06`
2. **Copiar output** para slides 
3. **Mostrar**: Volúmenes, distribuciones, estado de datos
4. **Demostrar**: ETL funcionando con `--dry-run`

¡**Listo para presentación en minutos**! 🚀