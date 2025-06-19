# ⚡ FACO ETL - Ejecución Rápida para Presentación

> **Para generar datos reales de cobranza en minutos usando Stored Procedures**

## 🚀 Quick Start (2 pasos)

### **Paso 1: Ejecutar el SP**
```sql
-- En BigQuery Console, ejecuta:
CALL `mibot-222814.BI_USA.sp_faco_etl_para_looker_studio`('2025-05-01', '2025-05-31');
```

### **Paso 2: Conectar Looker Studio**
- Fuente: BigQuery
- Proyecto: `mibot-222814`  
- Dataset: `BI_USA`
- Tablas: `faco_dash_*`

## 📊 Tablas Generadas

| Tabla | Descripción | Uso en Dashboard |
|-------|-------------|------------------|
| `faco_dash_asignacion_universo` | Universo de cuentas asignadas por cartera | KPIs base, filtros de cartera |
| `faco_dash_gestion_agregada` | Métricas diarias de gestión por canal | Gráficos de evolución, efectividad |
| `faco_dash_recupero_atribuido` | Pagos atribuidos a gestiones | ROI, atribución de canales |
| `faco_dash_kpis_ejecutivos` | KPIs consolidados ejecutivos | Dashboard principal, scorecards |

## 🎯 KPIs Disponibles

### **Contactabilidad**
- Tasa de contactabilidad por cartera/canal
- Clientes únicos contactados vs universo
- Efectividad de contacto por operador

### **Gestión**
- Interacciones totales vs clientes únicos
- Promesas de pago (PDPs) generadas
- Monto comprometido por canal

### **Recupero**
- Tasa de recuperación real
- Atribución de pagos a gestiones
- Cumplimiento de objetivo por cartera

### **Productividad**
- Interacciones por operador
- Efectividad por canal (BOT vs HUMANO)
- Tiempo entre gestión y pago

## 📈 Queries Listas para Looker Studio

### **KPI Dashboard Principal**
```sql
SELECT 
  CARTERA,
  CANAL,
  UNIVERSO_CUENTAS,
  CUENTAS_CONTACTADAS,
  TASA_CONTACTABILIDAD,
  EFECTIVIDAD_PROMEDIO,
  MONTO_RECUPERADO,
  TASA_RECUPERACION,
  CUMPLIMIENTO_OBJETIVO
FROM `mibot-222814.BI_USA.faco_dash_kpis_ejecutivos`
WHERE FECHA_CALCULO BETWEEN @fecha_inicio AND @fecha_fin
```

### **Evolución Diaria**
```sql
SELECT 
  FECHA_SERVICIO,
  CARTERA,
  CANAL,
  SUM(Q_INTERACCIONES_TOTAL) as INTERACCIONES,
  SUM(Q_CLIENTES_UNICOS_CONTACTADOS) as CLIENTES_CONTACTADOS,
  AVG(EFECTIVIDAD_CONTACTO) as EFECTIVIDAD
FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada`
WHERE FECHA_SERVICIO BETWEEN @fecha_inicio AND @fecha_fin
GROUP BY 1,2,3
```

### **Top Operadores**
```sql
SELECT 
  OPERADOR_FINAL,
  SUM(Q_INTERACCIONES_TOTAL) as INTERACCIONES,
  AVG(EFECTIVIDAD_CONTACTO) as EFECTIVIDAD,
  SUM(MONTO_COMPROMETIDO) as MONTO_COMPROMETIDO
FROM `mibot-222814.BI_USA.faco_dash_gestion_agregada`
WHERE FECHA_SERVICIO BETWEEN @fecha_inicio AND @fecha_fin
  AND OPERADOR_FINAL != 'SISTEMA_BOT'
GROUP BY 1
HAVING SUM(Q_INTERACCIONES_TOTAL) >= 100
ORDER BY 3 DESC
```

## 🔧 Configuración Looker Studio

### **Filtros Recomendados**
- `FECHA_*` (Date Range)
- `CARTERA` (Multi-select)
- `CANAL` (Multi-select)
- `OPERADOR_FINAL` (Multi-select)

### **Métricas Calculadas**
```javascript
// Efectividad Global
SUM(Q_CONTACTOS_EFECTIVOS) / SUM(Q_INTERACCIONES_TOTAL)

// ROI de Gestión  
SUM(MONTO_RECUPERADO) / SUM(MONTO_COMPROMETIDO)

// Productividad Operador
SUM(Q_INTERACCIONES_TOTAL) / COUNT_DISTINCT(OPERADOR_FINAL)
```

## ⚙️ Ejecución Automatizada

### **Script Bash (Linux/Mac)**
```bash
chmod +x scripts/ejecutar_etl_presentacion.sh
./scripts/ejecutar_etl_presentacion.sh
```

### **Manual (BigQuery Console)**
1. Abrir BigQuery Console
2. Copiar el SP de `stored_procedures/sp_faco_etl_looker_optimized.sql`
3. Ejecutar para crear el procedure
4. Llamar: `CALL sp_faco_etl_para_looker_studio('FECHA_INICIO', 'FECHA_FIN')`

## 🎯 Para tu Presentación

### **Dashboards Sugeridos**

**1. Executive Overview**
- Scorecard: Universo, Contactados, Recuperado, % Objetivo
- Gráfico: Evolución diaria de contactabilidad
- Tabla: Performance por cartera

**2. Operational Dashboard**  
- Gráfico: BOT vs HUMANO efectividad
- Ranking: Top 10 operadores
- Heatmap: Gestión por día de semana

**3. Attribution Analysis**
- Funnel: Asignación → Contacto → Promesa → Pago
- Sankey: Atribución de recupero por canal
- Scatter: Promesas vs Pagos reales

### **KPIs Clave para Mostrar**
- **Contactabilidad**: 45-65% es bueno
- **Efectividad**: 20-35% para humanos, 5-15% para bots
- **Recuperación**: 8-15% del monto exigible
- **Atribución**: 60-80% de pagos con gestión previa

## 🚨 Troubleshooting

**Error: Tabla no encontrada**
```sql
-- Verificar tablas disponibles
SELECT table_name 
FROM `mibot-222814.BI_USA.INFORMATION_SCHEMA.TABLES` 
WHERE table_name LIKE 'faco_dash_%'
```

**Error: Sin datos**
```sql
-- Verificar período de datos disponible
SELECT MIN(FECHA_ASIGNACION), MAX(FECHA_ASIGNACION)
FROM `mibot-222814.BI_USA.dash_P3fV4dWNeMkN5RJMhV8e_calendario_v3`
```

**Looker Studio lento**
- Usar filtros obligatorios en fecha
- Limitar a máximo 3 meses de datos
- Usar tablas pre-agregadas (kpis_ejecutivos)

---

## ✅ Checklist Presentación

- [ ] SP ejecutado exitosamente 
- [ ] 4 tablas `faco_dash_*` creadas
- [ ] Looker Studio conectado a BigQuery
- [ ] Dashboard principal con KPIs funcionando
- [ ] Filtros de fecha/cartera configurados
- [ ] Datos validados (coherencia temporal)
- [ ] Gráficos principales listos para mostrar

**¡Tu ETL está listo para la presentación! 🎉**