-- =====================================================
-- GOLD LAYER: Caracterización Mensual de Clientes
-- =====================================================
-- Descripción: Feature engineering avanzado para modelos de ML
-- Input: silver_transacciones, silver_devoluciones, silver_clientes
-- Output: gold_caracterizacion_mensual_clientes
-- Objetivo: Predicción de churn y clustering
-- =====================================================

CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail.gold_caracterizacion_mensual_clientes` AS

WITH meses_calendario AS (
  -- Generar serie temporal de meses
  SELECT DISTINCT
    DATE_TRUNC(fecha_transaccion, MONTH) as fecha_referencia
  FROM `<PROJECT_ID>.ventas_retail.silver_transacciones`
),

clientes_activos AS (
  -- Clientes que han tenido al menos una transacción
  SELECT DISTINCT cliente_id
  FROM `<PROJECT_ID>.ventas_retail.silver_transacciones`
),

base_cliente_mes AS (
  -- Producto cartesiano: todos los clientes x todos los meses
  SELECT 
    c.cliente_id,
    m.fecha_referencia
  FROM clientes_activos c
  CROSS JOIN meses_calendario m
),

transacciones_mes AS (
  -- Agregaciones de transacciones por cliente-mes
  SELECT
    cliente_id,
    DATE_TRUNC(fecha_transaccion, MONTH) as fecha_referencia,
    COUNT(*) as num_compras_mes,
    SUM(importe_neto) as valor_compras_mes,
    AVG(importe_neto) as ticket_promedio_mes,
    AVG(porcentaje_descuento) as promedio_descuento_mes,
    MAX(fecha_transaccion) as fecha_ultima_compra_mes
  FROM `<PROJECT_ID>.ventas_retail.silver_transacciones`
  GROUP BY cliente_id, fecha_referencia
),

devoluciones_mes AS (
  -- Agregaciones de devoluciones por cliente-mes
  SELECT
    cliente_id,
    DATE_TRUNC(fecha_devolucion, MONTH) as fecha_referencia,
    COUNT(*) as num_devoluciones_mes,
    SUM(importe_devuelto) as importe_devuelto_mes
  FROM `<PROJECT_ID>.ventas_retail.silver_devoluciones`
  GROUP BY cliente_id, fecha_referencia
),

metricas_ventanas AS (
  -- Métricas con ventanas temporales (3 y 6 meses)
  SELECT
    b.cliente_id,
    b.fecha_referencia,
    
    -- Recency: Días desde última compra
    DATE_DIFF(
      b.fecha_referencia,
      COALESCE(
        (SELECT MAX(fecha_transaccion) 
         FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
         WHERE t.cliente_id = b.cliente_id 
         AND t.fecha_transaccion < b.fecha_referencia),
        DATE '2020-01-01'
      ),
      DAY
    ) as dias_desde_ultima_compra,
    
    -- Frequency: Número de compras en ventanas temporales
    (SELECT COUNT(*) 
     FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
     WHERE t.cliente_id = b.cliente_id 
     AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 3 MONTH)
     AND t.fecha_transaccion < b.fecha_referencia
    ) as num_compras_3m,
    
    (SELECT COUNT(*) 
     FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
     WHERE t.cliente_id = b.cliente_id 
     AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 6 MONTH)
     AND t.fecha_transaccion < b.fecha_referencia
    ) as num_compras_6m,
    
    -- Monetary: Valor de compras en ventanas temporales
    COALESCE(
      (SELECT SUM(importe_neto) 
       FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
       WHERE t.cliente_id = b.cliente_id 
       AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 3 MONTH)
       AND t.fecha_transaccion < b.fecha_referencia),
      0
    ) as valor_compras_3m,
    
    COALESCE(
      (SELECT SUM(importe_neto) 
       FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
       WHERE t.cliente_id = b.cliente_id 
       AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 6 MONTH)
       AND t.fecha_transaccion < b.fecha_referencia),
      0
    ) as valor_compras_6m,
    
    -- Ticket promedio en 6 meses
    COALESCE(
      (SELECT AVG(importe_neto) 
       FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
       WHERE t.cliente_id = b.cliente_id 
       AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 6 MONTH)
       AND t.fecha_transaccion < b.fecha_referencia),
      0
    ) as ticket_promedio_6m,
    
    -- Tendencia de valor (diferencia entre periodos)
    COALESCE(
      (SELECT SUM(importe_neto) 
       FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
       WHERE t.cliente_id = b.cliente_id 
       AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 3 MONTH)
       AND t.fecha_transaccion < b.fecha_referencia),
      0
    ) - COALESCE(
      (SELECT SUM(importe_neto) 
       FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
       WHERE t.cliente_id = b.cliente_id 
       AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 6 MONTH)
       AND t.fecha_transaccion < DATE_SUB(b.fecha_referencia, INTERVAL 3 MONTH)),
      0
    ) as tendencia_valor_6m,
    
    -- Coeficiente de variación del valor de compras (estabilidad)
    CASE 
      WHEN (SELECT AVG(importe_neto) 
            FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
            WHERE t.cliente_id = b.cliente_id 
            AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 6 MONTH)
            AND t.fecha_transaccion < b.fecha_referencia) > 0
      THEN (SELECT STDDEV(importe_neto) 
            FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
            WHERE t.cliente_id = b.cliente_id 
            AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 6 MONTH)
            AND t.fecha_transaccion < b.fecha_referencia) /
           (SELECT AVG(importe_neto) 
            FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
            WHERE t.cliente_id = b.cliente_id 
            AND t.fecha_transaccion >= DATE_SUB(b.fecha_referencia, INTERVAL 6 MONTH)
            AND t.fecha_transaccion < b.fecha_referencia)
      ELSE 0
    END as coef_variacion_valor_6m
    
  FROM base_cliente_mes b
),

features_finales AS (
  SELECT
    m.*,
    
    -- Datos del mes actual
    COALESCE(tm.num_compras_mes, 0) as num_compras_mes,
    COALESCE(tm.valor_compras_mes, 0) as valor_compras_mes,
    COALESCE(tm.ticket_promedio_mes, 0) as ticket_promedio_mes,
    COALESCE(tm.promedio_descuento_mes, 0) as promedio_descuento_mes,
    
    -- Devoluciones del mes
    COALESCE(dm.num_devoluciones_mes, 0) as num_devoluciones_mes,
    COALESCE(dm.importe_devuelto_mes, 0) as importe_devuelto_mes,
    
    -- Tasa de devolución
    CASE 
      WHEN COALESCE(tm.valor_compras_mes, 0) > 0 
      THEN COALESCE(dm.importe_devuelto_mes, 0) / tm.valor_compras_mes
      ELSE 0
    END as tasa_devolucion_mes,
    
    -- Engagement score (métrica compuesta)
    CASE
      WHEN m.num_compras_6m > 0 AND m.valor_compras_6m > 0
      THEN (m.num_compras_6m * 0.4) + 
           (LOG10(m.valor_compras_6m + 1) * 0.3) +
           ((365.0 / GREATEST(m.dias_desde_ultima_compra, 1)) * 0.3)
      ELSE 0
    END as engagement_score,
    
    -- Ratio de actividad reciente vs histórica
    CASE
      WHEN m.num_compras_6m > 0
      THEN CAST(m.num_compras_3m AS FLOAT64) / m.num_compras_6m
      ELSE 0
    END as ratio_actividad_reciente,
    
    -- Features temporales
    EXTRACT(MONTH FROM m.fecha_referencia) as mes_del_anio,
    EXTRACT(QUARTER FROM m.fecha_referencia) as trimestre,
    
    -- Antigüedad del cliente
    DATE_DIFF(
      m.fecha_referencia,
      (SELECT MIN(fecha_transaccion) 
       FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
       WHERE t.cliente_id = m.cliente_id),
      MONTH
    ) as meses_desde_primera_compra,
    
    -- Target: Churn en el mes siguiente (si no compra en siguiente mes)
    CASE
      WHEN NOT EXISTS (
        SELECT 1 
        FROM `<PROJECT_ID>.ventas_retail.silver_transacciones` t
        WHERE t.cliente_id = m.cliente_id
        AND t.fecha_transaccion >= DATE_ADD(m.fecha_referencia, INTERVAL 1 MONTH)
        AND t.fecha_transaccion < DATE_ADD(m.fecha_referencia, INTERVAL 2 MONTH)
      )
      THEN 1
      ELSE 0
    END as fuga_combinada_1m
    
  FROM metricas_ventanas m
  LEFT JOIN transacciones_mes tm
    ON m.cliente_id = tm.cliente_id 
    AND m.fecha_referencia = tm.fecha_referencia
  LEFT JOIN devoluciones_mes dm
    ON m.cliente_id = dm.cliente_id 
    AND m.fecha_referencia = dm.fecha_referencia
)

-- Query principal
SELECT * FROM features_finales
ORDER BY cliente_id, fecha_referencia;


-- =====================================================
-- VALIDACIONES Y ESTADÍSTICAS
-- =====================================================

-- Resumen de features
SELECT
  'RESUMEN GOLD LAYER' as seccion,
  COUNT(*) as total_filas,
  COUNT(DISTINCT cliente_id) as clientes_unicos,
  COUNT(DISTINCT fecha_referencia) as meses_unicos,
  
  -- Estadísticas de features
  ROUND(AVG(dias_desde_ultima_compra), 1) as avg_dias_ultima_compra,
  ROUND(AVG(num_compras_6m), 2) as avg_compras_6m,
  ROUND(AVG(valor_compras_6m), 2) as avg_valor_6m,
  ROUND(AVG(engagement_score), 2) as avg_engagement,
  
  -- Distribución de target
  SUM(fuga_combinada_1m) as casos_churn,
  ROUND(AVG(fuga_combinada_1m) * 100, 2) as tasa_churn_pct

FROM `<PROJECT_ID>.ventas_retail.gold_caracterizacion_mensual_clientes`;
