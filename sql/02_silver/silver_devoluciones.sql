-- =====================================================
-- SILVER LAYER: Devoluciones Procesadas
-- =====================================================
-- Descripción: Procesamiento de devoluciones con join a transacciones originales
-- Input: bronze_devoluciones + silver_transacciones
-- Output: silver_devoluciones
-- =====================================================

CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail.silver_devoluciones` AS
SELECT
  -- Identificadores
  d.devolucion_id,
  d.transaccion_id,
  d.cliente_id,
  
  -- Datos de la devolución
  PARSE_DATE('%Y-%m-%d', d.fecha_devolucion) as fecha_devolucion,
  d.motivo,
  d.productos_devueltos,
  d.importe_devuelto,
  
  -- Join con transacción original
  t.fecha_transaccion,
  t.importe_neto as importe_transaccion_original,
  t.perfil_cliente,
  t.canal,
  
  -- Campos calculados
  DATE_DIFF(
    PARSE_DATE('%Y-%m-%d', d.fecha_devolucion),
    t.fecha_transaccion,
    DAY
  ) as dias_hasta_devolucion,
  
  ROUND((d.importe_devuelto / t.importe_neto) * 100, 2) as porcentaje_devolucion,
  
  -- Clasificación de devolución
  CASE
    WHEN d.importe_devuelto >= t.importe_neto * 0.95 THEN 'Completa'
    WHEN d.importe_devuelto >= t.importe_neto * 0.50 THEN 'Parcial Alta'
    WHEN d.importe_devuelto > 0 THEN 'Parcial Baja'
    ELSE 'Sin Importe'
  END as tipo_devolucion,
  
  -- Flags
  CASE 
    WHEN DATE_DIFF(PARSE_DATE('%Y-%m-%d', d.fecha_devolucion), t.fecha_transaccion, DAY) <= 7 
    THEN TRUE 
    ELSE FALSE 
  END as devolucion_rapida,
  
  -- Metadata
  CURRENT_TIMESTAMP() as fecha_procesamiento

FROM `<PROJECT_ID>.ventas_retail.bronze_devoluciones` d
LEFT JOIN `<PROJECT_ID>.ventas_retail.silver_transacciones` t
  ON d.transaccion_id = t.transaccion_id
WHERE
  d.devolucion_id IS NOT NULL
  AND d.transaccion_id IS NOT NULL;


-- =====================================================
-- VALIDACIONES Y ESTADÍSTICAS
-- =====================================================

-- Resumen de devoluciones
SELECT
  'RESUMEN DEVOLUCIONES - SILVER' as seccion,
  COUNT(*) as total_devoluciones,
  COUNT(DISTINCT devolucion_id) as devoluciones_unicas,
  COUNT(DISTINCT cliente_id) as clientes_con_devoluciones,
  
  -- Métricas financieras
  ROUND(SUM(importe_devuelto), 2) as importe_total_devuelto,
  ROUND(AVG(importe_devuelto), 2) as importe_promedio_devolucion,
  ROUND(AVG(porcentaje_devolucion), 2) as porcentaje_promedio,
  
  -- Tiempos
  ROUND(AVG(dias_hasta_devolucion), 1) as dias_promedio_hasta_devolucion,
  SUM(CASE WHEN devolucion_rapida THEN 1 ELSE 0 END) as devoluciones_rapidas_7dias,
  
  -- Tipos
  SUM(CASE WHEN tipo_devolucion = 'Completa' THEN 1 ELSE 0 END) as devoluciones_completas,
  SUM(CASE WHEN tipo_devolucion LIKE 'Parcial%' THEN 1 ELSE 0 END) as devoluciones_parciales

FROM `<PROJECT_ID>.ventas_retail.silver_devoluciones`;


-- Motivos de devolución más comunes
SELECT
  motivo,
  COUNT(*) as num_devoluciones,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje,
  ROUND(AVG(importe_devuelto), 2) as importe_promedio
FROM `<PROJECT_ID>.ventas_retail.silver_devoluciones`
GROUP BY motivo
ORDER BY num_devoluciones DESC;


-- Tasa de devolución por perfil de cliente
SELECT
  perfil_cliente,
  COUNT(*) as num_devoluciones,
  ROUND(AVG(porcentaje_devolucion), 2) as porcentaje_promedio_devuelto,
  ROUND(AVG(dias_hasta_devolucion), 1) as dias_promedio
FROM `<PROJECT_ID>.ventas_retail.silver_devoluciones`
GROUP BY perfil_cliente
ORDER BY num_devoluciones DESC;
