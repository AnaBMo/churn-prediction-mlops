-- =====================================================
-- SILVER LAYER: Transacciones Enriquecidas
-- =====================================================
-- Descripción: Enriquecimiento de transacciones con datos de clientes y cálculos
-- Input: bronze_transacciones + silver_clientes
-- Output: silver_transacciones
-- =====================================================

CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail.silver_transacciones` AS
SELECT
  -- Identificadores
  t.transaccion_id,
  t.cliente_id,
  
  -- Datos de la transacción
  PARSE_DATE('%Y-%m-%d', t.fecha_transaccion) as fecha_transaccion,
  t.producto,
  t.categoria,
  t.cantidad,
  t.precio_unitario,
  t.descuento,
  t.importe_neto,
  t.canal,
  t.metodo_pago,
  
  -- Join con datos del cliente
  c.perfil as perfil_cliente,
  c.ciudad as ciudad_cliente,
  
  -- Campos calculados temporales
  EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) as anio,
  EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) as mes,
  EXTRACT(QUARTER FROM PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) as trimestre,
  FORMAT_DATE('%B', PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) as nombre_mes,
  FORMAT_DATE('%A', PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) as dia_semana,
  
  -- Métricas calculadas
  ROUND((t.descuento / (t.precio_unitario * t.cantidad)) * 100, 2) as porcentaje_descuento,
  t.cantidad * t.precio_unitario as importe_bruto,
  
  -- Flags de comportamiento
  CASE 
    WHEN t.descuento > 0 THEN TRUE 
    ELSE FALSE 
  END as con_descuento,
  
  -- Estacionalidad (Black Friday, Navidad, etc.)
  CASE
    WHEN EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) = 11 
         AND EXTRACT(DAY FROM PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) BETWEEN 20 AND 30
    THEN TRUE ELSE FALSE
  END as es_black_friday,
  
  CASE
    WHEN EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) = 12 
         AND EXTRACT(DAY FROM PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) BETWEEN 1 AND 26
    THEN TRUE ELSE FALSE
  END as es_navidad,
  
  CASE
    WHEN EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', t.fecha_transaccion)) IN (1, 7)
    THEN TRUE ELSE FALSE
  END as es_rebajas,
  
  -- Metadata
  CURRENT_TIMESTAMP() as fecha_procesamiento

FROM `<PROJECT_ID>.ventas_retail.bronze_transacciones` t
LEFT JOIN `<PROJECT_ID>.ventas_retail.silver_clientes` c
  ON t.cliente_id = c.cliente_id
WHERE
  t.transaccion_id IS NOT NULL
  AND t.cliente_id IS NOT NULL
  AND t.importe_neto >= 0;


-- =====================================================
-- VALIDACIONES Y ESTADÍSTICAS
-- =====================================================

-- Resumen de transacciones
SELECT
  'RESUMEN TRANSACCIONES - SILVER' as seccion,
  COUNT(*) as total_transacciones,
  COUNT(DISTINCT transaccion_id) as transacciones_unicas,
  COUNT(DISTINCT cliente_id) as clientes_distintos,
  
  -- Métricas financieras
  ROUND(SUM(importe_neto), 2) as ventas_totales,
  ROUND(AVG(importe_neto), 2) as ticket_promedio,
  ROUND(AVG(porcentaje_descuento), 2) as descuento_promedio_pct,
  
  -- Por canal
  SUM(CASE WHEN canal = 'Web' THEN importe_neto ELSE 0 END) as ventas_web,
  SUM(CASE WHEN canal = 'App' THEN importe_neto ELSE 0 END) as ventas_app,
  SUM(CASE WHEN canal = 'Tienda' THEN importe_neto ELSE 0 END) as ventas_tienda,
  
  -- Estacionalidad
  SUM(CASE WHEN es_black_friday THEN 1 ELSE 0 END) as transacciones_black_friday,
  SUM(CASE WHEN es_navidad THEN 1 ELSE 0 END) as transacciones_navidad,
  SUM(CASE WHEN con_descuento THEN 1 ELSE 0 END) as transacciones_con_descuento

FROM `<PROJECT_ID>.ventas_retail.silver_transacciones`;


-- Ventas mensuales
SELECT
  anio,
  mes,
  nombre_mes,
  COUNT(*) as num_transacciones,
  ROUND(SUM(importe_neto), 2) as ventas_totales,
  ROUND(AVG(importe_neto), 2) as ticket_promedio
FROM `<PROJECT_ID>.ventas_retail.silver_transacciones`
GROUP BY anio, mes, nombre_mes
ORDER BY anio, mes;
