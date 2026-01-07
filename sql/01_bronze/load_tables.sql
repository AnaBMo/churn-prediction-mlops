-- =====================================================
-- BRONZE LAYER: Carga de Datos Crudos
-- =====================================================
-- Descripción: Cargar CSVs desde Cloud Storage a BigQuery
-- Prerequisito: Archivos CSV generados en gs://<BUCKET_NAME>/bronze/
-- =====================================================

-- =====================================================
-- TABLA 1: Bronze Clientes
-- =====================================================
LOAD DATA OVERWRITE `<PROJECT_ID>.ventas_retail.bronze_clientes`
FROM FILES (
  format = 'CSV',
  uris = ['gs://<BUCKET_NAME>/bronze/clientes/clientes_raw.csv'],
  skip_leading_rows = 1
);

-- Verificar carga
SELECT 
  'bronze_clientes' as tabla,
  COUNT(*) as total_filas,
  COUNT(DISTINCT cliente_id) as clientes_unicos
FROM `<PROJECT_ID>.ventas_retail.bronze_clientes`;


-- =====================================================
-- TABLA 2: Bronze Transacciones
-- =====================================================
LOAD DATA OVERWRITE `<PROJECT_ID>.ventas_retail.bronze_transacciones`
FROM FILES (
  format = 'CSV',
  uris = ['gs://<BUCKET_NAME>/bronze/transacciones/transacciones_raw.csv'],
  skip_leading_rows = 1
);

-- Verificar carga
SELECT 
  'bronze_transacciones' as tabla,
  COUNT(*) as total_filas,
  COUNT(DISTINCT transaccion_id) as transacciones_unicas,
  COUNT(DISTINCT cliente_id) as clientes_distintos,
  ROUND(SUM(importe_neto), 2) as ventas_totales
FROM `<PROJECT_ID>.ventas_retail.bronze_transacciones`;


-- =====================================================
-- TABLA 3: Bronze Devoluciones
-- =====================================================
LOAD DATA OVERWRITE `<PROJECT_ID>.ventas_retail.bronze_devoluciones`
FROM FILES (
  format = 'CSV',
  uris = ['gs://<BUCKET_NAME>/bronze/devoluciones/devoluciones_raw.csv'],
  skip_leading_rows = 1
);

-- Verificar carga
SELECT 
  'bronze_devoluciones' as tabla,
  COUNT(*) as total_filas,
  COUNT(DISTINCT devolucion_id) as devoluciones_unicas,
  ROUND(SUM(importe_devuelto), 2) as importe_total_devuelto
FROM `<PROJECT_ID>.ventas_retail.bronze_devoluciones`;


-- =====================================================
-- RESUMEN GENERAL DE BRONZE LAYER
-- =====================================================
SELECT 
  'RESUMEN BRONZE LAYER' as seccion,
  (SELECT COUNT(*) FROM `<PROJECT_ID>.ventas_retail.bronze_clientes`) as clientes,
  (SELECT COUNT(*) FROM `<PROJECT_ID>.ventas_retail.bronze_transacciones`) as transacciones,
  (SELECT COUNT(*) FROM `<PROJECT_ID>.ventas_retail.bronze_devoluciones`) as devoluciones;
