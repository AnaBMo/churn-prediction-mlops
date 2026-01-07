-- =====================================================
-- BRONZE LAYER: Creación de Datasets
-- =====================================================
-- Descripción: Crear los datasets necesarios para el proyecto
-- Autor: Ana Morales
-- Fecha: Enero 2025
-- =====================================================

-- Dataset principal para datos en región Madrid
CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.ventas_retail`
OPTIONS(
  location='europe-southwest1',
  description='Dataset principal - Arquitectura Medallion Bronze/Silver/Gold'
);

-- Dataset para modelos ML en región EU (requerido por BigQuery ML)
CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.ventas_retail_ml`
OPTIONS(
  location='EU',
  description='Dataset para modelos de Machine Learning'
);

-- Verificar creación
SELECT 
  schema_name,
  location,
  creation_time
FROM `<PROJECT_ID>.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name IN ('ventas_retail', 'ventas_retail_ml');
