-- =====================================================
-- SILVER LAYER: Clientes Limpios y Validados
-- =====================================================
-- Descripción: Limpieza, validación y enriquecimiento de datos de clientes
-- Input: bronze_clientes
-- Output: silver_clientes
-- =====================================================

CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail.silver_clientes` AS
SELECT
  -- Identificadores
  cliente_id,
  
  -- Datos personales normalizados
  TRIM(LOWER(email)) as email,
  TRIM(ciudad) as ciudad,
  TRIM(pais) as pais,
  
  -- Fechas parseadas
  PARSE_DATE('%Y-%m-%d', fecha_registro) as fecha_registro,
  PARSE_DATE('%Y-%m-%d', fecha_primera_compra) as fecha_primera_compra,
  
  -- Perfil de cliente
  perfil,
  
  -- Campos calculados
  DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y-%m-%d', fecha_primera_compra), DAY) as dias_desde_primera_compra,
  DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y-%m-%d', fecha_primera_compra), MONTH) as meses_desde_primera_compra,
  
  -- Flags de validación
  CASE 
    WHEN REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') 
    THEN TRUE 
    ELSE FALSE 
  END as email_valido,
  
  CASE 
    WHEN fecha_primera_compra IS NOT NULL 
    THEN TRUE 
    ELSE FALSE 
  END as tiene_compras,
  
  -- Metadata
  CURRENT_TIMESTAMP() as fecha_procesamiento

FROM `<PROJECT_ID>.ventas_retail.bronze_clientes`
WHERE 
  -- Filtros de calidad
  cliente_id IS NOT NULL
  AND email IS NOT NULL;


-- =====================================================
-- VALIDACIONES Y ESTADÍSTICAS
-- =====================================================

-- Verificar calidad de datos
SELECT
  'CALIDAD DE DATOS - SILVER CLIENTES' as seccion,
  COUNT(*) as total_clientes,
  COUNT(DISTINCT cliente_id) as clientes_unicos,
  
  -- Validaciones
  SUM(CASE WHEN email_valido THEN 1 ELSE 0 END) as emails_validos,
  SUM(CASE WHEN tiene_compras THEN 1 ELSE 0 END) as clientes_con_compras,
  
  -- Distribución por perfil
  COUNT(CASE WHEN perfil = 'VIP' THEN 1 END) as clientes_vip,
  COUNT(CASE WHEN perfil = 'Activo' THEN 1 END) as clientes_activos,
  COUNT(CASE WHEN perfil = 'Ocasional' THEN 1 END) as clientes_ocasionales,
  COUNT(CASE WHEN perfil = 'Churned' THEN 1 END) as clientes_churned,
  
  -- Estadísticas temporales
  ROUND(AVG(dias_desde_primera_compra), 1) as promedio_dias_antiguedad,
  ROUND(AVG(meses_desde_primera_compra), 1) as promedio_meses_antiguedad

FROM `<PROJECT_ID>.ventas_retail.silver_clientes`;


-- Top 10 ciudades con más clientes
SELECT
  ciudad,
  COUNT(*) as num_clientes,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje
FROM `<PROJECT_ID>.ventas_retail.silver_clientes`
GROUP BY ciudad
ORDER BY num_clientes DESC
LIMIT 10;
