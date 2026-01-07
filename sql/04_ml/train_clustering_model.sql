-- =====================================================
-- ML: Modelo de Segmentación de Clientes (K-Means)
-- =====================================================
-- Descripción: Clustering de clientes en 5 segmentos
-- Input: gold_caracterizacion_mensual_clientes
-- Output: modelo_clustering_kmeans
-- Tiempo estimado: 1-2 minutos
-- =====================================================

-- =====================================================
-- PASO 1: Preparar datos para clustering
-- =====================================================
CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail_ml.datos_clustering` AS
SELECT
  cliente_id,
  
  -- Features RFM normalizadas (importantes para clustering)
  dias_desde_ultima_compra,
  num_compras_6m,
  valor_compras_6m,
  ticket_promedio_6m,
  
  -- Features de comportamiento
  engagement_score,
  tendencia_valor_6m,
  coef_variacion_valor_6m,
  ratio_actividad_reciente,
  
  -- Temporales
  meses_desde_primera_compra

FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
WHERE 
  -- Usar último mes completo disponible (snapshot actual)
  fecha_referencia = (
    SELECT MAX(fecha_referencia) 
    FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
    WHERE fecha_referencia < (
      SELECT MAX(fecha_referencia) 
      FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
    )
  )
  AND dias_desde_ultima_compra <= 730;


-- =====================================================
-- PASO 2: Entrenar modelo K-Means con 5 clusters
-- =====================================================
CREATE OR REPLACE MODEL `<PROJECT_ID>.ventas_retail_ml.modelo_clustering_kmeans`
OPTIONS(
  model_type='KMEANS',
  num_clusters=5,                   -- 5 segmentos de clientes
  standardize_features=TRUE,        -- Normalizar features automáticamente
  kmeans_init_method='KMEANS++',    -- Método de inicialización
  max_iterations=20                 -- Máximo de iteraciones
) AS
SELECT
  dias_desde_ultima_compra,
  num_compras_6m,
  valor_compras_6m,
  ticket_promedio_6m,
  engagement_score,
  tendencia_valor_6m,
  coef_variacion_valor_6m,
  ratio_actividad_reciente,
  meses_desde_primera_compra
FROM `<PROJECT_ID>.ventas_retail_ml.datos_clustering`;


-- =====================================================
-- PASO 3: Asignar clusters a clientes
-- =====================================================
CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail_ml.clientes_segmentados` AS
SELECT
  d.cliente_id,
  d.dias_desde_ultima_compra,
  d.num_compras_6m,
  d.valor_compras_6m,
  d.engagement_score,
  
  -- Cluster asignado
  p.CENTROID_ID as cluster_id,
  
  -- Distancia al centroide (calidad de asignación)
  p.NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].CENTROID_ID as nearest_cluster,
  p.NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].DISTANCE as distance_to_centroid

FROM `<PROJECT_ID>.ventas_retail_ml.datos_clustering` d
LEFT JOIN ML.PREDICT(
  MODEL `<PROJECT_ID>.ventas_retail_ml.modelo_clustering_kmeans`,
  TABLE `<PROJECT_ID>.ventas_retail_ml.datos_clustering`
) p
ON d.cliente_id = p.cliente_id;


-- =====================================================
-- PASO 4: Caracterizar cada cluster
-- =====================================================
SELECT
  cluster_id,
  COUNT(*) as num_clientes,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje,
  
  -- Características promedio del cluster
  ROUND(AVG(dias_desde_ultima_compra), 1) as dias_ultima_compra_avg,
  ROUND(AVG(num_compras_6m), 2) as compras_6m_avg,
  ROUND(AVG(valor_compras_6m), 2) as valor_6m_avg,
  ROUND(AVG(engagement_score), 2) as engagement_avg,
  
  -- Interpretación del cluster (manual según resultados)
  CASE 
    WHEN RANK() OVER(ORDER BY AVG(engagement_score) DESC) = 1 THEN 'VIP / Champions'
    WHEN RANK() OVER(ORDER BY AVG(dias_desde_ultima_compra) DESC) = 1 THEN 'En Riesgo / Hibernating'
    WHEN RANK() OVER(ORDER BY AVG(num_compras_6m) DESC) BETWEEN 2 AND 3 THEN 'Activos / Loyal'
    ELSE 'Ocasionales / Potencial'
  END as nombre_segmento
  
FROM `<PROJECT_ID>.ventas_retail_ml.clientes_segmentados`
GROUP BY cluster_id
ORDER BY engagement_avg DESC;


-- =====================================================
-- Resultados esperados (aproximados):
-- =====================================================
-- Cluster 1: VIP/Champions (8-10%) - €3,000+/6m, 6+ compras, engagement alto
-- Cluster 2: Activos/Loyal (15-20%) - €1,500/6m, 2-3 compras, engagement medio-alto
-- Cluster 3: Ocasionales (40-50%) - €500-1,000/6m, 1-2 compras, engagement medio
-- Cluster 4: En Riesgo (20-30%) - €200/6m, <1 compra, engagement bajo
-- Cluster 5: Churned (5-10%) - >200 días sin comprar, engagement crítico
