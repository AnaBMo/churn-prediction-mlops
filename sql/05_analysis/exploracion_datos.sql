-- =====================================================
-- ANÁLISIS: Exploración de Datos
-- =====================================================
-- Descripción: Queries de análisis exploratorio para entender patrones
-- Input: gold_caracterizacion_mensual_clientes
-- =====================================================

-- =====================================================
-- QUERY 1: Churn por Perfil de Cliente
-- =====================================================
SELECT
  c.perfil,
  COUNT(*) as total_registros,
  SUM(g.fuga_combinada_1m) as casos_churn,
  ROUND(AVG(g.fuga_combinada_1m) * 100, 2) as tasa_churn_pct,
  
  -- Características promedio
  ROUND(AVG(g.dias_desde_ultima_compra), 1) as dias_ultima_compra_avg,
  ROUND(AVG(g.num_compras_6m), 2) as compras_6m_avg,
  ROUND(AVG(g.valor_compras_6m), 2) as valor_6m_avg,
  ROUND(AVG(g.engagement_score), 2) as engagement_avg

FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes` g
JOIN `<PROJECT_ID>.ventas_retail.silver_clientes` c
  ON g.cliente_id = c.cliente_id
WHERE g.fecha_referencia < (
  SELECT MAX(fecha_referencia) 
  FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
)
GROUP BY c.perfil
ORDER BY tasa_churn_pct DESC;


-- =====================================================
-- QUERY 2: Evolución Temporal del Churn
-- =====================================================
SELECT
  fecha_referencia,
  COUNT(DISTINCT cliente_id) as clientes_activos,
  SUM(fuga_combinada_1m) as casos_churn,
  ROUND(AVG(fuga_combinada_1m) * 100, 2) as tasa_churn_pct,
  ROUND(AVG(valor_compras_6m), 2) as valor_promedio_6m,
  ROUND(AVG(engagement_score), 2) as engagement_promedio

FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
WHERE fecha_referencia < (
  SELECT MAX(fecha_referencia) 
  FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
)
GROUP BY fecha_referencia
ORDER BY fecha_referencia;


-- =====================================================
-- QUERY 3: Segmentación RFM
-- =====================================================
WITH rfm_scores AS (
  SELECT
    cliente_id,
    dias_desde_ultima_compra,
    num_compras_6m,
    valor_compras_6m,
    fuga_combinada_1m,
    
    -- Calcular quintiles RFM
    NTILE(5) OVER (ORDER BY dias_desde_ultima_compra ASC) as recency_score,
    NTILE(5) OVER (ORDER BY num_compras_6m DESC) as frequency_score,
    NTILE(5) OVER (ORDER BY valor_compras_6m DESC) as monetary_score
    
  FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
  WHERE fecha_referencia = (
    SELECT MAX(fecha_referencia) 
    FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
    WHERE fecha_referencia < (
      SELECT MAX(fecha_referencia) 
      FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
    )
  )
)
SELECT
  recency_score,
  frequency_score,
  monetary_score,
  COUNT(*) as num_clientes,
  ROUND(AVG(fuga_combinada_1m) * 100, 2) as tasa_churn_pct,
  
  -- Clasificación del segmento
  CASE
    WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 
      THEN 'Champions'
    WHEN recency_score >= 4 AND frequency_score >= 3 
      THEN 'Loyal Customers'
    WHEN recency_score >= 4 
      THEN 'Recent Customers'
    WHEN frequency_score >= 4 AND monetary_score >= 4 
      THEN 'Big Spenders'
    WHEN recency_score <= 2 AND frequency_score <= 2 
      THEN 'At Risk'
    WHEN recency_score <= 2 
      THEN 'About to Sleep'
    ELSE 'Needs Attention'
  END as segmento_rfm

FROM rfm_scores
GROUP BY recency_score, frequency_score, monetary_score
HAVING COUNT(*) >= 10  -- Segmentos con al menos 10 clientes
ORDER BY num_clientes DESC
LIMIT 20;


-- =====================================================
-- QUERY 4: Comparación Churn vs No Churn
-- =====================================================
SELECT
  CASE WHEN fuga_combinada_1m = 1 THEN 'Churn' ELSE 'No Churn' END as tipo,
  COUNT(*) as num_casos,
  
  -- Métricas promedio
  ROUND(AVG(dias_desde_ultima_compra), 1) as dias_ultima_compra,
  ROUND(AVG(num_compras_6m), 2) as compras_6m,
  ROUND(AVG(valor_compras_6m), 2) as valor_6m,
  ROUND(AVG(engagement_score), 2) as engagement,
  ROUND(AVG(tendencia_valor_6m), 2) as tendencia_valor,
  ROUND(AVG(coef_variacion_valor_6m), 2) as coef_variacion,
  ROUND(AVG(ratio_actividad_reciente), 2) as ratio_actividad,
  ROUND(AVG(tasa_devolucion_mes) * 100, 2) as tasa_devolucion_pct

FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
WHERE fecha_referencia < (
  SELECT MAX(fecha_referencia) 
  FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
)
GROUP BY tipo
ORDER BY tipo;


-- =====================================================
-- QUERY 5: Estacionalidad y Churn
-- =====================================================
SELECT
  trimestre,
  mes_del_anio,
  CASE mes_del_anio
    WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
    WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
    WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
    WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
  END as nombre_mes,
  COUNT(*) as total_registros,
  ROUND(AVG(fuga_combinada_1m) * 100, 2) as tasa_churn_pct,
  ROUND(AVG(num_compras_6m), 2) as compras_6m_avg,
  ROUND(AVG(valor_compras_6m), 2) as valor_6m_avg

FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
WHERE fecha_referencia < (
  SELECT MAX(fecha_referencia) 
  FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
)
GROUP BY trimestre, mes_del_anio, nombre_mes
ORDER BY trimestre, mes_del_anio;
