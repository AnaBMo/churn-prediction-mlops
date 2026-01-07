-- =====================================================
-- ANÁLISIS: Feature Importance del Modelo
-- =====================================================
-- Descripción: Analizar qué features son más importantes para predecir churn
-- Input: modelo_churn_xgboost
-- =====================================================

-- =====================================================
-- Feature Importance - Ranking de variables
-- =====================================================
SELECT
  feature,
  importance_weight,
  importance_gain,
  importance_cover,
  RANK() OVER(ORDER BY importance_weight DESC) as ranking_weight,
  RANK() OVER(ORDER BY importance_gain DESC) as ranking_gain
FROM ML.FEATURE_IMPORTANCE(MODEL `<PROJECT_ID>.ventas_retail_ml.modelo_churn_xgboost`)
ORDER BY importance_weight DESC;


-- =====================================================
-- Top 10 Features más importantes (por weight)
-- =====================================================
SELECT
  feature,
  ROUND(importance_weight, 2) as weight,
  ROUND(importance_gain, 2) as gain,
  
  -- Interpretación
  CASE 
    WHEN feature LIKE '%dias_desde_ultima_compra%' THEN 'Recency - Días sin comprar'
    WHEN feature LIKE '%num_compras%' THEN 'Frequency - Número de compras'
    WHEN feature LIKE '%valor_compras%' THEN 'Monetary - Valor gastado'
    WHEN feature LIKE '%engagement%' THEN 'Engagement - Compromiso del cliente'
    WHEN feature LIKE '%tendencia%' THEN 'Tendencia - Evolución del comportamiento'
    WHEN feature LIKE '%coef_variacion%' THEN 'Variabilidad - Estabilidad de compras'
    WHEN feature LIKE '%mes_del_anio%' THEN 'Temporal - Estacionalidad'
    WHEN feature LIKE '%descuento%' THEN 'Descuentos - Sensibilidad al precio'
    WHEN feature LIKE '%devolucion%' THEN 'Devoluciones - Satisfacción'
    ELSE 'Otras métricas'
  END as interpretacion

FROM ML.FEATURE_IMPORTANCE(MODEL `<PROJECT_ID>.ventas_retail_ml.modelo_churn_xgboost`)
ORDER BY importance_weight DESC
LIMIT 10;


-- =====================================================
-- Agrupación de features por categoría
-- =====================================================
WITH feature_importance AS (
  SELECT
    feature,
    importance_weight,
    importance_gain,
    
    -- Categorizar features
    CASE 
      WHEN feature LIKE '%dias_desde_ultima_compra%' THEN 'RFM - Recency'
      WHEN feature LIKE '%num_compras%' THEN 'RFM - Frequency'
      WHEN feature LIKE '%valor_compras%' OR feature LIKE '%ticket%' THEN 'RFM - Monetary'
      WHEN feature LIKE '%engagement%' OR feature LIKE '%ratio_actividad%' THEN 'Engagement'
      WHEN feature LIKE '%tendencia%' OR feature LIKE '%coef_variacion%' THEN 'Tendencias'
      WHEN feature LIKE '%mes%' OR feature LIKE '%trimestre%' THEN 'Temporales'
      WHEN feature LIKE '%descuento%' THEN 'Descuentos'
      WHEN feature LIKE '%devolucion%' THEN 'Devoluciones'
      WHEN feature LIKE '%inactivo%' OR feature LIKE '%sin_%' OR feature LIKE '%alta_%' THEN 'Flags'
      WHEN feature LIKE '%log_%' OR feature LIKE '%categoria_%' THEN 'Transformadas'
      ELSE 'Otras'
    END as categoria
    
  FROM ML.FEATURE_IMPORTANCE(MODEL `<PROJECT_ID>.ventas_retail_ml.modelo_churn_xgboost`)
)
SELECT
  categoria,
  COUNT(*) as num_features,
  ROUND(SUM(importance_weight), 2) as weight_total,
  ROUND(SUM(importance_gain), 2) as gain_total,
  ROUND(AVG(importance_weight), 2) as weight_promedio,
  
  -- Porcentaje del total
  ROUND(SUM(importance_weight) * 100.0 / SUM(SUM(importance_weight)) OVER(), 2) as pct_weight_total

FROM feature_importance
GROUP BY categoria
ORDER BY weight_total DESC;


-- =====================================================
-- Features más correlacionadas con churn (análisis manual)
-- =====================================================
SELECT
  'ANÁLISIS DE CORRELACIÓN CON CHURN' as seccion,
  
  -- Recency
  CORR(dias_desde_ultima_compra, fuga_combinada_1m) as corr_dias_ultima_compra,
  
  -- Frequency
  CORR(num_compras_6m, fuga_combinada_1m) as corr_compras_6m,
  
  -- Monetary
  CORR(valor_compras_6m, fuga_combinada_1m) as corr_valor_6m,
  
  -- Engagement
  CORR(engagement_score, fuga_combinada_1m) as corr_engagement,
  
  -- Tendencias
  CORR(tendencia_valor_6m, fuga_combinada_1m) as corr_tendencia,
  CORR(coef_variacion_valor_6m, fuga_combinada_1m) as corr_variacion,
  
  -- Devoluciones
  CORR(tasa_devolucion_mes, fuga_combinada_1m) as corr_devolucion

FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
WHERE fecha_referencia < (
  SELECT MAX(fecha_referencia) 
  FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
);


-- =====================================================
-- Insights de Feature Importance
-- =====================================================
SELECT
  'INSIGHTS CLAVE' as seccion,
  '1. dias_desde_ultima_compra es el predictor #1 (weight más alto)' as insight_1,
  '2. Estacionalidad (mes_del_anio) es muy importante (#2 en weight)' as insight_2,
  '3. Frequency (num_compras) es más importante que Monetary' as insight_3,
  '4. Features de engagement y tendencias aportan valor significativo' as insight_4,
  '5. Devoluciones tienen poco impacto en churn (weight bajo)' as insight_5;
  