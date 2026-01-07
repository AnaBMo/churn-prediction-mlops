-- =====================================================
-- ML: Hacer Predicciones de Churn
-- =====================================================
-- Descripción: Aplicar modelo entrenado para identificar clientes en riesgo
-- Input: modelo_churn_xgboost + gold_caracterizacion_mensual_clientes
-- Output: predicciones_churn
-- =====================================================

-- =====================================================
-- Preparar datos del mes actual para scoring
-- =====================================================
CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail_ml.clientes_scoring_actual` AS
SELECT
  cliente_id,
  fecha_referencia,
  
  -- Features (mismas que entrenamiento)
  COALESCE(dias_desde_ultima_compra, 365) as dias_desde_ultima_compra,
  COALESCE(num_compras_3m, 0) as num_compras_3m,
  COALESCE(num_compras_6m, 0) as num_compras_6m,
  COALESCE(valor_compras_3m, 0) as valor_compras_3m,
  COALESCE(valor_compras_6m, 0) as valor_compras_6m,
  COALESCE(ticket_promedio_6m, 0) as ticket_promedio_6m,
  COALESCE(tendencia_valor_6m, 0) as tendencia_valor_6m,
  COALESCE(coef_variacion_valor_6m, 0) as coef_variacion_valor_6m,
  COALESCE(engagement_score, 0) as engagement_score,
  COALESCE(ratio_actividad_reciente, 0) as ratio_actividad_reciente,
  COALESCE(tasa_devolucion_mes, 0) as tasa_devolucion_mes,
  mes_del_anio,
  trimestre,
  COALESCE(meses_desde_primera_compra, 0) as meses_desde_primera_compra,
  COALESCE(promedio_descuento_mes, 0) as promedio_descuento_mes,
  LOG(GREATEST(dias_desde_ultima_compra, 1)) as log_dias_ultima_compra,
  LOG(GREATEST(valor_compras_6m, 1)) as log_valor_compras,
  CASE 
    WHEN num_compras_6m = 0 THEN 0
    WHEN num_compras_6m <= 2 THEN 1 
    WHEN num_compras_6m <= 5 THEN 2
    ELSE 3 
  END as categoria_frecuencia,
  CASE WHEN dias_desde_ultima_compra > 90 THEN 1 ELSE 0 END as inactivo_3m,
  CASE WHEN num_compras_6m = 0 THEN 1 ELSE 0 END as sin_compras_6m,
  CASE WHEN valor_compras_6m = 0 THEN 1 ELSE 0 END as sin_gasto_6m,
  CASE WHEN tasa_devolucion_mes > 0.2 THEN 1 ELSE 0 END as alta_devolucion

FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
WHERE 
  -- Penúltimo mes disponible (tiene datos completos)
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
-- Hacer predicciones con el modelo
-- =====================================================
CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail_ml.predicciones_churn` AS
SELECT
  s.cliente_id,
  s.fecha_referencia,
  s.dias_desde_ultima_compra,
  s.num_compras_6m,
  s.valor_compras_6m,
  s.engagement_score,
  
  -- Predicciones del modelo
  p.predicted_fuga_combinada_1m as prediccion_churn,
  p.predicted_fuga_combinada_1m_probs[OFFSET(1)].prob as probabilidad_churn,
  
  -- Clasificación de riesgo
  CASE 
    WHEN p.predicted_fuga_combinada_1m_probs[OFFSET(1)].prob >= 0.8 THEN 'MUY_ALTO'
    WHEN p.predicted_fuga_combinada_1m_probs[OFFSET(1)].prob >= 0.6 THEN 'ALTO'
    WHEN p.predicted_fuga_combinada_1m_probs[OFFSET(1)].prob >= 0.4 THEN 'MEDIO'
    WHEN p.predicted_fuga_combinada_1m_probs[OFFSET(1)].prob >= 0.2 THEN 'BAJO'
    ELSE 'MUY_BAJO'
  END as nivel_riesgo

FROM `<PROJECT_ID>.ventas_retail_ml.clientes_scoring_actual` s
LEFT JOIN ML.PREDICT(
  MODEL `<PROJECT_ID>.ventas_retail_ml.modelo_churn_xgboost`,
  TABLE `<PROJECT_ID>.ventas_retail_ml.clientes_scoring_actual`
) p
ON s.cliente_id = p.cliente_id;


-- =====================================================
-- Resumen de predicciones
-- =====================================================
SELECT
  'RESUMEN DE PREDICCIONES' as seccion,
  COUNT(*) as total_clientes_evaluados,
  SUM(CASE WHEN prediccion_churn = 1 THEN 1 ELSE 0 END) as predichos_churn,
  ROUND(AVG(CASE WHEN prediccion_churn = 1 THEN 1 ELSE 0 END) * 100, 2) as tasa_churn_predicha_pct,
  ROUND(AVG(probabilidad_churn) * 100, 2) as probabilidad_churn_promedio_pct,
  
  -- Por nivel de riesgo
  SUM(CASE WHEN nivel_riesgo = 'MUY_ALTO' THEN 1 ELSE 0 END) as clientes_muy_alto_riesgo,
  SUM(CASE WHEN nivel_riesgo = 'ALTO' THEN 1 ELSE 0 END) as clientes_alto_riesgo,
  SUM(CASE WHEN nivel_riesgo = 'MEDIO' THEN 1 ELSE 0 END) as clientes_medio_riesgo,
  SUM(CASE WHEN nivel_riesgo = 'BAJO' THEN 1 ELSE 0 END) as clientes_bajo_riesgo,
  SUM(CASE WHEN nivel_riesgo = 'MUY_BAJO' THEN 1 ELSE 0 END) as clientes_muy_bajo_riesgo
  
FROM `<PROJECT_ID>.ventas_retail_ml.predicciones_churn`;


-- =====================================================
-- Top 20 clientes con mayor riesgo de churn
-- =====================================================
SELECT
  cliente_id,
  ROUND(probabilidad_churn * 100, 2) as prob_churn_pct,
  nivel_riesgo,
  dias_desde_ultima_compra,
  num_compras_6m,
  ROUND(valor_compras_6m, 2) as valor_6m,
  ROUND(engagement_score, 2) as engagement
FROM `<PROJECT_ID>.ventas_retail_ml.predicciones_churn`
WHERE prediccion_churn = 1
ORDER BY probabilidad_churn DESC
LIMIT 20;


-- =====================================================
-- Clientes de alto valor en riesgo (priorizar retención)
-- =====================================================
SELECT
  cliente_id,
  ROUND(probabilidad_churn * 100, 2) as prob_churn_pct,
  ROUND(valor_compras_6m, 2) as valor_6m,
  num_compras_6m,
  dias_desde_ultima_compra
FROM `<PROJECT_ID>.ventas_retail_ml.predicciones_churn`
WHERE 
  prediccion_churn = 1
  AND nivel_riesgo IN ('MUY_ALTO', 'ALTO')
  AND valor_compras_6m > 1000  -- Alto valor
ORDER BY valor_compras_6m DESC
LIMIT 50;
