-- =====================================================
-- ML: Guardar y Consultar Métricas del Modelo
-- =====================================================
-- Descripción: Almacenar métricas de evaluación del modelo para tracking
-- Input: modelo_churn_xgboost (evaluación)
-- Output: metricas_modelo_churn
-- =====================================================

-- =====================================================
-- Guardar métricas del modelo en tabla permanente
-- =====================================================
CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail_ml.metricas_modelo_churn` AS
SELECT
  'modelo_churn_xgboost' as modelo_id,
  CURRENT_TIMESTAMP() as fecha_evaluacion,
  precision,
  recall,
  accuracy,
  f1_score,
  log_loss,
  roc_auc
FROM ML.EVALUATE(MODEL `<PROJECT_ID>.ventas_retail_ml.modelo_churn_xgboost`);


-- =====================================================
-- Consultar métricas almacenadas
-- =====================================================
SELECT 
  modelo_id,
  fecha_evaluacion,
  ROUND(roc_auc, 4) as roc_auc,
  ROUND(f1_score, 4) as f1_score,
  ROUND(precision, 4) as precision,
  ROUND(recall, 4) as recall,
  ROUND(accuracy, 4) as accuracy,
  ROUND(log_loss, 4) as log_loss
FROM `<PROJECT_ID>.ventas_retail_ml.metricas_modelo_churn`
ORDER BY fecha_evaluacion DESC;


-- =====================================================
-- Interpretación de métricas
-- =====================================================
SELECT
  'INTERPRETACIÓN DE MÉTRICAS' as seccion,
  
  -- ROC-AUC
  CASE
    WHEN roc_auc >= 0.9 THEN 'Excelente (>0.90)'
    WHEN roc_auc >= 0.8 THEN 'Muy Bueno (0.80-0.90)'
    WHEN roc_auc >= 0.7 THEN 'Bueno (0.70-0.80)'
    WHEN roc_auc >= 0.6 THEN 'Aceptable (0.60-0.70)'
    ELSE 'Mejorable (<0.60)'
  END as evaluacion_roc_auc,
  
  -- F1-Score
  CASE
    WHEN f1_score >= 0.9 THEN 'Excelente (>0.90)'
    WHEN f1_score >= 0.8 THEN 'Muy Bueno (0.80-0.90)'
    WHEN f1_score >= 0.7 THEN 'Bueno (0.70-0.80)'
    ELSE 'Mejorable (<0.70)'
  END as evaluacion_f1_score,
  
  -- Precision
  CASE
    WHEN precision >= 0.9 THEN 'Excelente (>0.90)'
    WHEN precision >= 0.8 THEN 'Muy Bueno (0.80-0.90)'
    WHEN precision >= 0.7 THEN 'Bueno (0.70-0.80)'
    ELSE 'Mejorable (<0.70)'
  END as evaluacion_precision,
  
  -- Recall
  CASE
    WHEN recall >= 0.9 THEN 'Excelente (>0.90)'
    WHEN recall >= 0.8 THEN 'Muy Bueno (0.80-0.90)'
    WHEN recall >= 0.7 THEN 'Bueno (0.70-0.80)'
    ELSE 'Mejorable (<0.70)'
  END as evaluacion_recall

FROM `<PROJECT_ID>.ventas_retail_ml.metricas_modelo_churn`
ORDER BY fecha_evaluacion DESC
LIMIT 1;


-- =====================================================
-- Información del modelo
-- =====================================================
SELECT
  model_type,
  creation_time,
  location,
  ARRAY_LENGTH(training_run) as num_training_runs,
  (SELECT input_feature_columns FROM UNNEST(training_run) LIMIT 1) as features_usadas
FROM `<PROJECT_ID>.ventas_retail_ml.INFORMATION_SCHEMA.MODELS`
WHERE model_name = 'modelo_churn_xgboost';
