-- =====================================================
-- ML: Modelo de Predicción de Churn (XGBoost)
-- =====================================================
-- Descripción: Entrenamiento de modelo XGBoost para predecir churn
-- Input: gold_caracterizacion_mensual_clientes
-- Output: modelo_churn_xgboost
-- Tiempo estimado: 2-4 minutos
-- =====================================================

-- =====================================================
-- PASO 1: Preparar datos de entrenamiento
-- =====================================================
CREATE OR REPLACE TABLE `<PROJECT_ID>.ventas_retail_ml.datos_entrenamiento_churn` AS
SELECT
  cliente_id,
  fecha_referencia,
  
  -- Features RFM básicas
  COALESCE(dias_desde_ultima_compra, 365) as dias_desde_ultima_compra,
  COALESCE(num_compras_3m, 0) as num_compras_3m,
  COALESCE(num_compras_6m, 0) as num_compras_6m,
  COALESCE(valor_compras_3m, 0) as valor_compras_3m,
  COALESCE(valor_compras_6m, 0) as valor_compras_6m,
  COALESCE(ticket_promedio_6m, 0) as ticket_promedio_6m,
  
  -- Features sofisticadas
  COALESCE(tendencia_valor_6m, 0) as tendencia_valor_6m,
  COALESCE(coef_variacion_valor_6m, 0) as coef_variacion_valor_6m,
  COALESCE(engagement_score, 0) as engagement_score,
  COALESCE(ratio_actividad_reciente, 0) as ratio_actividad_reciente,
  
  -- Devoluciones
  COALESCE(tasa_devolucion_mes, 0) as tasa_devolucion_mes,
  
  -- Temporales
  mes_del_anio,
  trimestre,
  COALESCE(meses_desde_primera_compra, 0) as meses_desde_primera_compra,
  
  -- Descuentos
  COALESCE(promedio_descuento_mes, 0) as promedio_descuento_mes,
  
  -- Features transformadas
  LOG(GREATEST(dias_desde_ultima_compra, 1)) as log_dias_ultima_compra,
  LOG(GREATEST(valor_compras_6m, 1)) as log_valor_compras,
  
  -- Categorías derivadas
  CASE 
    WHEN num_compras_6m = 0 THEN 0
    WHEN num_compras_6m <= 2 THEN 1 
    WHEN num_compras_6m <= 5 THEN 2
    ELSE 3 
  END as categoria_frecuencia,
  
  -- Flags binarias
  CASE WHEN dias_desde_ultima_compra > 90 THEN 1 ELSE 0 END as inactivo_3m,
  CASE WHEN num_compras_6m = 0 THEN 1 ELSE 0 END as sin_compras_6m,
  CASE WHEN valor_compras_6m = 0 THEN 1 ELSE 0 END as sin_gasto_6m,
  CASE WHEN tasa_devolucion_mes > 0.2 THEN 1 ELSE 0 END as alta_devolucion,
  
  -- Variable objetivo
  fuga_combinada_1m

FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`
WHERE 
  -- Filtrar último mes (no tiene target válido)
  fecha_referencia < (SELECT MAX(fecha_referencia) FROM `<PROJECT_ID>.ventas_retail_ml.gold_caracterizacion_mensual_clientes`)
  -- Filtrar clientes muy inactivos (ruido)
  AND dias_desde_ultima_compra <= 730;


-- =====================================================
-- PASO 2: Entrenar modelo XGBoost
-- =====================================================
CREATE OR REPLACE MODEL `<PROJECT_ID>.ventas_retail_ml.modelo_churn_xgboost`
OPTIONS(
  model_type='BOOSTED_TREE_CLASSIFIER',
  input_label_cols=['fuga_combinada_1m'],
  
  -- Parámetros optimizados para churn
  auto_class_weights=TRUE,        -- Balancear clases automáticamente
  max_tree_depth=4,                -- Profundidad del árbol
  num_parallel_tree=1,             -- Boosting (no random forest)
  subsample=0.8,                   -- Muestra de datos por árbol
  l1_reg=0.001,                    -- Regularización L1
  l2_reg=0.1,                      -- Regularización L2
  min_tree_child_weight=1,         -- Peso mínimo por hoja
  tree_method='HIST',              -- Método de construcción
  early_stop=TRUE,                 -- Parar early si no mejora
  min_rel_progress=0.01,           -- Progreso mínimo para continuar
  
  -- Data split
  data_split_method='RANDOM',      -- Split aleatorio
  data_split_eval_fraction=0.2     -- 80% train, 20% validation
  
) AS
SELECT
  -- Features para el modelo (22 features)
  dias_desde_ultima_compra,
  num_compras_3m,
  num_compras_6m,
  valor_compras_3m,
  valor_compras_6m,
  ticket_promedio_6m,
  tendencia_valor_6m,
  coef_variacion_valor_6m,
  engagement_score,
  ratio_actividad_reciente,
  tasa_devolucion_mes,
  mes_del_anio,
  trimestre,
  meses_desde_primera_compra,
  promedio_descuento_mes,
  log_dias_ultima_compra,
  log_valor_compras,
  categoria_frecuencia,
  inactivo_3m,
  sin_compras_6m,
  sin_gasto_6m,
  alta_devolucion,
  
  -- Variable objetivo
  fuga_combinada_1m

FROM `<PROJECT_ID>.ventas_retail_ml.datos_entrenamiento_churn`;


-- =====================================================
-- PASO 3: Evaluar el modelo
-- =====================================================
SELECT
  'EVALUACIÓN DEL MODELO CHURN' as seccion,
  *
FROM ML.EVALUATE(MODEL `<PROJECT_ID>.ventas_retail_ml.modelo_churn_xgboost`);


-- =====================================================
-- Resultados esperados:
-- =====================================================
-- ROC-AUC: ~0.77-0.78 (Muy buena capacidad predictiva)
-- Precision: ~0.81-0.82 (Alta confianza en predicciones positivas)
-- Recall: ~0.80-0.81 (Detecta 8 de cada 10 casos de churn)
-- F1-Score: ~0.81-0.82 (Excelente balance)
-- Accuracy: ~0.74 (74% de aciertos totales)
