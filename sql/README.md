# 📁 Queries SQL - Pipeline MLOps

Queries utilizadas para implementar la arquitectura Medallion y entrenar modelos de ML en BigQuery.

---

## 📂 Estructura

### 🥉 `01_bronze/` - Datos Crudos
- `create_datasets.sql` - Creación de datasets en BigQuery
- `load_tables.sql` - Carga de CSVs desde Cloud Storage

### 🥈 `02_silver/` - Transformaciones
- `silver_clientes.sql` - Limpieza y validación de clientes
- `silver_transacciones.sql` - Enriquecimiento de transacciones con features temporales
- `silver_devoluciones.sql` - Procesamiento de devoluciones

### 🥇 `03_gold/` - Feature Engineering
- `gold_caracterizacion_clientes.sql` - Generación de 22 features para ML (RFM, engagement, tendencias)

### 🤖 `04_ml/` - Machine Learning
- `train_churn_model.sql` - Entrenamiento de XGBoost (77.5% ROC-AUC)
- `train_clustering_model.sql` - Segmentación K-Means (5 clusters)
- `predict_churn.sql` - Predicciones sobre clientes actuales
- `metricas_modelo.sql` - Almacenamiento y consulta de métricas

### 📊 `05_analysis/` - Análisis Exploratorio
- `exploracion_datos.sql` - Análisis de patrones de churn
- `feature_importance.sql` - Importancia de variables del modelo

---

## 🎯 Resultados

**Modelo de Churn (XGBoost):**
- ROC-AUC: 77.5%
- F1-Score: 81.2%
- Precision: 81.7%
- Recall: 80.7%
- Features más importantes: recency (52 weight), estacionalidad (37 weight), frequency (22 weight)

**Segmentación (K-Means):**
- 5 segmentos identificados
- VIP (8.5%): €3,474/6m, engagement 5.11
- En Riesgo (31.2%): €168/6m, 203 días sin comprar

---

## 🔄 Orden de Ejecución

1. **Bronze:** `create_datasets.sql` → `load_tables.sql`
2. **Silver:** `silver_clientes.sql` → `silver_transacciones.sql` → `silver_devoluciones.sql`
3. **Gold:** `gold_caracterizacion_clientes.sql`
4. **ML:** `train_churn_model.sql` → `train_clustering_model.sql` → `predict_churn.sql` → `metricas_modelo.sql`
5. **Análisis (opcional):** `exploracion_datos.sql` → `feature_importance.sql`

---

## 💰 Tecnologías

- **BigQuery** - Almacenamiento, procesamiento y SQL
- **BigQuery ML** - Entrenamiento de modelos (XGBoost, K-Means)
- **Cloud Storage** - Datos crudos en CSV
- **Arquitectura Medallion** - Bronze → Silver → Gold

---

## 🔧 Configuración

Antes de ejecutar las queries, reemplaza los placeholders:
```sql
-- En todos los archivos .sql:
<PROJECT_ID>    → tu-proyecto-gcp-123
<BUCKET_NAME>   → tu-bucket-datos
```

**Ejemplo:**
```sql
-- Antes
`<PROJECT_ID>.ventas_retail.silver_clientes`
gs://<BUCKET_NAME>/bronze/clientes/

-- Después
`mi-proyecto-gcp-123.ventas_retail.silver_clientes`
gs://mi-bucket-datos/bronze/clientes/
```

---

## 📊 Datos Generados

| Layer | Tablas | Filas | Descripción |
|-------|--------|-------|-------------|
| Bronze | 3 | ~33K | Datos crudos sin transformar |
| Silver | 3 | ~33K | Datos limpios y validados |
| Gold | 1 | ~76K | Features ML (cliente × mes) |
| ML | 5 | ~5K | Modelos, predicciones, métricas |

---

## 🚀 Uso Rápido
```bash
# 1. Crear datasets
bq query < 01_bronze/create_datasets.sql

# 2. Cargar datos
bq query < 01_bronze/load_tables.sql

# 3. Ejecutar pipeline completo
for file in 02_silver/*.sql 03_gold/*.sql 04_ml/*.sql; do
  bq query < $file
done
```

---

## 📈 Insights Clave

### Feature Importance:
1. **dias_desde_ultima_compra** (52 weight) - Predictor #1
2. **mes_del_anio** (37 weight) - Estacionalidad crítica
3. **num_compras_6m** (22 weight) - Frequency

### Patrones de Churn:
- VIP: 18% churn vs Ocasionales: 76% churn
- Q4 (Oct-Dic): -15% churn vs Q1 (Ene-Mar)
- Clientes con churn: 112 días sin comprar vs 25 días (no-churn)

---

## 📝 Notas Técnicas

- **Región principal:** `europe-southwest1` (Madrid)
- **Región ML:** `EU` (requerido por BigQuery ML)
- **22 features** para modelo de churn
- **9 features** para clustering
- Modelos entrenados con **80/20 split** automático