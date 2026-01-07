# 🎯 Predicción de Churn de Clientes - Proyecto MLOps End-to-End

[![GitHub Pages](https://img.shields.io/badge/GitHub%20Pages-Live-success)](https://anabmo.github.io/churn-prediction-mlops/)
[![BigQuery ML](https://img.shields.io/badge/BigQuery-ML-blue)](https://www.skills.google/course_templates/626?catalog_rank=%7B%22rank%22%3A5%2C%22num_filters%22%3A0%2C%22has_search%22%3Atrue%7D&search_id=66735415)
[![Python](https://img.shields.io/badge/Python-3.10-green)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Proyecto MLOps completo** para predecir la fuga de clientes (churn) y segmentarlos usando BigQuery ML, XGBoost, K-Means y Looker Studio.

🌐 **[Ver Proyecto en Vivo](https://anabmo.github.io/churn-prediction-mlops/)**

---

## 📋 Tabla de Contenidos

- [Resumen Ejecutivo](#-resumen-ejecutivo)
- [Arquitectura](#-arquitectura)
- [Datos](#-datos)
- [Modelos de ML](#-modelos-de-ml)
- [Resultados](#-resultados)
- [Tecnologías](#-tecnologías)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Cómo Reproducir](#-cómo-reproducir)
- [Dashboards](#-dashboards)
- [Contacto](#-contacto)

---

## 🎯 Resumen Ejecutivo

### Objetivo
Desarrollar un sistema end-to-end de predicción de churn y segmentación de clientes para el sector retail tecnológico, implementando mejores prácticas de MLOps y arquitectura de datos moderna.

### Resultados Clave
- **🤖 Modelo XGBoost:** 77.5% ROC-AUC, 81.2% F1-Score
- **📊 Segmentación K-Means:** 5 segmentos diferenciados (VIP, Activos, Ocasionales, En Riesgo)
- **📈 Insights accionables:** Identificados 979 clientes en alto riesgo, oportunidad de €60K en win-back
- **💰 Coste:** $0 (100% en free tier de GCP)

---

## 🏗️ Arquitectura

### Arquitectura Medallion (Bronze → Silver → Gold)

```
📊 Cloud Storage (Bronze)
    └─ Datos crudos generados (CSV)
         ↓
🥉 BigQuery - Bronze Layer
    └─ 3 tablas (33K filas)
    └─ bronze_clientes, bronze_transacciones, bronze_devoluciones
         ↓
🥈 BigQuery - Silver Layer
    └─ Limpieza y validaciones
    └─ silver_clientes, silver_transacciones, silver_devoluciones
         ↓
🥇 BigQuery - Gold Layer
    └─ Features para ML (76K filas, 22 features)
    └─ gold_caracterizacion_mensual_clientes
         ↓
🤖 Modelos ML (BigQuery ML)
    ├─ modelo_churn_xgboost (BOOSTED_TREE_CLASSIFIER)
    └─ modelo_clustering_kmeans (KMEANS, 5 clusters)
         ↓
📈 Dashboards (Looker Studio)
    ├─ Análisis Exploratorio
    ├─ Modelo de Churn
    └─ Segmentación de Clientes
```

### Stack Tecnológico

| Componente | Tecnología | Propósito |
|------------|------------|-----------|
| **Almacenamiento** | Google Cloud Storage | Datos crudos (Bronze) |
| **Data Warehouse** | Google BigQuery | Procesamiento y almacenamiento |
| **ML Training** | BigQuery ML | XGBoost + K-Means |
| **Orquestación** | Python + Colab | Generación de datos |
| **Visualización** | Looker Studio | Dashboards interactivos |
| **Versionado** | GitHub | Control de versiones |
| **Deployment** | GitHub Pages | Hosting de la web |

---

## 📊 Datos

### Generación de Datos Sintéticos

Se generaron **datos ficticios realistas** para simular un negocio retail tecnológico:

- **5,000 clientes** con 4 perfiles: VIP (10%), Activo (40%), Ocasional (30%), Churned (20%)
- **26,674 transacciones** distribuidas en 24 meses (2023-2024)
- **1,333 devoluciones** (~5% tasa)
- **Estacionalidad realista:** Picos en Black Friday, Navidad, rebajas
- **Comportamiento de churn:** Patrones de inactividad progresiva

### Características del Dataset

```
📊 Volumen de Datos:
   └─ Bronze: 33,007 filas
   └─ Silver: 33,007 filas (validadas)
   └─ Gold: 76,545 filas (cliente x mes)

💰 Métricas de Negocio:
   └─ Ventas totales: €13.4M (2 años)
   └─ Ticket promedio: €504
   └─ Clientes activos: 4,921

📈 Variables:
   └─ RFM: Recency, Frequency, Monetary
   └─ Comportamiento: Engagement, tendencias, variabilidad
   └─ Temporales: Mes, trimestre, antigüedad
   └─ Devoluciones: Tasa, importe
```

---

## 🤖 Modelos de ML

### 1. Modelo de Predicción de Churn (XGBoost)

**Algoritmo:** BOOSTED_TREE_CLASSIFIER (BigQuery ML)

**Features:**
- RFM básicas: días_desde_ultima_compra, num_compras_6m, valor_compras_6m
- Features sofisticadas: engagement_score, tendencia_valor_6m, coef_variacion_valor_6m
- Temporales: mes_del_anio, trimestre, meses_desde_primera_compra
- Transformadas: log_dias_ultima_compra, categoria_frecuencia
- Flags: inactivo_3m, sin_compras_6m, alta_devolucion

**Hiperparámetros:**
```sql
OPTIONS(
  model_type='BOOSTED_TREE_CLASSIFIER',
  auto_class_weights=TRUE,
  max_tree_depth=4,
  subsample=0.8,
  l1_reg=0.001,
  l2_reg=0.1
)
```

**Resultados:**

| Métrica | Valor | Interpretación |
|---------|-------|----------------|
| **ROC-AUC** | 0.775 | Muy buena capacidad de discriminación |
| **Precision** | 0.817 | 82% de predicciones positivas correctas |
| **Recall** | 0.807 | Detecta 81% de los casos reales de churn |
| **F1-Score** | 0.812 | Excelente balance precision/recall |
| **Accuracy** | 0.743 | 74% de aciertos totales |

**Feature Importance (Top 5):**
1. `dias_desde_ultima_compra` (weight: 52) - Factor crítico
2. `mes_del_anio` (weight: 36) - Estacionalidad fuerte
3. `num_compras_6m` (weight: 22) - Frecuencia clave
4. `num_compras_3m` (weight: 17) - Actividad reciente
5. `promedio_descuento_mes` (weight: 16) - Influencia en lealtad

---

### 2. Modelo de Segmentación (K-Means)

**Algoritmo:** KMEANS con 5 clusters

**Features utilizadas:**
- días_desde_ultima_compra
- num_compras_6m
- valor_compras_6m
- engagement_score
- tendencia_valor_6m

**Segmentos Identificados:**

| Cluster | Nombre | % Clientes | Características | Acción Recomendada |
|---------|--------|------------|-----------------|-------------------|
| **3** |  VIP/Champions | 8.5% | €3,474/6m, 6.75 compras | Programa VIP exclusivo |
| **4** |  Activos/Loyal | 15.3% | €1,609/6m, 2.5 compras | Cross-sell, convertir a VIP |
| **2** |  Ocasionales | 20.3% | €1,349/6m, 2.24 compras | Campañas de activación |
| **5** |  Ocasionales Bajo Valor | 24.6% | €536/6m, 1.36 compras | Promociones agresivas |
| **1** |  En Riesgo | 31.2% | €168/6m, 0.55 compras | Win-back urgente |

---

## 📈 Resultados

### Insights de Negocio

#### 1. Predicción de Churn
- **56.3% de clientes** predichos con churn en el próximo mes
- **979 clientes** en nivel de riesgo ALTO/MUY_ALTO requieren atención inmediata
- **Patrón clave:** Clientes con >90 días sin comprar tienen 92% probabilidad de churn

#### 2. Segmentación
- **31% de clientes en riesgo** (1,521 clientes) - Oportunidad: recuperar 20% = +€60K/6m
- **8.5% VIP generan 30% de ingresos** - Crítico retenerlos
- **Valor por segmento:**
  - VIP: €1.4M
  - Activos: €1.2M
  - Ocasionales: €1.3M
  - Bajo valor: €0.6M
  - En riesgo: €0.3M

#### 3. Recomendaciones Accionables

**🔴 Urgente (Q1 2025):**
1. Campaña win-back para 1,521 clientes en riesgo
2. Alertas automáticas a los 60-90 días sin compra
3. Programa VIP diferenciado para retener 417 clientes top

**🟡 Mediano plazo (Q2-Q3):**
1. Campañas de activación en meses de bajo engagement (Q1-Q3)
2. Promociones personalizadas por segmento
3. Cross-sell a clientes activos para convertir a VIP

**🟢 Largo plazo:**
1. Modelo de LTV (Lifetime Value) por segmento
2. Sistema de recomendación personalizado
3. A/B testing de estrategias de retención

---

## 🛠️ Tecnologías

### Cloud & Data
- **Google Cloud Platform (GCP):** Infraestructura completa
- **BigQuery:** Data Warehouse + ML nativo
- **Cloud Storage:** Almacenamiento de datos crudos
- **BigQuery ML:** Entrenamiento de modelos (XGBoost, K-Means)

### Lenguajes & Tools
- **SQL:** Transformaciones y feature engineering
- **Python:** Generación de datos sintéticos
- **Looker Studio:** Dashboards interactivos

### DevOps & Deployment
- **GitHub:** Control de versiones
- **GitHub Pages:** Hosting de la web del proyecto
- **Colab:** Notebooks para generación de datos

---

## 📁 Estructura del Proyecto

```
churn-prediction-mlops/
│
├── index.html                          
├── README.md                           
│
├── notebooks/
│   └── generar_datos_ventas_demo.ipynb # Generación de datos sintéticos
│
├── sql/
│   ├── 01_bronze/
│   │   ├── create_dataset.sql
│   │   └── load_tables.sql
│   │
│   ├── 02_silver/
│   │   ├── silver_clientes.sql
│   │   ├── silver_transacciones.sql
│   │   └── silver_devoluciones.sql
│   │
│   ├── 03_gold/
│   │   └── gold_caracterizacion_clientes.sql
│   │
│   ├── 04_ml/
│   │   ├── train_churn_model.sql
│   │   ├── train_clustering_model.sql
│   │   ├── predict_churn.sql
│   │   └── metricas_modelo.sql
│   │
│   └── 05_analysis/
│       ├── exploracion_datos.sql
│       └── feature_importance.sql
│
└── docs/
    ├── arquitectura.md
    └── insights_negocio.md
```

---

## 🚀 Cómo Reproducir

### Prerrequisitos
- Cuenta de Google Cloud (Free Tier)
- Python 3.10+
- Google Colab (opcional)

### Paso 1: Clonar el repositorio
```bash
git clone https://github.com/AnaBMo/churn-prediction-mlops.git
cd churn-prediction-mlops
```

### Paso 2: Generar datos sintéticos
1. Abre el notebook `notebooks/generar_datos_ventas_demo.ipynb` en Colab
2. Actualiza `PROJECT_ID` con tu ID de proyecto GCP
3. Ejecuta todas las celdas
4. Los datos se subirán automáticamente a Cloud Storage

### Paso 3: Crear infraestructura en BigQuery

#### Crear datasets:
```sql
-- Dataset principal (región europe-southwest1)
CREATE SCHEMA IF NOT EXISTS `tu-project-id.ventas_retail`
OPTIONS(location='europe-southwest1');

-- Dataset para ML (región EU multi-región)
CREATE SCHEMA IF NOT EXISTS `tu-project-id.ventas_retail_ml`
OPTIONS(location='EU');
```

#### Cargar datos (Bronze):
Ejecuta los scripts en `sql/01_bronze/` en orden.

#### Transformar datos (Silver):
Ejecuta los scripts en `sql/02_silver/` en orden.

#### Crear features (Gold):
Ejecuta `sql/03_gold/gold_caracterizacion_clientes.sql`.

### Paso 4: Entrenar modelos
Ejecuta los scripts en `sql/04_ml/`:
1. `train_churn_model.sql` (~2-4 min)
2. `train_clustering_model.sql` (~1-2 min)
3. `predict_churn.sql`
4. `metricas_modelo.sql`

### Paso 5: Crear dashboards en Looker Studio
1. Conecta Looker Studio a BigQuery
2. Usa las tablas:
   - `silver_transacciones`
   - `predicciones_churn`
   - `clientes_segmentados`
3. Replica los diseños de los dashboards públicos

---

## 📊 Dashboards

### Dashboard 1: Análisis Exploratorio
📊 [Ver Dashboard](https://lookerstudio.google.com/s/lLTLuQyYick)

**Contenido:**
- KPIs principales (ventas, transacciones, clientes, ticket medio)
- Evolución temporal de ventas
- Distribución de clientes por perfil
- Ventas por trimestre (estacionalidad)
- Canales de venta
- Top 10 mejores meses

---

### Dashboard 2: Modelo de Churn
🤖 [Ver Dashboard](https://lookerstudio.google.com/s/nC2zyX5c9J0)

**Contenido:**
- Métricas del modelo (ROC-AUC, F1, Precision, Recall)
- Distribución de predicciones (Churn vs No Churn)
- Clientes por nivel de riesgo
- Distribución de probabilidades
- Top 20 clientes en mayor riesgo
- Comparativa: Características Churn vs No Churn

---

### Dashboard 3: Segmentación
👥 [Ver Dashboard](https://lookerstudio.google.com/s/vLntPlwT_uk)

**Contenido:**
- Tabla de características por segmento
- Distribución de clientes (donut chart)
- Engagement score por segmento
- Scatter plot: Valor vs Frecuencia
- Insights y recomendaciones por segmento

---

## 📧 Contacto

- Portfolio: [anabmo.github.io/churn-prediction-mlops](https://anabmo.github.io/churn-prediction-mlops/)
- LinkedIn: [linkedin.com/in/anabmo](www.linkedin.com/in/anabmo)
- GitHub: [@AnaBMo](https://github.com/AnaBMo)

---

## 📄 Licencia

Este proyecto es de código abierto bajo la licencia MIT.

**Nota:** Los datos utilizados son 100% sintéticos y generados para fines educativos. No contienen información real de clientes ni empresas.

---

**Desarrollado con ❤️ por Ana Morales | Enero 2025**