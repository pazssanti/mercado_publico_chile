# Data Lake Mercado Público Chile

Plataforma analítica orientada a Data Engineering, Analytics Engineering y Business Intelligence utilizando datos históricos de Mercado Público Chile, ChileCompra y registros públicos complementarios.

El proyecto busca construir un lakehouse analítico escalable para procesamiento, validación, modelado y explotación de grandes volúmenes de datos públicos.

---

# Objetivo del Proyecto

Construir una arquitectura de datos moderna para:

* procesar datasets históricos masivos,
* validar y normalizar información pública,
* analizar comportamiento de compras públicas,
* explorar tendencias de mercado,
* validar hipótesis de negocio,
* generar KPIs y dashboards analíticos,
* identificar oportunidades comerciales y patrones relevantes.

---

# Arquitectura General

```text
RAW CSV/ZIP/TXT
    ↓
Staging Parquet Seguro
    ↓
Normalización y Validación
    ↓
Warehouse Analítico
    ↓
DuckDB
    ↓
Power BI / Analytics / ML
```

---

# Stack Tecnológico

| Herramienta    | Uso                            |
| -------------- | ------------------------------ |
| Python         | Orquestación ETL               |
| Polars         | Procesamiento masivo eficiente |
| DuckDB         | Motor analítico SQL            |
| PyArrow        | Manejo parquet/schema          |
| Apache Parquet | Almacenamiento columnar        |
| Power BI       | Visualización                  |
| VS Code        | Desarrollo                     |

---

# Fuentes de Datos

## Mercado Público

Datos históricos de:

* licitaciones,
* órdenes de compra,
* proveedores,
* organismos públicos.

Formatos:

* CSV
* ZIP
* API

Período trabajado:

* 2022–2026

---

## ChileCompra

Archivos históricos masivos utilizados para:

* análisis de compras públicas,
* comportamiento de mercado,
* evolución histórica de licitaciones.

---

## Servicio de Impuestos Internos (SII)

Datos utilizados para enriquecimiento empresarial:

* razón social,
* actividades económicas,
* clasificación tributaria,
* análisis de proveedores.

---

# Objetivos Analíticos

El proyecto busca permitir:

* análisis de mercado,
* segmentación de proveedores,
* análisis de comportamiento de compra pública,
* análisis temporal y regional,
* validación de hipótesis de negocio,
* generación de KPIs,
* detección de anomalías,
* identificación de oportunidades comerciales.

---

# Estructura del Proyecto

```text
b2b_mercadopublico/
│
├── data/
│   ├── raw/
│   ├── staging/
│   ├── warehouse/
│   ├── logs/
│   ├── schemas/
│   └── exports/
│
├── scripts/
├── notebooks/
├── sql/
└── README.md
```

---

# Pipeline de Datos

## 1. Extracción

Obtención de datos desde:

* API Mercado Público,
* archivos históricos CSV/ZIP,
* registros públicos del SII.

---

## 2. Staging

Conversión segura de archivos RAW hacia formato Parquet.

Características:

* preservación lossless,
* almacenamiento columnar,
* compresión zstd,
* procesamiento incremental.

---

## 3. Normalización

Procesos orientados a:

* limpieza de datos,
* tipificación controlada,
* normalización decimal,
* estandarización histórica,
* manejo de schema drift.

---

## 4. Warehouse Analítico

Construcción de tablas analíticas para:

* reporting,
* dashboards,
* consultas SQL,
* análisis exploratorio.

Tablas objetivo:

```text
fact_licitaciones
fact_ordenes_compra
dim_oferentes
dim_sii_empresas
dim_fecha
```

---

## 5. Analytics & BI

Consumo analítico mediante:

* DuckDB,
* Power BI,
* consultas SQL,
* dashboards interactivos.

---

# Hallazgos Técnicos Relevantes

## Schema Drift Histórico

Se detectaron diferencias estructurales entre años históricos.

Ejemplo:

* columnas agregadas,
* columnas eliminadas,
* cambios de formato.

Conclusión:

* no es posible concatenar datasets directamente,
* se requiere normalización histórica controlada.

---

## Problemas de Inferencia Automática

Se detectaron inconsistencias como:

* decimales con coma,
* notación científica,
* mezcla texto/número,
* valores especiales,
* NULL inconsistentes.

Por esta razón:

```text
Todos los datos son leídos inicialmente como STRING.
```

---

## Encodings Inconsistentes

Encodings detectados:

* UTF-8
* UTF-8-SIG
* Windows-1252
* ISO-8859-15

Se implementó normalización controlada para evitar corrupción de datos.

---

## Optimización de Rendimiento

El proyecto prioriza:

* bajo consumo RAM,
* procesamiento incremental,
* consultas sobre parquet,
* evitar cargas masivas en memoria.

Tecnologías utilizadas:

* Polars Lazy
* DuckDB
* Apache Parquet

---

# Estrategia Técnica

## Principios del Proyecto

* RAW nunca se modifica
* no usar inferencia automática de tipos
* staging debe ser lossless
* parquet como estándar analítico
* evitar cargar datasets completos en RAM
* priorizar reproducibilidad
* procesamiento incremental

---

# Estrategia Parquet

Formato utilizado:

```text
Parquet + compresión zstd
```

Particionado esperado:

```text
warehouse/
    licitaciones/
        year=2024/
            month=06/
```

---

# Estrategia DuckDB

Uso esperado:

```sql
SELECT *
FROM parquet_scan('warehouse/**/*.parquet')
```

Ventajas:

* consultas rápidas,
* bajo consumo RAM,
* integración eficiente con Power BI,
* arquitectura lightweight.

---

# Scripts Implementados

## 01_profile_raw_data.py

Objetivo:

* profiling general,
* detección de encoding,
* auditoría de archivos,
* inspección estructural.

---

## 02_extract_schemas.py

Objetivo:

* extracción de columnas,
* detección de schema drift,
* análisis estructural histórico.

---

## 03_raw_to_staging_parquet.py

Objetivo:

* conversión segura RAW → Parquet,
* preservación lossless,
* staging inicial.

Características:

* todo string,
* ignore_errors,
* truncate_ragged_lines,
* parquet zstd.

---

# Estado Actual

Actualmente implementado:

* pipeline ETL inicial,
* profiling de datos,
* extracción de schemas,
* procesamiento RAW → Parquet,
* validación estructural,
* consultas analíticas iniciales,
* preparación para visualización en Power BI.

---

# Próximas Etapas

* normalización histórica,
* modelado dimensional,
* dashboards Power BI,
* KPIs analíticos,
* automatización del pipeline,
* análisis predictivo y exploratorio,
* métricas de mercado y proveedores.

---

# Estado General del Proyecto

El proyecto se encuentra actualmente en una etapa de:

```text
Data Engineering + Analytics Engineering
```

No corresponde a un análisis simple de CSV, sino a la construcción de una plataforma analítica escalable sobre datos históricos de Mercado Público Chile.

---

# Autor

Proyecto personal desarrollado como práctica aplicada de:

* Data Analytics
* Business Intelligence
* Data Engineering
* Analytics Engineering

utilizando datos públicos reales del ecosistema chileno.
