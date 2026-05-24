# Resumen Técnico — Proyecto Data Lake Mercado Público Chile

## Objetivo del Proyecto

Construir una plataforma analítica profesional para el procesamiento, validación, almacenamiento y explotación de datos históricos de Mercado Público Chile.

El objetivo final es:

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

# Estado Actual del Proyecto

## Estructura del Proyecto

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

# Stack Tecnológico

| Herramienta | Uso |
|---|---|
| Python | Orquestación ETL |
| Polars | Procesamiento masivo eficiente |
| DuckDB | Motor analítico SQL |
| PyArrow | Manejo parquet/schema |
| Apache Parquet | Almacenamiento columnar |
| Power BI | Visualización |
| VS Code | Desarrollo |

---

# Inventario de Datos

## 1. Licitaciones

Ruta:

```text
/data/raw/licitaciones/
```

Contenido:
- Archivos ZIP mensuales
- Período: 2022–2026
- Algunos CSV manualmente extraídos

Ejemplos:

```text
2022-1.zip
2023-10.zip
2024-6.csv
2025-12.zip
```

---

## 2. Órdenes de Compra

Ruta:

```text
/data/raw/ordenes_compra/
```

Contenido:
- ZIP mensuales
- Período: 2022–2026
- Algunos CSV extraídos manualmente

---

## 3. Oferentes

Ruta:

```text
/data/raw/oferentes/
```

Archivos:

```text
oferentes_completos_2022_2026.csv
oferentes_perdedores_completos.csv
```

Uso esperado:
- dimensión proveedores
- análisis competitivo
- scoring proveedores
- joins históricos

---

## 4. Datos SII

Ruta:

```text
/data/raw/sii/
```

Archivos:

```text
nomina_sii.txt
nomina_sii_actividades.txt
nomina_sii_razon_social.txt
```

Uso esperado:
- dimensión empresas
- actividades económicas
- razones sociales
- enriquecimiento tributario

---

# Hallazgos Técnicos Importantes

## 1. Schema Drift Histórico Detectado

Se detectó evolución estructural entre años.

Cantidad de columnas identificadas:

| Dataset | Columnas |
|---|---|
| 2022 | ~183 |
| 2025 | ~189 |
| 2026 | ~188 |

Conclusión:
- Mercado Público modifica schemas históricamente
- existen columnas nuevas/eliminadas
- no es posible concatenar datasets directamente sin normalización

---

# 2. Problemas de Inferencia Automática

Se detectó que la inferencia automática de tipos produce errores.

Ejemplos:

```text
596061,6
17,5
0,529
6e+05
NA
```

Problemas observados:
- decimales con coma
- notación científica
- valores especiales
- mezcla texto/número
- columnas numéricas inconsistentes

Conclusión:

## NO usar inferencia automática de schema.

---

# 3. Encodings Inconsistentes

Encodings detectados:

- UTF-8-SIG
- Windows-1252
- iso8859-15
- utf-8

Conclusión:
- requiere normalización controlada
- no asumir UTF-8 puro

---

# 4. TXT SII de Gran Tamaño

Archivo detectado:

```text
nomina_sii.txt ≈ 1.5 GB
```

Problemas:
- filas irregulares
- delimitadores inconsistentes
- schema variable

Conclusión:
- evitar pandas completo en memoria
- usar streaming/lazy processing

---

# 5. Columnas Relevantes Detectadas

## Tributarias

- PorcentajeIva
- Impuestos
- Cargos
- Descuentos
- totalImpuestos
- totalDescuentos

## Ambientales / Sociales

- CriteriosAmbientales
- DescripcionCriteriosAmbientales
- CriteriosSociales
- DescripcionCriteriosSociales

## Compras Públicas

- CodigoLicitacion
- CodigoUnidadCompra
- OrganismoPublico
- MontoTotalOC
- FechaAceptacion
- FechaEnvio
- EsCompraAgil
- EsTratoDirecto

## Proveedores

- ActividadProveedor
- ComunaProveedor
- PaisProveedor

---

# Problemas Técnicos que se Están Abordando

## 1. Evitar pérdida de datos

Problema:
- NULL inesperados
- columnas perdidas
- coerción incorrecta

Estrategia:
- staging parquet lossless
- todo inicialmente como string

---

## 2. Estandarización de schemas

Problema:
- columnas distintas entre años

Estrategia:
- schema maestro canónico
- normalización histórica

---

## 3. Optimización memoria/RAM

Problema:
- millones de filas
- cientos de columnas
- TXT gigantes

Estrategia:
- Polars Lazy
- parquet particionado
- DuckDB sobre parquet
- evitar concat gigantes

---

## 4. Rendimiento Power BI

Problema:
- CSV gigantes son lentos

Estrategia:

```text
CSV → Parquet → DuckDB → Power BI
```

---

# Arquitectura Objetivo

## Capas del Lakehouse

### RAW

Datos originales inmutables.

```text
/data/raw
```

---

### STAGING

Objetivo:
- parquet seguro
- todo string
- sin pérdida
- sin tipificación

```text
/data/staging
```

---

### NORMALIZED

Objetivo:
- limpieza
- tipificación controlada
- normalización decimal
- fechas
- columnas duplicadas

---

### WAREHOUSE

Modelo analítico final.

Tablas esperadas:

```text
fact_licitaciones
fact_ordenes_compra
dim_oferentes
dim_sii_empresas
dim_fecha
```

---

# Estrategia Técnica Adoptada

## Decisión Crítica

Todos los CSV serán leídos inicialmente como:

```text
STRING
```

No se realizará inferencia automática de tipos.

---

# Estrategia Parquet

Formato:

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
- evita importar DB física
- consultas rápidas
- bajo consumo RAM
- integración Power BI

---

# Scripts Implementados

## 01_profile_raw_data.py

Objetivo:
- profiling general
- detección encoding
- detección errores
- inspección archivos

Salida:

```text
/data/logs/profile_raw_data_*.csv
```

---

## 02_extract_schemas.py

Objetivo:
- extracción columnas
- detección schema drift
- auditoría estructural

Salida:

```text
/data/schemas/all_schemas.csv
```

---

## 03_raw_to_staging_parquet.py

Objetivo:
- conversión segura RAW → Parquet
- preservación lossless
- staging inicial

Características:
- todo string
- ignore_errors
- truncate_ragged_lines
- parquet zstd

---

# Próximas Etapas

## ETAPA 1

Conversión RAW → STAGING parquet.

---

## ETAPA 2

Normalización:
- fechas
- montos
- IVA
- decimales
- nulls
- columnas históricas

---

## ETAPA 3

Construcción Warehouse:
- tablas fact
- dimensiones
- relaciones
- claves negocio

---

## ETAPA 4

DuckDB:
- vistas analíticas
- métricas
- agregaciones
- consultas optimizadas

---

## ETAPA 5

Power BI:
- dashboards
- KPIs
- análisis temporal
- proveedores
- organismos públicos
- comportamiento licitaciones

---

# Principios Técnicos Definidos

## Reglas del Proyecto

- RAW nunca se modifica
- no usar Excel
- no inferir tipos automáticamente
- staging debe ser lossless
- parquet como estándar analítico
- evitar cargar datasets completos en RAM
- usar procesamiento incremental
- priorizar reproducibilidad

---

# Estado General

El proyecto ya se encuentra en una fase de:

```text
Data Engineering + Analytics Engineering
```

No corresponde a un flujo simple de análisis CSV.

Se está construyendo una plataforma analítica escalable sobre datos históricos de Mercado Público Chile.

