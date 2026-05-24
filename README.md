Mercado Público Analytics Chile

Proyecto de análisis de datos enfocado en licitaciones públicas y comportamiento de mercado utilizando datos abiertos de Chile.

Objetivo del proyecto

Construir una plataforma de análisis orientada a Business Intelligence para explorar patrones de mercado, validar hipótesis de negocio y generar insights accionables a partir de datos públicos relacionados con compras y proveedores en Chile.

El proyecto busca responder preguntas como:

¿Qué organismos realizan mayores compras?
¿Qué categorías presentan más oportunidades de negocio?
¿Qué proveedores dominan ciertos mercados?
¿Existen patrones regionales o temporales en las licitaciones?
¿Qué tendencias pueden transformarse en oportunidades comerciales?
Fuentes de datos

Los datos utilizados provienen de fuentes públicas oficiales:

API de Mercado Público
CSV históricos de ChileCompra
Nóminas y registros del SII
Tecnologías utilizadas
Python
SQL
DuckDB
Polars
Parquet
Power BI
Pipeline de datos
Extracción

Obtención de datos desde:

API de Mercado Público
archivos CSV masivos
registros públicos complementarios
Transformación

Procesamiento y limpieza utilizando:

Polars para manipulación eficiente de grandes volúmenes de datos
DuckDB para consultas analíticas
almacenamiento en formato Parquet
Modelado

Construcción de datasets analíticos optimizados para:

análisis exploratorio
validación de hipótesis
visualización en Power BI
Visualización

Exportación mediante formato ODBC hacia Power BI para construcción de dashboards e indicadores de negocio.

Arquitectura general
Fuentes de datos
    ↓
Extracción Python
    ↓
Transformación con Polars
    ↓
Almacenamiento Parquet
    ↓
Consultas analíticas con DuckDB
    ↓
Conexión ODBC
    ↓
Power BI
Objetivos analíticos
Análisis de mercado y tendencias
Identificación de oportunidades comerciales
Segmentación de proveedores
Análisis de comportamiento de compra pública
Generación de KPIs
Detección de patrones y anomalías
Validación de hipótesis de negocio
Estado actual

Proyecto en desarrollo.

Actualmente se encuentra implementado:

pipeline ETL inicial
procesamiento de datos
normalización
consultas analíticas
preparación de datasets para visualización

Próximamente:

dashboards Power BI
KPIs interactivos
análisis temporal y regional
automatización del pipeline
Próximos pasos
Construcción de dashboards ejecutivos
Modelos de análisis por industria
Automatización de actualización de datos
Integración de más fuentes públicas
Detección de oportunidades mediante análisis exploratorio
Autor

Proyecto personal desarrollado como práctica aplicada de Data Analytics y Business Intelligence utilizando datos reales del ecosistema público chileno.
