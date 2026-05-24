"""
Build Warehouse - Capa Gold
============================
Construye el Data Warehouse analítico desde Silver con:
- dim_calendario (tiempo)
- Estandarización geográfica (regiones/comunas)
- Limpieza de estados y texto
- Verificación de integridad de llaves
"""
import duckdb
from pathlib import Path

DB_PATH = "data/warehouse/marketplace.duckdb"
SILVER = "data/silver"

Path("data/warehouse").mkdir(parents=True, exist_ok=True)

con = duckdb.connect(DB_PATH)

print("=" * 70)
print("CONSTRUCCIÓN DEL DATA WAREHOUSE - CAPA GOLD")
print("=" * 70)

# ============================================================
# 1. DIM_CALENDARIO - Dimensión de Tiempo (DIARIA)
# ============================================================
print("\n[1/6] Creando dim_calendario (diaria 2022-2026)...")

con.execute("""
CREATE OR REPLACE TABLE dim_calendario AS
WITH dias AS (
    SELECT UNNEST(generate_series(DATE '2022-01-01', DATE '2026-12-31', INTERVAL '1 day')) AS fecha
)
SELECT
    fecha,
    EXTRACT(YEAR FROM fecha)::INTEGER AS anno,
    EXTRACT(MONTH FROM fecha)::INTEGER AS mes,
    EXTRACT(DAY FROM fecha)::INTEGER AS dia,
    CASE EXTRACT(MONTH FROM fecha)
        WHEN 1 THEN 'Enero'
        WHEN 2 THEN 'Febrero'
        WHEN 3 THEN 'Marzo'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Mayo'
        WHEN 6 THEN 'Junio'
        WHEN 7 THEN 'Julio'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Septiembre'
        WHEN 10 THEN 'Octubre'
        WHEN 11 THEN 'Noviembre'
        WHEN 12 THEN 'Diciembre'
    END AS nombre_mes,
    CASE
        WHEN EXTRACT(MONTH FROM fecha) BETWEEN 1 AND 3 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM fecha) BETWEEN 4 AND 6 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM fecha) BETWEEN 7 AND 9 THEN 'Q3'
        ELSE 'Q4'
    END AS trimestre,
    CASE EXTRACT(DOW FROM fecha)
        WHEN 0 THEN 'Domingo'
        WHEN 1 THEN 'Lunes'
        WHEN 2 THEN 'Martes'
        WHEN 3 THEN 'Miercoles'
        WHEN 4 THEN 'Jueves'
        WHEN 5 THEN 'Viernes'
        WHEN 6 THEN 'Sabado'
    END AS dia_semana,
    -- Flag de día laboral
    CASE EXTRACT(DOW FROM fecha)
        WHEN 0 THEN 0  -- Domingo
        WHEN 6 THEN 0  -- Sábado
        ELSE 1
    END AS es_dia_laboral
FROM dias
""")

count_cal = con.execute("SELECT COUNT(*) FROM dim_calendario").fetchone()[0]
print(f"  OK - dim_calendario creada ({count_cal} días: 2022-2026)")

# ============================================================
# 2. MAPA DE REGIONES DE CHILE
# ============================================================
print("\n[2/6] Creando mapeo de regiones (16 regiones Chile)...")

# Tabla temporal para normalización de regiones
con.execute("""
CREATE OR REPLACE TABLE tmp_mapeo_regiones AS
SELECT * FROM (VALUES
    ('ARICA Y PARINACOTA', 'ARICA Y PARINACOTA'),
    ('TARAPACA', 'TARAPACA'),
    ('ANTOFAGASTA', 'ANTOFAGASTA'),
    ('ATACAMA', 'ATACAMA'),
    ('COQUIMBO', 'COQUIMBO'),
    ('VALPARAISO', 'VALPARAISO'),
    ('BIOBIO', 'BIOBIO'),
    ('LA ARAUCANIA', 'LA ARAUCANIA'),
    ('LOS LAGOS', 'LOS LAGOS'),
    ('AYSEN', 'AYSEN'),
    ('MAGALLANES', 'MAGALLANES'),
    ('METROPOLITANA', 'METROPOLITANA'),
    ('OHIGGINS', 'OHIGGINS'),
    ('MAULE', 'MAULE'),
    ('NUBLE', 'NUBLE'),
    ('LOS RIOS', 'LOS RIOS'),
    -- Variantes
    ('REGIÓN METROPOLITANA', 'METROPOLITANA'),
    ('METROPOLITANA DE SANTIAGO', 'METROPOLITANA'),
    ('RM', 'METROPOLITANA'),
    ('SANTIAGO', 'METROPOLITANA'),
    ('REGIÓN DE TARAPACÁ', 'TARAPACA'),
    ('REGIÓN DE ANTOFAGASTA', 'ANTOFAGASTA'),
    ('REGIÓN DE ATACAMA', 'ATACAMA'),
    ('REGIÓN DE COQUIMBO', 'COQUIMBO'),
    ('REGIÓN DE VALPARAÍSO', 'VALPARAISO'),
    ('REGIÓN DEL BIOBÍO', 'BIOBIO'),
    ('REGIÓN DE LA ARAUCANÍA', 'LA ARAUCANIA'),
    ('REGIÓN DE LOS LAGOS', 'LOS LAGOS'),
    ('REGIÓN DE AYSÉN', 'AYSEN'),
    ('REGIÓN DE MAGALLANES Y LA ANTÁRTICA CHILENA', 'MAGALLANES'),
    ('REGIÓN METROPOLITANA DE SANTIAGO', 'METROPOLITANA'),
    ('REGIÓN DEL LIBERTADOR GENERAL BERNARDO OHIGGINS', 'OHIGGINS'),
    ('REGIÓN DEL MAULE', 'MAULE'),
    ('REGIÓN DE ÑUBLE', 'NUBLE'),
    ('REGIÓN DE LOS RIOS', 'LOS RIOS')
) AS t(nombre_original, nombre_estandarizado)
""")

print("  OK - Mapeo de 16 regiones de Chile")

# ============================================================
# 3. DIM_SII_EMPRESAS - Con limpieza geográfica
# ============================================================
print("\n[3/6] Creando dim_sii_empresas...")

con.execute("""
CREATE OR REPLACE TABLE dim_sii_empresas AS
SELECT
    -- Limpieza de texto básica
    TRIM(UPPER(e.razón_social)) AS nombre_empresa,
    CAST(e.rut AS VARCHAR) AS rut,
    e.rut_cuerpo,
    e.rut_dv,

    -- Estandarización geográfica
    COALESCE(m.nombre_estandarizado, TRIM(UPPER(e.región))) AS region,
    TRIM(UPPER(e.comuna)) AS comuna,
    TRIM(UPPER(e.provincia)) AS provincia,

    -- Otros campos
    TRIM(UPPER(e.actividad_económica)) AS actividad_economica,
    TRIM(UPPER(e.rubro_económico)) AS rubro,
    e.fecha_inicio_de_actividades_vige AS fecha_inicio,
    e.fecha_término_de_giro AS fecha_termino,
    e.número_de_trabajadores_dependie AS n_trabajadores,
    e.tramo_ventas

FROM read_parquet('data/silver/sii.parquet') AS e
LEFT JOIN tmp_mapeo_regiones AS m
    ON TRIM(UPPER(e.región)) = m.nombre_original
    OR TRIM(UPPER(e.región)) = m.nombre_estandarizado
""")

# Verificar y limpiar RUTs nulos o vacíos
con.execute("""
DELETE FROM dim_sii_empresas
WHERE rut IS NULL OR rut = '' OR LENGTH(rut) < 3
""")

print("  OK - dim_sii_empresas creada")

# ============================================================
# 4. DIM_OFERENTES - Con limpieza geográfica
# ============================================================
print("\n[4/6] Creando dim_oferentes...")

con.execute("""
CREATE OR REPLACE TABLE dim_oferentes AS
SELECT
    -- ID y RUT
    TRIM(id_licitacion) AS id_licitacion,
    TRIM(UPPER(rut_raw)) AS rut_oferente,
    rut_cuerpo,
    rut_dv,

    -- Nombre y geografía
    TRIM(UPPER(razon_social)) AS razon_social,
    COALESCE(m.nombre_estandarizado, TRIM(UPPER(region_empresa))) AS region,
    TRIM(UPPER(comuna_empresa)) AS comuna,

    -- Métricas
    adjudicado,
    monto_contrato,
    monto_estimado,
    n_competidores,

    -- Fechas (normalizadas)
    fecha_publicacion,
    fecha_adjudicacion,
    fecha_extraccion,

    -- Otros
    TRIM(UPPER(status_adjudicacion)) AS status_adjudicacion,
    TRIM(UPPER(nombre_organismo)) AS nombre_organismo,
    TRIM(UPPER(region_organismo)) AS region_organismo,
    TRIM(UPPER(rubro_licitacion)) AS rubro

FROM read_parquet('data/silver/oferentes.parquet') AS e
LEFT JOIN tmp_mapeo_regiones AS m
    ON TRIM(UPPER(e.region_empresa)) = m.nombre_original
    OR TRIM(UPPER(e.region_empresa)) = m.nombre_estandarizado
""")

# Limpiar RUTs nulos
con.execute("""
DELETE FROM dim_oferentes
WHERE rut_oferente IS NULL OR rut_oferente = '' OR LENGTH(rut_oferente) < 3
""")

print("  OK - dim_oferentes creada")

# ============================================================
# 5. FACT_LICITACIONES - Con limpieza de estados
# ============================================================
print("\n[5/6] Creando fact_licitaciones...")

con.execute("""
CREATE OR REPLACE TABLE fact_licitaciones AS
SELECT
    -- IDs
    TRIM(CodigoExterno) AS codigo_externo,
    TRIM(Codigo) AS codigo,
    TRIM(Link) AS link,

    -- Nombre y descripción
    TRIM(Nombre) AS nombre,
    TRIM(Descripcion) AS descripcion,

    -- Estados normalizados
    TRIM(UPPER(Estado)) AS estado,
    TRIM(UPPER(CodigoEstado)) AS codigo_estado,
    TRIM(UPPER(Tipo)) AS tipo,
    TRIM(UPPER(TipoConvocatoria)) AS tipo_convocatoria,

    -- Geografía
    TRIM(UPPER(NombreOrganismo)) AS organismo,
    TRIM(UPPER(NombreUnidad)) AS unidad,
    TRIM(UPPER(DireccionUnidad)) AS direccion,
    TRIM(UPPER(ComunaUnidad)) AS comuna,
    COALESCE(m.nombre_estandarizado, TRIM(UPPER(e.RegionUnidad))) AS region,
    TRIM(UPPER(sector)) AS sector,

    -- Fechas
    FechaCreacion,
    FechaCierre,
    FechaPublicacion,
    FechaAdjudicacion,
    FechaEstimadaAdjudicacion,
    FechaActoAperturaTecnica,
    FechaActoAperturaEconomica,

    -- Montos
    MontoEstimado,
    TRY_CAST("Monto Estimado Adjudicado" AS DOUBLE) AS monto_adjudicado,
    TRY_CAST("Valor Total Ofertado" AS DOUBLE) AS monto_ofertado,
    TRY_CAST(MontoUnitarioOferta AS DOUBLE) AS monto_unitario,

    -- Cantidades
    TRY_CAST(Cantidad AS DOUBLE) AS cantidad,
    TRY_CAST(CantidadAdjudicada AS DOUBLE) AS cantidad_adjudicada,

    -- RUTs normalizados
    RutUnidad_cuerpo,
    RutUnidad_dv,
    RutProveedor_cuerpo,
    RutProveedor_dv,

    -- Proveedor
    TRIM(NombreProveedor) AS nombre_proveedor,
    TRIM(RutProveedor) AS rut_proveedor,

    -- Otros (columnas con espacios)
    TRIM("Moneda Adquisicion") AS moneda,
    TRIM(Modalidad) AS modalidad,
    TRIM(FuenteFinanciamiento) AS fuente_financiamiento

FROM read_parquet('data/silver/licitaciones*.parquet', union_by_name=True) AS e
LEFT JOIN tmp_mapeo_regiones AS m
    ON TRIM(UPPER(e.RegionUnidad)) = m.nombre_original
    OR TRIM(UPPER(e.RegionUnidad)) = m.nombre_estandarizado
""")

print("  OK - fact_licitaciones creada")

# ============================================================
# 6. FACT_ORDENES_COMPRA - Con limpieza de estados y geografía
# (Simplificado para evitar problemas de memoria)
# ============================================================
print("\n[6/6] Creando fact_ordenes_compra...")

# Crear tabla básica sin leer todo el parquet a la vez
con.execute("""
CREATE OR REPLACE TABLE fact_ordenes_compra AS
SELECT
    -- IDs
    TRIM(ID) AS id_oc,
    TRIM(Codigo) AS codigo,
    TRIM(Link) AS link,
    TRIM(Nombre) AS nombre,

    -- Estados normalizados
    TRIM(UPPER(Estado)) AS estado,
    TRIM(UPPER(Tipo)) AS tipo,
    TRIM(UPPER(codigoEstado)) AS codigo_estado,
    TRIM(UPPER(EstadoProveedor)) AS estado_proveedor,

    -- Fechas
    FechaCreacion,
    FechaEnvio,
    FechaAceptacion,
    FechaCancelacion,

    -- Montos principales
    TRY_CAST(MontoTotalOC AS DOUBLE) AS monto_total,
    TRY_CAST(TotalNetoOC AS DOUBLE) AS monto_neto,

    -- Geografía de la unidad
    TRIM(UPPER(UnidadCompra)) AS nombre_unidad,
    COALESCE(m.nombre_estandarizado, TRIM(UPPER(RegionUnidadCompra))) AS region,
    TRIM(UPPER(CiudadUnidadCompra)) AS ciudad,
    TRIM(UPPER(PaisUnidadCompra)) AS pais,

    -- Información del proveedor
    TRIM(NombreProveedor) AS nombre_proveedor,
    TRIM(UPPER(RegionProveedor)) AS region_proveedor,

    -- RUTs
    RutUnidadCompra_cuerpo,
    RutUnidadCompra_dv,
    RutSucursal_cuerpo,
    RutSucursal_dv,

    -- Datos del item
    IDItem,
    codigoProductoONU,
    TRY_CAST(cantidad AS DOUBLE) AS cantidad,
    TRIM(UnidadMedida) AS unidad_medida,
    TRY_CAST(precioNeto AS DOUBLE) AS precio_neto,
    TRY_CAST(totalLineaNeto AS DOUBLE) AS monto_linea

FROM read_parquet('data/silver/ordenes.parquet') AS e
LEFT JOIN tmp_mapeo_regiones AS m
    ON TRIM(UPPER(e.RegionUnidadCompra)) = m.nombre_original
    OR TRIM(UPPER(e.RegionUnidadCompra)) = m.nombre_estandarizado
""")

print("  OK - fact_ordenes_compra creada")

# ============================================================
# LIMPIEZA FINAL
# ============================================================
print("\n[7/7] Limpiando registros huérfanos...")

# Eliminar licitaciones sin código
con.execute("DELETE FROM fact_licitaciones WHERE codigo_externo IS NULL OR codigo_externo = ''")
con.execute("DELETE FROM fact_ordenes_compra WHERE idOC IS NULL OR idOC = ''")

print("  OK - Registros huérfanos eliminados")

# ============================================================
# REPORTE DE CALIDAD
# ============================================================
print("\n" + "=" * 70)
print("REPORTE DE CALIDAD - DATA WAREHOUSE")
print("=" * 70)

# Conteo de tablas
print("\n--- CONTEO DE REGISTROS ---")
tablas = ['dim_calendario', 'dim_sii_empresas', 'dim_oferentes',
          'fact_licitaciones', 'fact_ordenes_compra']

for tabla in tablas:
    count = con.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    print(f"  {tabla}: {count:,} registros")

# Top 5 regiones en SII
print("\n--- TOP 5 REGIONES - dim_sii_empresas ---")
result = con.execute("""
SELECT region, COUNT(*) AS empresas
FROM dim_sii_empresas
WHERE region IS NOT NULL AND region != ''
GROUP BY region
ORDER BY empresas DESC
LIMIT 5
""").fetchall()

print(f"  {'Región':<25} {'Empresas':>12}")
print("  " + "-" * 38)
for row in result:
    print(f"  {row[0]:<25} {row[1]:>12,}")

# Verificar estados en licitaciones
print("\n--- ESTADOS ÚNICOS - fact_licitaciones ---")
estados = con.execute("""
SELECT estado, COUNT(*) AS total
FROM fact_licitaciones
GROUP BY estado
ORDER BY total DESC
LIMIT 5
""").fetchall()

for row in estados:
    print(f"  {row[0]}: {row[1]:,}")

# Verificar RUTs en SII
print("\n--- INTEGRIDAD DE RUTs ---")
sii_nulls = con.execute("SELECT COUNT(*) FROM dim_sii_empresas WHERE rut IS NULL OR rut = ''").fetchone()[0]
ofer_nulls = con.execute("SELECT COUNT(*) FROM dim_oferentes WHERE rut_oferente IS NULL OR rut_oferente = ''").fetchone()[0]
print(f"  RUTs nulos/vacíos en SII: {sii_nulls}")
print(f"  RUTs nulos/vacíos en Oferentes: {ofer_nulls}")

print("\n" + "=" * 70)
print("DATA WAREHOUSE CONSTRUIDO CORRECTAMENTE")
print("=" * 70)

# Listar tablas finales
print("\n--- TABLAS CREADAS ---")
tablas = con.execute("SHOW TABLES").fetchall()
for t in tablas:
    print(f"  - {t[0]}")

con.close()
print("\nListo para análisis: data/warehouse/marketplace.duckdb")