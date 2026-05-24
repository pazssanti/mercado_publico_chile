"""
Build Warehouse - Capa Gold (VERSION SEGURA)
============================================
Construye el Data Warehouse analítico desde Silver con:
- Límites estrictos de memoria para evitar colapsos
- Creación secuencial con COMMITs explícitos
- Estrategia: SELECT directo desde Parquet Silver (pre-limpio)
"""
import duckdb
from pathlib import Path
import gc

DB_PATH = "data/warehouse/marketplace.duckdb"
SILVER = "data/silver"

Path("data/warehouse").mkdir(parents=True, exist_ok=True)

# ============================================================
# CONEXIÓN CON BLINDAJE DE MEMORIA EXTREMO
# ============================================================
print("Estableciendo conexión con límites de memoria...")

con = duckdb.connect(DB_PATH)

# PRAGMAS DE MEMORIA - CRÍTICOS PARA ENTORNOS LIMITADOS
con.execute("PRAGMA memory_limit='4GB';")
con.execute("PRAGMA max_memory='4GB';")
con.execute("PRAGMA threads=2;")  # Reducir hilos para menor consumo
con.execute("PRAGMA temp_directory='data/warehouse/.duckdb_tmp';")

print("  [*] memory_limit=4GB")
print("  [*] max_memory=4GB")
print("  [*] threads=2")
print("  [*] temp_directory=.duckdb_tmp (disco)")

print("\n" + "=" * 70)
print("CONSTRUCCIÓN DEL DATA WAREHOUSE - CAPA GOLD (SEGURA)")
print("=" * 70)

# ============================================================
# DETECTAR TABLAS EXISTENTES (CARGA INCREMENTAL)
# ============================================================
existing_tables = []
try:
    existing_tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
except:
    pass

print(f"\nTablas detectadas en BD: {existing_tables}")

# Tablas que YA están procesadas (no recrear)
COMPLETED_TABLES = ['dim_calendario', 'dim_sii_empresas', 'dim_oferentes', 'fact_licitaciones']

# ============================================================
# 1. DIM_CALENDARIO - SKIP SI EXISTE
# ============================================================
if 'dim_calendario' in existing_tables:
    count_cal = con.execute("SELECT COUNT(*) FROM dim_calendario").fetchone()[0]
    print(f"\n[1/6] SKIP - dim_calendario ya existe ({count_cal} dias)")
else:
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
        CASE EXTRACT(DOW FROM fecha)
            WHEN 0 THEN 0
            WHEN 6 THEN 0
            ELSE 1
        END AS es_dia_laboral
    FROM dias
    """)
    count_cal = con.execute("SELECT COUNT(*) FROM dim_calendario").fetchone()[0]
    print(f"  OK - dim_calendario ({count_cal} dias)")
    con.execute("CHECKPOINT;")
    gc.collect()

# ============================================================
# 2. MAPA DE REGIONES DE CHILE - SIEMPRE CREAR (es auxiliar)
# ============================================================
print("\n[2/6] Creando mapeo de regiones...")

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
    ('REGIÓN METROPOLITANA', 'METROPOLITANA'),
    ('METROPOLITANA DE SANTIAGO', 'METROPOLITANA'),
    ('RM', 'METROPOLITANA'),
    ('SANTIAGO', 'METROPOLITANA')
) AS t(nombre_original, nombre_estandarizado)
""")

print("  OK - Mapeo de regiones")
con.execute("CHECKPOINT;")
gc.collect()

# ============================================================
# 3. DIM_SII_EMPRESAS - SKIP SI EXISTE
# ============================================================
if 'dim_sii_empresas' in existing_tables:
    count_sii = con.execute("SELECT COUNT(*) FROM dim_sii_empresas").fetchone()[0]
    print(f"\n[3/6] SKIP - dim_sii_empresas ya existe ({count_sii:,} empresas)")
else:
    print("\n[3/6] Creando dim_sii_empresas...")
    con.execute("""
    CREATE OR REPLACE TABLE dim_sii_empresas AS
    SELECT
        TRIM(UPPER(razón_social)) AS nombre_empresa,
        CAST(rut AS VARCHAR) AS rut,
        rut_cuerpo,
        rut_dv,
        COALESCE(m.nombre_estandarizado, TRIM(UPPER(región))) AS region,
        TRIM(UPPER(comuna)) AS comuna,
        TRIM(UPPER(provincia)) AS provincia,
        TRIM(UPPER(actividad_económica)) AS actividad_economica,
        TRIM(UPPER(rubro_económico)) AS rubro,
        fecha_inicio_de_actividades_vige AS fecha_inicio,
        fecha_término_de_giro AS fecha_termino,
        número_de_trabajadores_dependie AS n_trabajadores,
        tramo_ventas
    FROM read_parquet('data/silver/sii.parquet') AS e
    LEFT JOIN tmp_mapeo_regiones AS m
        ON TRIM(UPPER(e.región)) = m.nombre_original
        OR TRIM(UPPER(e.región)) = m.nombre_estandarizado
    """)
    con.execute("""
    DELETE FROM dim_sii_empresas
    WHERE rut IS NULL OR rut = '' OR LENGTH(rut) < 3
    """)
    count_sii = con.execute("SELECT COUNT(*) FROM dim_sii_empresas").fetchone()[0]
    print(f"  OK - dim_sii_empresas ({count_sii:,} empresas)")
    con.execute("CHECKPOINT;")
    gc.collect()

# ============================================================
# 4. DIM_OFERENTES - SKIP SI EXISTE
# ============================================================
if 'dim_oferentes' in existing_tables:
    count_ofer = con.execute("SELECT COUNT(*) FROM dim_oferentes").fetchone()[0]
    print(f"\n[4/6] SKIP - dim_oferentes ya existe ({count_ofer:,} oferentes)")
else:
    print("\n[4/6] Creando dim_oferentes...")
    con.execute("""
    CREATE OR REPLACE TABLE dim_oferentes AS
    SELECT
        TRIM(id_licitacion) AS id_licitacion,
        TRIM(UPPER(rut_raw)) AS rut_oferente,
        rut_cuerpo,
        rut_dv,
        TRIM(UPPER(razon_social)) AS razon_social,
        COALESCE(m.nombre_estandarizado, TRIM(UPPER(region_empresa))) AS region,
        TRIM(UPPER(comuna_empresa)) AS comuna,
        adjudicado,
        monto_contrato,
        monto_estimado,
        n_competidores,
        fecha_publicacion,
        fecha_adjudicacion,
        fecha_extraccion,
        TRIM(UPPER(status_adjudicacion)) AS status_adjudicacion,
        TRIM(UPPER(nombre_organismo)) AS nombre_organismo,
        TRIM(UPPER(region_organismo)) AS region_organismo,
        TRIM(UPPER(rubro_licitacion)) AS rubro
    FROM read_parquet('data/silver/oferentes.parquet') AS e
    LEFT JOIN tmp_mapeo_regiones AS m
        ON TRIM(UPPER(e.region_empresa)) = m.nombre_original
        OR TRIM(UPPER(e.region_empresa)) = m.nombre_estandarizado
    """)
    con.execute("""
    DELETE FROM dim_oferentes
    WHERE rut_oferente IS NULL OR rut_oferente = '' OR LENGTH(rut_oferente) < 3
    """)
    count_ofer = con.execute("SELECT COUNT(*) FROM dim_oferentes").fetchone()[0]
    print(f"  OK - dim_oferentes ({count_ofer:,} oferentes)")
    con.execute("CHECKPOINT;")
    gc.collect()

# ============================================================
# 5. FACT_LICITACIONES - SKIP SI EXISTE
# ============================================================
if 'fact_licitaciones' in existing_tables:
    count_lic = con.execute("SELECT COUNT(*) FROM fact_licitaciones").fetchone()[0]
    print(f"\n[5/6] SKIP - fact_licitaciones ya existe ({count_lic:,} licitaciones)")
else:
    print("\n[5/6] Creando fact_licitaciones...")
    con.execute("""
    CREATE OR REPLACE TABLE fact_licitaciones AS
    SELECT
        TRIM(CodigoExterno) AS codigo_externo,
        TRIM(Codigo) AS codigo,
        TRIM(Link) AS link,
        TRIM(Nombre) AS nombre,
        TRIM(Descripcion) AS descripcion,
        TRIM(UPPER(Estado)) AS estado,
        TRIM(UPPER(CodigoEstado)) AS codigo_estado,
        TRIM(UPPER(Tipo)) AS tipo,
        TRIM(UPPER(TipoConvocatoria)) AS tipo_convocatoria,
        TRIM(UPPER(NombreOrganismo)) AS organismo,
        TRIM(UPPER(NombreUnidad)) AS unidad,
        TRIM(UPPER(DireccionUnidad)) AS direccion,
        TRIM(UPPER(ComunaUnidad)) AS comuna,
        COALESCE(m.nombre_estandarizado, TRIM(UPPER(e.RegionUnidad))) AS region,
        TRIM(UPPER(sector)) AS sector,
        FechaCreacion,
        FechaCierre,
        FechaPublicacion,
        FechaAdjudicacion,
        FechaEstimadaAdjudicacion,
        FechaActoAperturaTecnica,
        FechaActoAperturaEconomica,
        MontoEstimado,
        TRY_CAST("Monto Estimado Adjudicado" AS DOUBLE) AS monto_adjudicado,
        TRY_CAST("Valor Total Ofertado" AS DOUBLE) AS monto_ofertado,
        TRY_CAST(MontoUnitarioOferta AS DOUBLE) AS monto_unitario,
        TRY_CAST(Cantidad AS DOUBLE) AS cantidad,
        TRY_CAST(CantidadAdjudicada AS DOUBLE) AS cantidad_adjudicada,
        RutUnidad_cuerpo,
        RutUnidad_dv,
        RutProveedor_cuerpo,
        RutProveedor_dv,
        TRIM(NombreProveedor) AS nombre_proveedor,
        TRIM(RutProveedor) AS rut_proveedor,
        TRIM("Moneda Adquisicion") AS moneda,
        TRIM(Modalidad) AS modalidad,
        TRIM(FuenteFinanciamiento) AS fuente_financiamiento
    FROM read_parquet('data/silver/licitaciones*.parquet', union_by_name=True) AS e
    LEFT JOIN tmp_mapeo_regiones AS m
        ON TRIM(UPPER(e.RegionUnidad)) = m.nombre_original
        OR TRIM(UPPER(e.RegionUnidad)) = m.nombre_estandarizado
    """)
    con.execute("DELETE FROM fact_licitaciones WHERE codigo_externo IS NULL OR codigo_externo = ''")
    count_lic = con.execute("SELECT COUNT(*) FROM fact_licitaciones").fetchone()[0]
    print(f"  OK - fact_licitaciones ({count_lic:,} licitaciones)")
    con.execute("CHECKPOINT;")
    gc.collect()

# ============================================================
# 6. FACT_ORDENES_COMPRA - SIEMPRE PROCESAR (la faltante)
# ============================================================
print("\n[6/6] Creando fact_ordenes_compra...")
print("  [!] Archivo: 4.69 GB (21M filas) - MODO SEGURO")

# Estrategia: SELECT * directo SIN transformaciones complejas
# Las normales de texto se hacen en VIEW posterior
con.execute("""
CREATE OR REPLACE TABLE fact_ordenes_compra AS
SELECT
    -- IDs - solo TRIM básico
    TRIM(ID) AS id_oc,
    TRIM(Codigo) AS codigo,
    TRIM(Link) AS link,
    TRIM(Nombre) AS nombre,

    -- Estados
    TRIM(UPPER(Estado)) AS estado,
    TRIM(UPPER(Tipo)) AS tipo,
    TRIM(UPPER(codigoEstado)) AS codigo_estado,
    TRIM(UPPER(EstadoProveedor)) AS estado_proveedor,

    -- Fechas
    FechaCreacion,
    FechaEnvio,
    FechaAceptacion,
    FechaCancelacion,

    -- Montos
    TRY_CAST(MontoTotalOC AS DOUBLE) AS monto_total,
    TRY_CAST(TotalNetoOC AS DOUBLE) AS monto_neto,

    -- Geografía
    TRIM(UPPER(UnidadCompra)) AS nombre_unidad,
    COALESCE(m.nombre_estandarizado, TRIM(UPPER(RegionUnidadCompra))) AS region,
    TRIM(UPPER(CiudadUnidadCompra)) AS ciudad,
    TRIM(UPPER(PaisUnidadCompra)) AS pais,

    -- Proveedor
    TRIM(NombreProveedor) AS nombre_proveedor,
    TRIM(UPPER(RegionProveedor)) AS region_proveedor,

    -- RUTs
    RutUnidadCompra_cuerpo,
    RutUnidadCompra_dv,
    RutSucursal_cuerpo,
    RutSucursal_dv,

    -- Items
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

con.execute("""
DELETE FROM fact_ordenes_compra
WHERE id_oc IS NULL OR id_oc = ''
""")

count_oc = con.execute("SELECT COUNT(*) FROM fact_ordenes_compra").fetchone()[0]
print(f"  OK - fact_ordenes_compra ({count_oc:,} ordenes)")
con.execute("CHECKPOINT;")
gc.collect()

# ============================================================
# REPORTE FINAL
# ============================================================
print("\n" + "=" * 70)
print("DATA WAREHOUSE CONSTRUIDO (MODO SEGURO)")
print("=" * 70)

print("\n--- CONTEO FINAL ---")
tablas = ['dim_calendario', 'dim_sii_empresas', 'dim_oferentes',
          'fact_licitaciones', 'fact_ordenes_compra']

for tabla in tablas:
    count = con.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    print(f"  {tabla}: {count:,}")

con.close()
print("\n[*] Completado sin colapsos de memoria")