"""
Refine Gold Layer - Corrección de Calidad de Datos
====================================================
Script para sanar la integridad del Data Warehouse antes de Power BI.

Correcciones implementadas:
1. Crear dim_proveedores_consolidada (RUTs únicos de todas fuentes)
2. Limpiar outliers financieros (>10B CLP)
3. Gestionar fechas nulas con etiqueta descriptiva
"""
import duckdb
from pathlib import Path
import gc

DB_PATH = "data/warehouse/marketplace.duckdb"

Path("data/warehouse").mkdir(parents=True, exist_ok=True)

# ============================================================
# CONEXIÓN CON BLINDAJE DE MEMORIA
# ============================================================
print("Estableciendo conexión...")

con = duckdb.connect(DB_PATH)

# PRAGMAS de seguridad
con.execute("PRAGMA memory_limit='4GB';")
con.execute("PRAGMA threads=2;")

print("[*] Conexión establecida")
print("[*] memory_limit=4GB, threads=2")

print("\n" + "=" * 70)
print("REFINAMIENTO DEL DATA WAREHOUSE - CAPA GOLD")
print("=" * 70)

# ============================================================
# 1. CREAR DIM_PROVEEDORES_CONSOLIDADA
# ============================================================
print("\n[1/4] Creando dim_proveedores_consolidada...")

# Recolectar todos los RUTs únicos de las fuentes
con.execute("""
CREATE OR REPLACE TABLE dim_proveedores_consolidada AS
SELECT
    -- RUT normalizado (sin puntos, sin guión)
    REPLACE(SPLIT_PART(rut_proveedor, '-', 1), '.', '') AS rut_cuerpo_normalizado,
    rut_proveedor AS rut_original,
    nombre_proveedor AS razon_social,
    'licitaciones' AS fuente,
    'No Disponible' AS region
FROM fact_licitaciones
WHERE rut_proveedor IS NOT NULL AND rut_proveedor != ''
GROUP BY 1, 2, 3

UNION ALL

SELECT
    REPLACE(SPLIT_PART(RutProveedor, '-', 1), '.', '') AS rut_cuerpo_normalizado,
    RutProveedor AS rut_original,
    NombreProveedor AS razon_social,
    'ordenes' AS fuente,
    'No Disponible' AS region
FROM fact_ordenes_compra
WHERE RutProveedor IS NOT NULL AND RutProveedor != ''
GROUP BY 1, 2, 3
""")

# Unir con SII para obtener datos adicionales
con.execute("""
MERGE INTO dim_proveedores_consolidada dpc
USING (
    SELECT
        rut_cuerpo,
        TRIM(UPPER(razón_social)) AS razon_social_sii,
        TRIM(UPPER(región)) AS region_sii,
        TRIM(UPPER(actividad_económica)) AS actividad_economica
    FROM dim_sii_empresas
    WHERE rut_cuerpo IS NOT NULL
) AS sii
ON REPLACE(dpc.rut_cuerpo_normalizado, '.', '') = sii.rut_cuerpo
WHEN MATCHED THEN UPDATE SET
    razon_social = COALESCE(dpc.razon_social, sii.razon_social_sii),
    region = COALESCE(dpc.region, sii.region_sii),
    actividad_economica = sii.actividad_economica
""")

count_prov = con.execute("SELECT COUNT(*) FROM dim_proveedores_consolidada").fetchone()[0]
print(f"  OK - dim_proveedores_consolidada ({count_prov:,} proveedores únicos)")

con.execute("CHECKPOINT;")
gc.collect()

# ============================================================
# 2. LIMPIAR OUTLIERS FINANCIEROS
# ============================================================
print("\n[2/4] Limpiando outliers financieros...")

# Crear vista cleaned con filtro de montos razonable
# Umbral: 10,000 millones CLP (10B) es el máximo razonable para una licitacion
con.execute("""
CREATE OR REPLACE VIEW v_fact_licitaciones_clean AS
SELECT
    *,
    CASE
        WHEN MontoEstimado > 10000000000 THEN NULL
        ELSE MontoEstimado
    END AS monto_estimado_limpio
FROM fact_licitaciones
WHERE MontoEstimado <= 10000000000
   OR MontoEstimado IS NULL
""")

con.execute("""
CREATE OR REPLACE VIEW v_fact_ordenes_compra_clean AS
SELECT
    *,
    CASE
        WHEN monto_total > 10000000000 THEN NULL
        ELSE monto_total
    END AS monto_total_limpio
FROM fact_ordenes_compra
WHERE monto_total <= 10000000000
   OR monto_total IS NULL
""")

# Contar outliers eliminados
lic_outliers = con.execute("""
SELECT COUNT(*) FROM fact_licitaciones WHERE MontoEstimado > 10000000000
""").fetchone()[0]

oc_outliers = con.execute("""
SELECT COUNT(*) FROM fact_ordenes_compra WHERE monto_total > 10000000000
""").fetchone()[0]

print(f"  OK - Licitaciones con outliers eliminados: {lic_outliers:,}")
print(f"  OK - Órdenes con outliers eliminados: {oc_outliers:,}")

con.execute("CHECKPOINT;")
gc.collect()

# ============================================================
# 3. GESTIONAR FECHAS NULAS
# ============================================================
print("\n[3/4] Gestionando fechas nulas...")

# Crear vista con fechas nulas gestionadas
con.execute("""
CREATE OR REPLACE VIEW v_fact_licitaciones_dated AS
SELECT
    *,
    CASE
        WHEN FechaCreacion IS NULL THEN 'Sin Fecha (No Adjudicada)'
        ELSE CAST(FechaCreacion AS VARCHAR)
    END AS fecha_creacion_gestionada
FROM fact_licitaciones
""")

# Contar nulos por estado
nulos_estado = con.execute("""
SELECT estado, COUNT(*) AS total
FROM fact_licitaciones
WHERE FechaCreacion IS NULL
GROUP BY estado
ORDER BY total DESC
LIMIT 5
""").fetchall()

print("  Distribución de fechas nulas por estado:")
for row in nulos_estado:
    print(f"    - {row[0]}: {row[1]:,}")

con.execute("CHECKPOINT;")
gc.collect()

# ============================================================
# 4. REPORTE DE CALIDAD REFINADO
# ============================================================
print("\n[4/4] Generando reporte de calidad refinado...")

print("\n" + "=" * 70)
print("REPORTE DE CALIDAD - CAPA GOLD REFINADA")
print("=" * 70)

# Verificar integridad con nueva dimensión
print("\n--- INTEGRIDAD CON dim_proveedores_consolidada ---")

# Match rate en licitaciones
match_rate_lic = con.execute("""
SELECT COUNT(*) * 100.0 / (
    SELECT COUNT(*) FROM fact_licitaciones
    WHERE rut_proveedor IS NOT NULL AND rut_proveedor != ''
)
FROM fact_licitaciones fl
WHERE fl.rut_proveedor IS NOT NULL
  AND fl.rut_proveedor != ''
  AND EXISTS (
    SELECT 1 FROM dim_proveedores_consolidada dpc
    WHERE REPLACE(SPLIT_PART(fl.rut_proveedor, '-', 1), '.', '') = dpc.rut_cuerpo_normalizado
  )
""").fetchone()[0]

print(f"  Match rate licitaciones: {match_rate_lic:.2f}%")

# Match rate en órdenes
match_rate_oc = con.execute("""
SELECT COUNT(*) * 100.0 / (
    SELECT COUNT(*) FROM fact_ordenes_compra
    WHERE RutProveedor IS NOT NULL AND RutProveedor != ''
)
FROM fact_ordenes_compra foc
WHERE foc.RutProveedor IS NOT NULL
  AND foc.RutProveedor != ''
  AND EXISTS (
    SELECT 1 FROM dim_proveedores_consolidada dpc
    WHERE REPLACE(SPLIT_PART(foc.RutProveedor, '-', 1), '.', '') = dpc.rut_cuerpo_normalizado
  )
"").fetchone()[0]

print(f"  Match rate órdenes: {match_rate_oc:.2f}%")

# Verificar vistas creadas
print("\n--- VISTAS CREADAS ---")
vistas = con.execute("""
SELECT name FROM sqlite_master WHERE type='view' AND name LIKE 'v_%'
""").fetchall()

for v in vistas:
    count = con.execute(f"SELECT COUNT(*) FROM {v[0]}").fetchone()[0]
    print(f"  - {v[0]}: {count:,} registros")

con.close()

print("\n" + "=" * 70)
print("REFINAMIENTO COMPLETADO")
print("=" * 70)
print("""
El Data Warehouse ahora cuenta con:
- dim_proveedores_consolidada: Todos los proveedores unicos
- v_fact_licitaciones_clean: Sin outliers financieros
- v_fact_ordenes_compra_clean: Sin outliers financieros
- v_fact_licitaciones_dated: Fechas nulas gestionadas
""")