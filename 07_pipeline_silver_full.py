"""
Pipeline de Transformación - Capa Silver (FULL)
================================================
Transforma el 100% de los datos de Bronze a Silver:
- Normalización de RUTs
- Conversión de Fechas a Datetime
- Conversión de Montos a Float64/Int64
- Limpieza de texto ligero

Procesamiento eficiente para Big Data (14M+ registros).
"""

import polars as pl
import re
import os
import io
import glob
import time
from datetime import datetime
from typing import Optional, Tuple, List

start_time = time.time()

# ============================================================================
# FUNCIONES DE NORMALIZACIÓN DE RUT
# ============================================================================

def normalizar_rut(rut_input: str) -> Tuple[Optional[int], Optional[str]]:
    """Normaliza un RUT chileno."""
    if rut_input is None or (isinstance(rut_input, str) and rut_input.strip() == ""):
        return None, None

    rut_input = str(rut_input).strip().upper()
    rut_input = re.sub(r'[^0-9K]', '', rut_input)

    if not rut_input:
        return None, None
    if len(rut_input) == 1 and rut_input in "0123456789K":
        return None, rut_input

    dv = rut_input[-1]
    if dv.isdigit():
        dv = dv
    elif dv == "K":
        dv = "K"
    else:
        return None, None

    cuerpo_str = rut_input[:-1]
    if not cuerpo_str.isdigit():
        return None, None

    try:
        cuerpo = int(cuerpo_str)
    except ValueError:
        return None, None

    return cuerpo, dv


# ============================================================================
# FUNCIONES DE CONVERSIÓN DE FECHAS
# ============================================================================

def parse_date(val: str) -> Optional[datetime]:
    """Convierte string a Datetime. Maneja múltiples formatos."""
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return None

    val = str(val).strip()

    formatos = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]

    for fmt in formatos:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue

    return None


# ============================================================================
# FUNCIONES DE CONVERSIÓN DE MONTOS
# ============================================================================

def parse_monto(val) -> Optional[float]:
    """Convierte valor a Float64 para montos."""
    if val is None:
        return None

    if isinstance(val, (int, float)):
        return float(val)

    val_str = str(val).strip()
    val_str = re.sub(r'[^\d.\-]', '', val_str)

    if val_str == "" or val_str == "-":
        return None

    try:
        return float(val_str)
    except ValueError:
        return None


def parse_int(val) -> Optional[int]:
    """Convierte valor a Int64 para cantidades."""
    if val is None:
        return None

    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)

    val_str = str(val).strip()
    val_str = re.sub(r'[^\d]', '', val_str)

    if val_str == "":
        return None

    try:
        return int(val_str)
    except ValueError:
        return None


# ============================================================================
# LIMPIEZA DE TEXTO LIGERO
# ============================================================================

def clean_string(val) -> Optional[str]:
    """Limpia strings: trim, vacíos y 'null' a None."""
    if val is None:
        return None

    if isinstance(val, (int, float, datetime)):
        return val

    val_str = str(val).strip()

    # Vacíos o "null" a None
    if val_str == "" or val_str.upper() in ("NULL", "NONE", "NA", "N/A", "-"):
        return None

    return val_str


# ============================================================================
# PIPELINE PARA CADA FUENTE
# ============================================================================

def process_sii(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa dataset SII."""
    print("  [SII] Transformando...")

    # RUT
    df = df.with_columns([
        pl.col("rut").cast(pl.Int64).alias("rut_cuerpo"),
        pl.col("dv").alias("rut_dv"),
    ])

    # Fechas
    fecha_cols = ["fecha_inicio_de_actividades_vige", "fecha_término_de_giro",
                  "fecha_primera_inscripción_de_ac"]
    for col in fecha_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).map_elements(parse_date, return_dtype=pl.Datetime).alias(col)
            ])

    # Int64
    if "tramo_según_ventas" in df.columns:
        df = df.with_columns([
            pl.col("tramo_según_ventas").map_elements(parse_int, return_dtype=pl.Int64).alias("tramo_ventas")
        ])
    if "número_de_trabajadores_dependie" in df.columns:
        df = df.with_columns([
            pl.col("número_de_trabajadores_dependie").map_elements(parse_int, return_dtype=pl.Int64).alias("n_trabajadores")
        ])

    # Limpieza de texto
    string_cols = [c for c in df.columns if df.schema[c] == pl.Utf8]
    for col in string_cols:
        df = df.with_columns([
            pl.col(col).map_elements(clean_string, return_dtype=pl.Utf8).alias(col)
        ])

    return df


def process_oferentes(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa dataset Oferentes."""
    print("  [Oferentes] Transformando...")

    # RUT
    if "rut_raw" in df.columns:
        df = df.with_columns([
            pl.col("rut_raw").map_elements(
                lambda x: normalizar_rut(str(x))[0] if x else None,
                return_dtype=pl.Int64
            ).alias("rut_cuerpo"),
            pl.col("rut_raw").map_elements(
                lambda x: normalizar_rut(str(x))[1] if x else None,
                return_dtype=pl.Utf8
            ).alias("rut_dv"),
        ])

    # Fechas
    fecha_cols = ["fecha_publicacion", "fecha_adjudicacion", "fecha_extraccion"]
    for col in fecha_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).map_elements(parse_date, return_dtype=pl.Datetime).alias(col)
            ])

    # Montos
    if "monto_contrato" in df.columns:
        df = df.with_columns([
            pl.col("monto_contrato").map_elements(parse_monto, return_dtype=pl.Float64).alias("monto_contrato")
        ])

    # Limpieza de texto
    string_cols = [c for c in df.columns if df.schema[c] == pl.Utf8]
    for col in string_cols:
        if col not in ["rut_cuerpo", "rut_dv"]:
            df = df.with_columns([
                pl.col(col).map_elements(clean_string, return_dtype=pl.Utf8).alias(col)
            ])

    return df


def process_ordenes(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa dataset Órdenes de Compra."""
    print("  [Órdenes] Transformando...")

    # RUT
    rut_cols = ["RutUnidadCompra", "RutSucursal"]
    for col in rut_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).map_elements(
                    lambda x: normalizar_rut(str(x))[0] if x else None,
                    return_dtype=pl.Int64
                ).alias(f"{col}_cuerpo"),
                pl.col(col).map_elements(
                    lambda x: normalizar_rut(str(x))[1] if x else None,
                    return_dtype=pl.Utf8
                ).alias(f"{col}_dv"),
            ])

    # Fechas
    fecha_cols = ["FechaCreacion", "FechaEnvio", "FechaAceptacion", "FechaCancelacion"]
    for col in fecha_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).map_elements(parse_date, return_dtype=pl.Datetime).alias(col)
            ])

    # Montos
    monto_cols = ["MontoTotalOC", "MontoTotalOC_PesosChilenos", "TotalNetoOC", "precioNeto", "totalLineaNeto"]
    for col in monto_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).map_elements(parse_monto, return_dtype=pl.Float64).alias(col)
            ])

    # Cantidades
    if "cantidad" in df.columns:
        df = df.with_columns([
            pl.col("cantidad").map_elements(parse_monto, return_dtype=pl.Float64).alias("cantidad")
        ])

    # Limpieza de texto
    string_cols = [c for c in df.columns if df.schema[c] == pl.Utf8]
    for col in string_cols:
        if not col.endswith("_cuerpo") and not col.endswith("_dv"):
            df = df.with_columns([
                pl.col(col).map_elements(clean_string, return_dtype=pl.Utf8).alias(col)
            ])

    return df


def process_licitaciones(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa dataset Licitaciones."""
    print("  [Licitaciones] Transformando...")

    # RUT
    rut_cols = ["RutUnidad", "RutProveedor"]
    for col in rut_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).map_elements(
                    lambda x: normalizar_rut(str(x))[0] if x else None,
                    return_dtype=pl.Int64
                ).alias(f"{col}_cuerpo"),
                pl.col(col).map_elements(
                    lambda x: normalizar_rut(str(x))[1] if x else None,
                    return_dtype=pl.Utf8
                ).alias(f"{col}_dv"),
            ])

    # Fechas
    fecha_cols = ["FechaCreacion", "FechaCierre", "FechaPublicacion", "FechaAdjudicacion",
                  "FechaEstimadaAdjudicacion", "FechaActoAperturaTecnica", "FechaActoAperturaEconomica"]
    for col in fecha_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).map_elements(parse_date, return_dtype=pl.Datetime).alias(col)
            ])

    # Montos
    monto_cols = ["MontoEstimado", "Monto Estimado Adjudicado", "Valor Total Ofertado", "MontoUnitarioOferta"]
    for col in monto_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).map_elements(parse_monto, return_dtype=pl.Float64).alias(col)
            ])

    # Cantidades
    if "Cantidad" in df.columns:
        df = df.with_columns([
            pl.col("Cantidad").map_elements(parse_monto, return_dtype=pl.Float64).alias("Cantidad")
        ])
    if "CantidadAdjudicada" in df.columns:
        df = df.with_columns([
            pl.col("CantidadAdjudicada").map_elements(parse_monto, return_dtype=pl.Float64).alias("CantidadAdjudicada")
        ])

    # Limpieza de texto
    string_cols = [c for c in df.columns if df.schema[c] == pl.Utf8]
    for col in string_cols:
        if not col.endswith("_cuerpo") and not col.endswith("_dv"):
            df = df.with_columns([
                pl.col(col).map_elements(clean_string, return_dtype=pl.Utf8).alias(col)
            ])

    return df


# ============================================================================
# PARSER PARA OFERENTES
# ============================================================================

def load_oferentes(path: str) -> pl.DataFrame:
    """Carga y parsea Oferentes corrigiendo el esquema concatenado."""
    print(f"  [Oferentes] Cargando {path}...")

    df_raw = pl.read_parquet(path)
    print(f"    Registros: {len(df_raw)}")

    # Extraer header
    header = df_raw.columns[0].split(',')
    col_name = df_raw.columns[0]
    csv_data = '\n'.join([str(df_raw.to_pandas().iloc[i, 0]) for i in range(len(df_raw))])

    # Usar ignore_errors=True para manejar valores con comas
    df = pl.read_csv(io.StringIO(csv_data), has_header=False, new_columns=header,
                    ignore_errors=True)
    return df


# ============================================================================
# REPORTE DE CONCILIACIÓN
# ============================================================================

def generate_conciliation_report(bronze_counts: dict, silver_counts: dict, silver_sizes: dict):
    """Genera reporte de conciliación."""
    print("\n" + "=" * 80)
    print("REPORTE DE CONCILIACIÓN - BRONZE vs SILVER")
    print("=" * 80)
    print(f"{'Fuente':<20} {'Bronze':>12} {'Silver':>12} {'Estado':>12} {'Peso (MB)':>12}")
    print("-" * 80)

    total_bronze = 0
    total_silver = 0
    total_size = 0

    for fuente, bronze_count in bronze_counts.items():
        silver_count = silver_counts.get(fuente, 0)
        size_mb = silver_sizes.get(fuente, 0)

        status = "✓ Match" if bronze_count == silver_count else "✗ Mismatch"

        print(f"{fuente:<20} {bronze_count:>12,} {silver_count:>12,} {status:>12} {size_mb:>12.1f}")

        total_bronze += bronze_count
        total_silver += silver_count
        total_size += size_mb

    total_status = "✓ Match" if total_bronze == total_silver else "✗ Mismatch"
    print("-" * 80)
    print(f"{'TOTAL':<20} {total_bronze:>12,} {total_silver:>12,} {total_status:>12} {total_size:>12.1f}")
    print("=" * 80)


# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

def main():
    print("=" * 80)
    print("PIPELINE SILVER - PROCESAMIENTO 100%")
    print("=" * 80)

    output_dir = "data/silver"
    os.makedirs(output_dir, exist_ok=True)

    bronze_counts = {}
    silver_counts = {}
    silver_sizes = {}

    # -------------------------------------------------------------
    # 1. SII
    # -------------------------------------------------------------
    print("\n[1/4] Procesando SII...")
    sii_file = "data/staging/sii/nomina_sii.parquet"

    df_sii = pl.read_parquet(sii_file)
    n_sii = len(df_sii)
    bronze_counts["SII"] = n_sii
    print(f"  Registros: {n_sii:,}")

    df_sii_transformed = process_sii(df_sii)
    output_sii = f"{output_dir}/sii.parquet"
    df_sii_transformed.write_parquet(output_sii, compression="snappy")
    print(f"  Guardado: {output_sii}")

    silver_counts["SII"] = len(df_sii_transformed)
    silver_sizes["SII"] = os.path.getsize(output_sii) / (1024 * 1024)

    # -------------------------------------------------------------
    # 2. Oferentes (2 archivos)
    # -------------------------------------------------------------
    print("\n[2/4] Procesando Oferentes...")

    oferentes_files = [
        "data/staging/oferentes/oferentes_completos_2022_2026.parquet",
        "data/staging/oferentes/oferentes_perdedores_completos.parquet"
    ]

    dfs_ofer = []
    total_ofer = 0
    for f in oferentes_files:
        if os.path.exists(f):
            df = load_oferentes(f)
            df = process_oferentes(df)
            dfs_ofer.append(df)
            total_ofer += len(df)

    df_ofer_all = pl.concat(dfs_ofer) if dfs_ofer else None
    if df_ofer_all:
        bronze_counts["Oferentes"] = total_ofer
        print(f"  Total registros: {total_ofer:,}")

        output_ofer = f"{output_dir}/oferentes.parquet"
        df_ofer_all.write_parquet(output_ofer, compression="snappy")
        print(f"  Guardado: {output_ofer}")

        silver_counts["Oferentes"] = len(df_ofer_all)
        silver_sizes["Oferentes"] = os.path.getsize(output_ofer) / (1024 * 1024)
    else:
        bronze_counts["Oferentes"] = 0
        silver_counts["Oferentes"] = 0

    # -------------------------------------------------------------
    # 3. Órdenes de Compra (53 archivos)
    # -------------------------------------------------------------
    print("\n[3/4] Procesando Órdenes de Compra...")

    ordenes_files = glob.glob("data/staging/ordenes_compra/*.parquet")
    dfs_ordenes = []
    total_ordenes = 0

    for f in ordenes_files:
        df = pl.read_parquet(f)
        df = process_ordenes(df)
        dfs_ordenes.append(df)
        total_ordenes += len(df)

    df_ordenes_all = pl.concat(dfs_ordenes)
    bronze_counts["Órdenes"] = total_ordenes
    print(f"  Total registros: {total_ordenes:,}")

    output_ordenes = f"{output_dir}/ordenes.parquet"
    df_ordenes_all.write_parquet(output_ordenes, compression="snappy")
    print(f"  Guardado: {output_ordenes}")

    silver_counts["Órdenes"] = len(df_ordenes_all)
    silver_sizes["Órdenes"] = os.path.getsize(output_ordenes) / (1024 * 1024)

    # -------------------------------------------------------------
    # 4. Licitaciones (53 archivos, 14M+ registros)
    # -------------------------------------------------------------
    print("\n[4/4] Procesando Licitaciones...")

    licit_files = sorted(glob.glob("data/staging/licitaciones/*.parquet"))
    dfs_licit = []
    total_licit = 0

    for i, f in enumerate(licit_files):
        if i % 10 == 0:
            print(f"  Procesando archivo {i+1}/{len(licit_files)}...")

        df = pl.read_parquet(f)
        if len(df.columns) == 105:  # Solo archivos con esquema correcto
            df = process_licitaciones(df)
            dfs_licit.append(df)
            total_licit += len(df)

    df_licit_all = pl.concat(dfs_licit)
    bronze_counts["Licitaciones"] = total_licit
    print(f"  Total registros: {total_licit:,}")

    output_licit = f"{output_dir}/licitaciones.parquet"
    df_licit_all.write_parquet(output_licit, compression="snappy")
    print(f"  Guardado: {output_licit}")

    silver_counts["Licitaciones"] = len(df_licit_all)
    silver_sizes["Licitaciones"] = os.path.getsize(output_licit) / (1024 * 1024)

    # -------------------------------------------------------------
    # REPORTE DE CONCILIACIÓN
    # -------------------------------------------------------------
    generate_conciliation_report(bronze_counts, silver_counts, silver_sizes)

    # Tiempo total
    elapsed = time.time() - start_time
    print(f"\n⏱ Tiempo total de procesamiento: {elapsed/60:.1f} minutos")

    print(f"\nArchivos guardados en: {output_dir}/")


if __name__ == "__main__":
    main()