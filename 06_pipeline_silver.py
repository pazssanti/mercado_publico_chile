"""
Pipeline de Transformación - Capa Silver (Versión Resiliente)
=============================================================
Transforma datos de Bronze a Silver:
- Normalización de RUTs
- Conversión de Fechas a Datetime
- Conversión de Montos a Float64/Int64

Estrategia:
- Skipping automático de fuentes ya procesadas
- Procesamiento por chunks para evitar OOM
- Garbage collection explícito
- Logging estructurado de progreso
"""

import polars as pl
import re
import os
import io
import gc
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import Optional, Tuple, List

# Configuración de chunks
CHUNK_SIZE = 500_000  # 500K filas por chunk

# ============================================================================
# FUNCIONES DE SKIPPING Y VERIFICACIÓN
# ============================================================================

def check_source_completed(source_name: str, output_path: str) -> bool:
    """Verifica si una fuente ya fue procesada completamente."""
    if not os.path.exists(output_path):
        return False

    try:
        meta = pq.read_metadata(output_path)
        print(f"  [SKIP] {source_name}: ya existe con {meta.num_rows:,} filas")
        return True
    except Exception as e:
        print(f"  [WARN] {source_name}: archivo corrupto, se re-procesara")
        try:
            os.remove(output_path)
        except:
            pass
        return False


def get_total_rows_in_files(files: List[str]) -> int:
    """Cuenta filas totales en una lista de archivos Parquet."""
    total = 0
    for f in files:
        try:
            t = pq.read_table(f)
            total += len(t)
        except:
            pass
    return total


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

    # Formatos a intentar
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

    # Remover caracteres no numéricos excepto punto y signo menos
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
# PIPELINE PARA CADA FUENTE
# ============================================================================

def process_sii(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa dataset SII."""
    print("  [SII] Transformando...")

    # RUT: rut (Int64) + dv (String) -> Cuerpo y DV separados
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

    # Montos/Int64
    if "tramo_según_ventas" in df.columns:
        df = df.with_columns([
            pl.col("tramo_según_ventas").map_elements(parse_int, return_dtype=pl.Int64).alias("tramo_ventas")
        ])

    if "número_de_trabajadores_dependie" in df.columns:
        df = df.with_columns([
            pl.col("número_de_trabajadores_dependie").map_elements(parse_int, return_dtype=pl.Int64).alias("n_trabajadores")
        ])

    return df


def process_oferentes(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa dataset Oferentes (con parsing previo del esquema)."""
    print("  [Oferentes] Transformando...")

    # RUT: rut_raw
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

    return df


def process_ordenes(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa dataset Órdenes de Compra."""
    print("  [Órdenes] Transformando...")

    # RUT: RutUnidadCompra, RutSucursal
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

    return df


def process_licitaciones(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa dataset Licitaciones."""
    print("  [Licitaciones] Transformando...")

    # RUT: RutUnidad, RutProveedor
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

    return df


# ============================================================================
# PROCESAMIENTO POR CHUNKS
# ============================================================================

def process_source_in_chunks(source_name: str, input_files: List[str],
                            output_path: str, transform_func,
                            chunk_size: int = CHUNK_SIZE):
    """Procesa una fuente en chunks con escritura inmediata."""

    # Verificar si ya está completo
    if check_source_completed(source_name, output_path):
        return True

    # Contar total de filas
    total_rows = get_total_rows_in_files(input_files)
    total_chunks = (total_rows // chunk_size) + 1

    print(f"  [INFO] {source_name}: {total_rows:,} filas en {total_chunks} chunks")

    chunk_num = 0
    first_chunk = True

    for file_path in input_files:
        try:
            df = pl.read_parquet(file_path)
            file_rows = len(df)
            n_chunks_file = (file_rows // chunk_size) + 1

            print(f"  [INFO] Procesando archivo: {os.path.basename(file_path)} ({file_rows:,} filas)")

            for i in range(n_chunks_file):
                chunk_num += 1
                start = i * chunk_size
                end = min(start + chunk_size, file_rows)
                chunk = df.slice(start, end - start)

                # Transformar
                transformed = transform_func(chunk)

                # Escribir inmediatamente usando PyArrow para modo append
                table = transformed.to_arrow()
                if first_chunk:
                    pq.write_table(table, output_path)
                    first_chunk = False
                else:
                    # Append: leer existente y concatenar
                    existing = pq.read_table(output_path)
                    combined = pa.concat_tables([existing, table])
                    pq.write_table(combined, output_path)

                print(f"  [PROGRESO] {source_name} - Chunk {chunk_num}/{total_chunks} "
                      f"(Filas {start:,} a {end:,}) - RAM liberada")

                # Liberar memoria
                del chunk, transformed, table
                gc.collect()

            del df
            gc.collect()

        except Exception as e:
            print(f"  [ERROR] {source_name}: Fallo en {file_path}: {e}")
            return False

    print(f"  [OK] {source_name} completado: {total_rows:,} filas escritas")
    return True


# ============================================================================
# PARSER PARA OFERENTES (esquema concatenado)
# ============================================================================

def parse_oferentes_parquet(path: str, sample_frac: float = 0.01, seed: int = 42) -> pl.DataFrame:
    """Carga y parsea Oferentes corrigiendo el esquema concatenado."""
    df_raw = pl.read_parquet(path)

    # Muestrear antes de parsear
    n_sample = max(1, int(len(df_raw) * sample_frac))
    df_raw = df_raw.sample(n_sample, seed=seed)

    # Extraer header del nombre de columna
    header = df_raw.columns[0].split(',')

    # Extraer datos
    col_name = df_raw.columns[0]
    csv_data = '\n'.join([str(df_raw.to_pandas().iloc[i, 0]) for i in range(len(df_raw))])

    # Parsear
    df = pl.read_csv(io.StringIO(csv_data), has_header=False, new_columns=header)

    return df


# ============================================================================
# REPORTE DE CALIDAD
# ============================================================================

def generate_quality_report(df_original: pl.DataFrame, df_transformed: pl.DataFrame,
                           source_name: str) -> List[dict]:
    """Genera reporte de calidad comparando tipos originales vs transformados."""
    results = []

    # Columnas transformadas comunes
    transformaciones = {
        # Fechas
        "fecha_publicacion": ("Datetime", pl.Datetime),
        "fecha_adjudicacion": ("Datetime", pl.Datetime),
        "fecha_extraccion": ("Datetime", pl.Datetime),
        "FechaCreacion": ("Datetime", pl.Datetime),
        "FechaCierre": ("Datetime", pl.Datetime),
        "FechaPublicacion": ("Datetime", pl.Datetime),
        "FechaAdjudicacion": ("Datetime", pl.Datetime),
        "fecha_inicio_de_actividades_vige": ("Datetime", pl.Datetime),
        "fecha_término_de_giro": ("Datetime", pl.Datetime),
        # Montos
        "monto_contrato": ("Float64", pl.Float64),
        "MontoTotalOC": ("Float64", pl.Float64),
        "TotalNetoOC": ("Float64", pl.Float64),
        "MontoEstimado": ("Float64", pl.Float64),
        "precioNeto": ("Float64", pl.Float64),
        "totalLineaNeto": ("Float64", pl.Float64),
        "Cantidad": ("Float64", pl.Float64),
        # RUTs
        "rut_cuerpo": ("Int64", pl.Int64),
        "rut_dv": ("String", pl.Utf8),
    }

    for col, (tipo_final, dtype) in transformaciones.items():
        if col in df_transformed.columns:
            col_data = df_transformed.to_pandas()[col]
            n_total = len(col_data)
            n_nulls = col_data.isna().sum()

            # Tipo original en Bronze
            tipo_original = "String"

            pct_null = (n_nulls / n_total * 100) if n_total > 0 else 0

            results.append({
                "Columna": col,
                "Tipo Original": tipo_original,
                "Tipo Final": tipo_final,
                "% Nulos": round(pct_null, 2)
            })

    return results


# ============================================================================
# EJECUCIÓN PRINCIPAL (VERSION RESILIENTE)
# ============================================================================

def main():
    import glob

    print("=" * 70)
    print("PIPELINE SILVER - Ejecución RESILIENTE con Chunking")
    print("=" * 70)

    output_dir = "data/silver"
    os.makedirs(output_dir, exist_ok=True)

    # =============================================================
    # 1. SII - Verificar skip o procesar completo
    # =============================================================
    print("\n[1/4] Verificando SII...")
    sii_output = f"{output_dir}/sii.parquet"

    if check_source_completed("SII", sii_output):
        pass  # Ya completado
    else:
        print("  [INFO] Procesando SII completo...")
        sii_files = glob.glob("data/staging/sii/*.parquet")
        process_source_in_chunks("SII", sii_files, sii_output, process_sii, chunk_size=CHUNK_SIZE)

    gc.collect()

    # =============================================================
    # 2. Órdenes - Verificar skip o procesar completo
    # =============================================================
    print("\n[2/4] Verificando Órdenes de Compra...")
    ordenes_output = f"{output_dir}/ordenes.parquet"

    if check_source_completed("Órdenes", ordenes_output):
        pass  # Ya completado
    else:
        print("  [INFO] Procesando Órdenes completo...")
        ordenes_files = glob.glob("data/staging/ordenes_compra/*.parquet")
        process_source_in_chunks("Órdenes", ordenes_files, ordenes_output, process_ordenes, chunk_size=CHUNK_SIZE)

    gc.collect()

    # =============================================================
    # 3. Oferentes - Volumen pequeño, procesar en chunks pequeños
    # =============================================================
    print("\n[3/4] Procesando Oferentes...")
    oferentes_output = f"{output_dir}/oferentes.parquet"

    if check_source_completed("Oferentes", oferentes_output):
        pass
    else:
        print("  [INFO] Oferentes: volumen bajo, procesando en chunks...")
        oferentes_files = glob.glob("data/staging/oferentes/*.parquet")
        # Oferentes es pequeño: chunks de 100K
        process_source_in_chunks("Oferentes", oferentes_files, oferentes_output,
                                 process_oferentes, chunk_size=100_000)

    gc.collect()

    # =============================================================
    # 4. Licitations - Volumen grande, chunks de 500K
    # =============================================================
    print("\n[4/4] Procesando Licitaciones...")
    licit_output = f"{output_dir}/licitaciones.parquet"

    if check_source_completed("Licitaciones", licit_output):
        pass
    else:
        print("  [INFO] Licitaciones: gran volumen, procesando en chunks de 500K...")
        licit_files = glob.glob("data/staging/licitaciones/*.parquet")
        process_source_in_chunks("Licitaciones", licit_files, licit_output,
                                 process_licitaciones, chunk_size=CHUNK_SIZE)

    gc.collect()

    # =============================================================
    # REPORTE FINAL
    # =============================================================
    print("\n" + "=" * 70)
    print("CONCILIACIÓN FINAL - CAPA SILVER")
    print("=" * 70)

    sources = [
        ("SII", f"{output_dir}/sii.parquet"),
        ("Órdenes", f"{output_dir}/ordenes.parquet"),
        ("Oferentes", f"{output_dir}/oferentes.parquet"),
        ("Licitaciones", f"{output_dir}/licitaciones.parquet"),
    ]

    total_silver = 0
    print(f"\n{'Fuente':<20} {'Filas en Silver':>15} {'Estado':>15}")
    print("-" * 55)

    for name, path in sources:
        if os.path.exists(path):
            try:
                meta = pq.read_metadata(path)
                rows = meta.num_rows
                total_silver += rows
                print(f"{name:<20} {rows:>15,} [OK]")
            except Exception as e:
                print(f"{name:<20} {'ERROR':>15} {str(e):>15}")
        else:
            print(f"{name:<20} {'AUSENTE':>15} [FALTA]")

    print("-" * 55)
    print(f"{'TOTAL':<20} {total_silver:>15,}")
    print("\n== PIPELINE SILVER COMPLETADO ==")
    print(f"\nArchivos guardados en: {output_dir}/")


if __name__ == "__main__":
    main()