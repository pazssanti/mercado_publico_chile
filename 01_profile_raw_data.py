from pathlib import Path
import zipfile
import chardet
import polars as pl
import pandas as pd
from datetime import datetime

RAW_PATH = Path("data/raw")

OUTPUT_DIR = Path("data/logs")
OUTPUT_DIR.mkdir(exist_ok=True)

results = []

# extensiones válidas
VALID_EXTENSIONS = [".csv", ".zip", ".txt"]

files = [
    f for f in RAW_PATH.rglob("*")
    if f.is_file() and f.suffix.lower() in VALID_EXTENSIONS
]

print(f"\nArchivos encontrados: {len(files)}\n")

for file in files:

    print(f"Analizando: {file.name}")

    info = {
        "file_name": file.name,
        "path": str(file),
        "extension": file.suffix.lower(),
        "size_mb": round(file.stat().st_size / (1024 * 1024), 2),
        "encoding": None,
        "delimiter": None,
        "columns": None,
        "sample_columns": None,
        "error": None
    }

    try:

        # Detectar encoding
        with open(file, "rb") as f:
            rawdata = f.read(100000)

        detected = chardet.detect(rawdata)
        encoding = detected["encoding"]

        info["encoding"] = encoding

        # ZIP
        if file.suffix.lower() == ".zip":

            with zipfile.ZipFile(file, "r") as z:

                inner_files = z.namelist()

                csv_files = [
                    x for x in inner_files
                    if x.lower().endswith(".csv")
                ]

                if len(csv_files) > 0:

                    first_csv = csv_files[0]

                    with z.open(first_csv) as f:

                        df = pl.read_csv(
                            f,
                            infer_schema_length=1000,
                            encoding="utf8-lossy",
                            separator=";",
                            n_rows=5
                        )

                        info["columns"] = len(df.columns)
                        info["sample_columns"] = ", ".join(df.columns[:10])
                        info["delimiter"] = ";"

        # CSV/TXT
        else:

            df = pl.read_csv(
                file,
                infer_schema_length=1000,
                encoding="utf8-lossy",
                separator=";",
                n_rows=5
            )

            info["columns"] = len(df.columns)
            info["sample_columns"] = ", ".join(df.columns[:10])
            info["delimiter"] = ";"

    except Exception as e:

        info["error"] = str(e)

    results.append(info)

# Exportar reporte
df_report = pd.DataFrame(results)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

output_file = OUTPUT_DIR / f"profile_raw_data_{timestamp}.csv"

df_report.to_csv(output_file, index=False)

print("\n===================================")
print("PROFILING TERMINADO")
print("===================================")
print(f"\nReporte generado:\n{output_file}\n")