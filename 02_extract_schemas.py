from pathlib import Path
import zipfile
import polars as pl
import pandas as pd

RAW_PATH = Path("data/raw")
OUTPUT = Path("data/schemas")
OUTPUT.mkdir(exist_ok=True)

results = []

files = list(RAW_PATH.rglob("*.zip"))
files += list(RAW_PATH.rglob("*.csv"))

for file in files:

    print(f"Procesando: {file.name}")

    try:

        # ZIP
        if file.suffix == ".zip":

            with zipfile.ZipFile(file, "r") as z:

                csv_files = [
                    x for x in z.namelist()
                    if x.endswith(".csv")
                ]

                if not csv_files:
                    continue

                with z.open(csv_files[0]) as f:

                    df = pl.read_csv(
                        f,
                        separator=";",
                        infer_schema=False,
                        n_rows=5,
                        encoding="utf8-lossy"
                    )

        else:

            df = pl.read_csv(
                file,
                separator=";",
                infer_schema=False,
                n_rows=5,
                encoding="utf8-lossy"
            )

        for idx, col in enumerate(df.columns):

            results.append({
                "file": file.name,
                "column_position": idx,
                "column_name": col
            })

    except Exception as e:

        results.append({
            "file": file.name,
            "column_position": -1,
            "column_name": f"ERROR: {str(e)}"
        })

schema_df = pd.DataFrame(results)

schema_df.to_csv(
    OUTPUT / "all_schemas.csv",
    index=False
)

print("\nSchemas exportados.")