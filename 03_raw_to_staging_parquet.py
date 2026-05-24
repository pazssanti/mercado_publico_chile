from pathlib import Path
import zipfile
import polars as pl

RAW_PATH = Path("data/raw")
STAGING_PATH = Path("data/staging")

STAGING_PATH.mkdir(exist_ok=True)

files = list(RAW_PATH.rglob("*.zip"))
files += list(RAW_PATH.rglob("*.csv"))

for file in files:

    print(f"\nProcesando: {file.name}")

    try:

        dataset_name = file.parent.name

        output_dir = STAGING_PATH / dataset_name
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"{file.stem}.parquet"

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
                        ignore_errors=True,
                        truncate_ragged_lines=True,
                        encoding="utf8-lossy"
                    )

        # CSV
        else:

            df = pl.read_csv(
                file,
                separator=";",
                infer_schema=False,
                ignore_errors=True,
                truncate_ragged_lines=True,
                encoding="utf8-lossy"
            )

        # Todo string
        df = df.with_columns(
            [pl.col(c).cast(pl.Utf8) for c in df.columns]
        )

        # Export parquet
        df.write_parquet(
            output_file,
            compression="zstd"
        )

        print(f"OK -> {output_file}")

    except Exception as e:

        print(f"ERROR: {e}")

 #python scripts/03_raw_to_staging_parquet.py       