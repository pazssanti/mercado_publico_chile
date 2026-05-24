from pathlib import Path
import pandas as pd
import polars as pl

RAW_DIR = Path("data/raw/sii")
OUT_DIR = Path("data/staging/sii")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def detect_sep(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        line = f.readline()

    for sep in ["|", ";", "\t", ","]:
        if sep in line:
            return sep
    return "|"


def process_file(file_path):
    print(f"\nProcesando SII: {file_path.name}")

    sep = detect_sep(file_path)

    try:
        df = pd.read_csv(
            file_path,
            sep=sep,
            encoding="utf-8",
            engine="python",
            on_bad_lines="skip"
        )
    except UnicodeDecodeError:
        df = pd.read_csv(
            file_path,
            sep=sep,
            encoding="latin1",
            engine="python",
            on_bad_lines="skip"
        )

    # normalización básica
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # limpiar strings vacíos
    df = df.replace(["NA", "N/A", "null", ""], None)

    # exportar parquet
    out_file = OUT_DIR / f"{file_path.stem}.parquet"

    pl.from_pandas(df).write_parquet(out_file)

    print(f"OK -> {out_file} | filas={len(df)} | cols={len(df.columns)}")


def main():
    files = list(RAW_DIR.glob("*.txt"))

    print(f"Archivos SII encontrados: {len(files)}")

    for f in files:
        process_file(f)

    print("\nSII staging completado.")


if __name__ == "__main__":
    main()