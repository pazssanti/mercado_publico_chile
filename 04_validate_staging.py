import duckdb

con = duckdb.connect()

tables = {
    "licitaciones": "data/staging/licitaciones/*.parquet",
    "ordenes_compra": "data/staging/ordenes_compra/*.parquet",
    "oferentes": "data/staging/oferentes/*.parquet"
}

for name, path in tables.items():
    result = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchall()
    print(name, result)