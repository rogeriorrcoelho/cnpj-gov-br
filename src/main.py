#%%

# Código Otimizado
# Este código otimizado utiliza DuckDB para processar o CSV de forma eficiente, gerando arquivos Parquet separados para registros únicos e duplicados, e detectando os CNPJs duplicados.

import duckdb
import pandas as pd
import numpy as np

#%%

# conectar banco local

con = duckdb.connect("data/cnpj.duckdb")

# otimizações
con.execute("PRAGMA threads=4")
con.execute("PRAGMA memory_limit='8GB'")
con.execute("PRAGMA enable_progress_bar")

#%%

# -----------------------------
# 1. carregar CSV em tabela
# -----------------------------
con.execute("""
CREATE OR REPLACE TABLE empresas AS
SELECT *
FROM read_csv_auto(
    'data/empresa0.CSV',
    delim=';',
    header=false,
    ignore_errors=true,
    store_rejects=true,
    sample_size=100000
)
""")
print("CSV carregado")

#%%
print("Estrutura da tabela 'empresas':")
print(con.execute("DESCRIBE empresas").fetchdf())

#%%
# -----------------------------
# 2. gerar Parquet sem duplicados e duplicados
# -----------------------------
# Usando ROW_NUMBER para separar duplicados
con.execute("""
COPY (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cnpj) AS rn
        FROM empresas
    )
    WHERE rn = 1
)
TO 'empresas_sem_dup.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD)
""")
print("Parquet sem duplicados criado")
#%%
con.execute("""
COPY (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cnpj) AS rn
        FROM empresas
    )
    WHERE rn > 1
)
TO 'empresas_duplicadas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD)
""")
print("Parquet de duplicados criado")

# -----------------------------
# 3. detectar e mostrar CNPJs duplicados
# -----------------------------
duplicados = con.execute("""
SELECT cnpj, COUNT(*) as total
FROM empresas
GROUP BY cnpj
HAVING COUNT(*) > 1
""").fetchdf()

print("Duplicados encontrados (top 10):")
print(duplicados.head(10))
# %%
