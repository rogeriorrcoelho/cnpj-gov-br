#%%
import duckdb

con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')

tabelas = con.execute("SHOW TABLES from raw").fetchall()
print("TABELAS:", tabelas)

for (tabela,) in tabelas:
    count = con.execute(f"SELECT COUNT(*) FROM raw.{tabela}").fetchone()[0]
    print(f"{tabela}: {count:,}".replace(",", "."))


