#%%
import duckdb

def converte_tabela(nm_tab_origem: str, nm_tab_destino: str, path: str, nm_coluna: str, con: str):

    caminho_origem = f"{path}/{nm_tab_origem}"
    caminho_destino = f"{path}/{nm_tab_destino}"


    try:
        query = f"""
        CREATE OR REPLACE TABLE nova_tabela AS
        SELECT 
            TRY_CAST({nm_coluna} AS DATE) AS {nm_coluna},
            * EXCLUDE ({nm_coluna})
        FROM read_parquet('{caminho_origem}');
        """

        con.execute(query)

        con.execute(f"""
        COPY nova_tabela TO '{caminho_destino}' (FORMAT PARQUET);
        """)

        print(f"✅ Arquivo gerado em: {caminho_destino}")

    except Exception as e:
        print(f"❌ Erro: {e}")

    finally:
        con.close()

        
#%%

con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')

# ✅ Exemplo de uso
converte_tabela(
    nm_tab_origem='estabelecimentos.parquet',
    nm_tab_destino='estabelecimentos_convertida.parquet',
    path='/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet',
    nm_coluna='data_inicio_atividade',
    con=con
)
# %%
# Verificando se a conversão foi bem-sucedida e 
# se o número de registros é o mesmo
con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')
query = f"""
SELECT 
    count(*) as total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos.parquet');
"""

con.execute(query)
print(con.fetchone())

query = f"""
SELECT 
    count(*) as total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos_convertida.parquet');
"""

con.execute(query)

print(con.fetchone())

# %%
converte_tabela(
    nm_tab_origem='estabelecimentos_convertida.parquet',
    nm_tab_destino='estabelecimentos_convertida2.parquet',
    path='/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet',
    nm_coluna='data_situacao_cadastral',
    con=con
)
# %%
# Verificando se a conversão foi bem-sucedida e 
# se o número de registros é o mesmo
con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')
query = f"""
SELECT 
    count(*) as total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos_convertida.parquet');
"""

con.execute(query)
print(con.fetchone())

query = f"""
SELECT 
    count(*) as total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos_convertida2.parquet');
"""

con.execute(query)

print(con.fetchone())

# %%
