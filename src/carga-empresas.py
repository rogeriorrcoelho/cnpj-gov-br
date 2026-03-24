#%%
import duckdb
import glob
from pathlib import Path
import subprocess
import re

# funções para criar banco, tabela, carregar dados, etc
def criar_banco(nome_banco: str, schema: str):
    """
    Cria ou abre um banco DuckDB e cria o schema se não existir.
    """

    # garantir diretório do banco
    db_path = Path(nome_banco)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(nome_banco)

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    return con


def criar_tabela(con, schema: str, tabela: str, colunas: dict):
    """
    Cria tabela se não existir.

    colunas = {
        "nome_coluna": "TIPO_SQL"
    }
    """

    colunas_sql = ",\n".join([f"{nome} {tipo}" for nome, tipo in colunas.items()])

    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{tabela} (
        {colunas_sql}
    )
    """

    con.execute(sql)



#%%    

# Criar banco e schema
con = criar_banco(
    nome_banco="/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb",
    schema="raw"                      # raw, staging, analytics
)

# Configurar DuckDB para usar todo o hardware disponível
con.execute("PRAGMA threads=4")
con.execute("PRAGMA memory_limit='12GB'")
con.execute("PRAGMA temp_directory='/tmp'")

#%%

# Estrutura da tabela
colunas_tabela_empresas = {
    "cnpj_basico": "VARCHAR",
    "razao_social": "VARCHAR",
    "natureza_juridica": "VARCHAR",
    "qualificacao_responsavel": "VARCHAR",
    "capital_social": "VARCHAR",
    "porte_empresa": "VARCHAR",
    "ente_federativo_responsavel": "VARCHAR"
}

# Criar tabela
criar_tabela(
    con=con,
    schema="raw",
    tabela="empresas",
    colunas=colunas_tabela_empresas
)


colunas_tabela_estabelecimentos = {
    "cnpj_basico": "VARCHAR",
    "cnpj_ordem": "VARCHAR",
    "cnpj_dv": "VARCHAR",
    "identificador_matriz_filial": "VARCHAR",
    "nome_fantasia": "VARCHAR",
    "situacao_cadastral": "VARCHAR",
    "data_situacao_cadastral": "VARCHAR",
    "motivo_situacao_cadastral": "VARCHAR",
    "nome_cidade_exterior": "VARCHAR",
    "pais": "VARCHAR",
    "data_inicio_atividade": "VARCHAR",
    "cnae_fiscal_principal": "VARCHAR",
    "cnae_fiscal_secundaria": "VARCHAR",
    "tipo_logradouro": "VARCHAR",
    "logradouro": "VARCHAR",
    "numero": "VARCHAR",
    "complemento": "VARCHAR",
    "bairro": "VARCHAR",
    "cep": "VARCHAR",
    "uf": "VARCHAR",
    "municipio": "VARCHAR",
    "ddd_1": "VARCHAR",
    "telefone_1": "VARCHAR",
    "ddd_2": "VARCHAR",
    "telefone_2": "VARCHAR",
    "ddd_fax": "VARCHAR",
    "fax": "VARCHAR",
    "correio_eletronico": "VARCHAR",
    "situacao_especial": "VARCHAR",
    "data_situacao_especial": "VARCHAR"
}

# Criar tabela_estabelecimentos
criar_tabela(
    con=con,
    schema="raw",
    tabela="estabelecimentos",
    colunas=colunas_tabela_estabelecimentos
)


#%%

# Conferir estrutura criada
print(con.execute("DESCRIBE raw.empresas").fetchdf())
# Conferir estrutura criada
print(con.execute("DESCRIBE raw.estabelecimentos").fetchdf())

#%%
# Converte os arquivos CSV para UTF-8 usando o iconv

def converter_csv_para_utf8(diretorio_origem, diretorio_destino=None):

    origem = Path(diretorio_origem)

    if diretorio_destino:
        destino = Path(diretorio_destino)
        destino.mkdir(parents=True, exist_ok=True)
    else:
        destino = origem

    arquivos = sorted(origem.glob("*.CSV"))

    if not arquivos:
        print("Nenhum arquivo CSV encontrado.")
        return

    print(f"{len(arquivos)} arquivos encontrados para conversão")

    for arquivo in arquivos:

        nome = arquivo.stem

        # extrai número do final do nome
        match = re.search(r'(\d+)$', nome)

        if match:
            numero = match.group(1)
            prefixo = nome[:match.start()]
            novo_nome = f"{prefixo}_utf8_{numero}.CSV"
        else:
            novo_nome = f"{nome}_utf8.CSV"

        saida = destino / novo_nome

        subprocess.run([
            "iconv",
            "-f", "ISO-8859-1",
            "-t", "UTF-8",
            str(arquivo),
            "-o", str(saida)
        ], check=True)

        print(f"{arquivo.name} -> {novo_nome}")

    print("Conversão concluída.")

#%%
# variáveis de diretórios
dir_csv = "/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv"
dir_csv_utf8 = "/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8"

# converte os arquivos
converter_csv_para_utf8(
    dir_csv,
    dir_csv_utf8
)


#%%
# Carga dos dados no banco

def carregar_arquivos_csv(con, pattern, schema, tabela, delimiter=";", header=False):

    arquivos = glob.glob(pattern)

    if not arquivos:
        print(f"Nenhum arquivo encontrado para: {pattern}")
        return

    print(f"{len(arquivos)} arquivos encontrados para carga em {schema}.{tabela}")

    header_sql = "TRUE" if header else "FALSE"

    sql = f"""
    COPY {schema}.{tabela}
    FROM '{pattern}'
    (
        DELIMITER '{delimiter}',
        HEADER {header_sql},
        QUOTE '"',
        ESCAPE '"',
        NULLSTR '',
        AUTO_DETECT FALSE
    )
    """

    con.execute(sql)

    total = con.execute(f"SELECT COUNT(*) FROM {schema}.{tabela}").fetchone()[0]

    print(f"Carga concluída em {schema}.{tabela}. Total de registros: {total}")


#%%
# PIPELINE completo para as cargas dos arquivos CSV
cargas = [
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/empresa_utf8_*.CSV", "raw", "empresas"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/estabelecimento_utf8_*.CSV", "raw", "estabelecimentos")
]

for pattern, schema, tabela in cargas:
    carregar_arquivos_csv(con, pattern, schema, tabela)



#%%

# padrão para o DuckDB
# padrao = f"{dir_csv_utf8}/empresa_utf8_*.CSV"
# 
# # Ingestão dos arquivos CSV para o banco duckdb 
# carregar_arquivos_csv(
#     con,
#     pattern=padrao,
#     schema="raw",
#     tabela="empresas"
# )
# 
# # padrão para o DuckDB
# padrao = f"{dir_csv_utf8}/estabelecimento_utf8_*.CSV"
# 
# # Ingestão dos arquivos CSV para o banco duckdb 
# carregar_arquivos_csv(
#     con,
#     pattern=padrao,
#     schema="raw",
#     tabela="estabelecimentos"
# )cnpj_env_3_12

#%%
# Exportar para PARQUET
con.execute("""
COPY raw.empresas
TO '/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/empresas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
)
""")

# %%
