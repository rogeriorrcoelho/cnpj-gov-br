#%%
import duckdb
import glob
from pathlib import Path
import subprocess
import re

#%%
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
#%%
# Configurar DuckDB para usar todo o hardware disponível
#con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')
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


# Estrutura da tabela socios
colunas_tabela_socios = {
    "cnpj_basico": "VARCHAR",
    "identificador_socio": "VARCHAR",
    "nome_socio": "VARCHAR",
    "cnpj_cpf_socio": "VARCHAR",
    "qualificacao_socio": "VARCHAR",
    "data_entrada_sociedade": "VARCHAR",
    "pais": "VARCHAR",
    "representante_legal": "VARCHAR",
    "nome_representante": "VARCHAR",
    "qualificacao_representante_legal": "VARCHAR",
    "faixa_etaria": "VARCHAR"
}

# Criar tabela
criar_tabela(
    con=con,
    schema="raw",
    tabela="socios",
    colunas=colunas_tabela_socios
)

colunas_tabela_cnae = {
    "codigo": "VARCHAR",
    "descricao": "VARCHAR"
}

criar_tabela(
    con=con,
    schema="raw",
    tabela="cnae",
    colunas=colunas_tabela_cnae
)

colunas_tabela_motivos = {
    "codigo": "VARCHAR",
    "descricao": "VARCHAR"
}

criar_tabela(
    con=con,
    schema="raw",
    tabela="motivos",
    colunas=colunas_tabela_motivos
)

colunas_tabela_municipios = {
    "codigo": "VARCHAR",
    "descricao": "VARCHAR"
}

criar_tabela(
    con=con,
    schema="raw",
    tabela="municipios",
    colunas=colunas_tabela_municipios
)

colunas_tabela_natureza_juridica = {
    "codigo": "VARCHAR",
    "descricao": "VARCHAR"
}

criar_tabela(
    con=con,
    schema="raw",
    tabela="natureza_juridica",
    colunas=colunas_tabela_natureza_juridica
)

colunas_tabela_pais = {
    "codigo": "VARCHAR",
    "descricao": "VARCHAR"
}

criar_tabela(
    con=con,
    schema="raw",
    tabela="pais",
    colunas=colunas_tabela_pais
)

colunas_tabela_qualificacoes = {
    "codigo": "VARCHAR",
    "descricao": "VARCHAR"
}

criar_tabela(
    con=con,
    schema="raw",
    tabela="qualificacoes",
    colunas=colunas_tabela_qualificacoes
)


colunas_tabela_simples = {
    "cnpj_basico": "VARCHAR",
    "opcao_simples": "VARCHAR",
    "data_opcao_simples": "VARCHAR",
    "data_exclusao_simples": "VARCHAR",
    "opcao_mei": "VARCHAR",
    "data_opcao_mei": "VARCHAR",
    "data_exclusao_mei": "VARCHAR"
}

criar_tabela(
    con=con,
    schema="raw",
    tabela="simples",
    colunas=colunas_tabela_simples
)

#%%

# Conferir estrutura criada
print(con.execute("DESCRIBE raw.empresas").fetchdf())
print(con.execute("DESCRIBE raw.estabelecimentos").fetchdf())
print(con.execute("DESCRIBE raw.socios").fetchdf())
print(con.execute("DESCRIBE raw.cnae").fetchdf())
print(con.execute("DESCRIBE raw.motivos").fetchdf())
print(con.execute("DESCRIBE raw.municipios").fetchdf())
print(con.execute("DESCRIBE raw.natureza_juridica").fetchdf())
print(con.execute("DESCRIBE raw.pais").fetchdf())
print(con.execute("DESCRIBE raw.qualificacoes").fetchdf())
print(con.execute("DESCRIBE raw.simples").fetchdf())

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

#%%
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
# Ingestão dos arquivos CSV para o banco duckdb 
# carregar_arquivos_csv(
#     con,
#     pattern=padrao,
#     schema="raw",
#     tabela="/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/estabelecimento_utf8_*.CSV"
# )


#%%
# PIPELINE completo para as cargas dos arquivos CSV
cargas = [
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/empresa_utf8_*.CSV", "raw", "empresas"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/estabelecimento_utf8_*.CSV", "raw", "estabelecimentos"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/SOCIO_utf8_*.CSV", "raw", "socios"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/CNAE_utf8.CSV", "raw", "cnae"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/MOTI_utf8.CSV", "raw", "motivos"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/MUNIC_utf8_*.CSV", "raw", "municipios"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/NATJU_utf8.CSV", "raw", "natureza_juridica"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/PAIS_utf8.CSV", "raw", "pais"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/QUALS_utf8.CSV", "raw", "qualificacoes"),
    ("/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/csv_utf8/SIMPLES_utf8.CSV", "raw", "simples")
]

for pattern, schema, tabela in cargas:
    carregar_arquivos_csv(con, pattern, schema, tabela)


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
import duckdb
con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')

import duckdb
import os

# Conexão com o banco DuckDB
con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')

# Pasta de destino para os arquivos Parquet
output_dir = '/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet'
os.makedirs(output_dir, exist_ok=True)

# Lista todas as tabelas do schema 'raw'
tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='raw'").fetchall()
tables = [t[0] for t in tables]

# Loop para exportar cada tabela
for table in tables:
    output_file = os.path.join(output_dir, f"{table}.parquet")
    print(f"Exportando {table} para {output_file}...")
    con.execute(f"""
        COPY raw.{table}
        TO '{output_file}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

print("Exportação finalizada!")
# %%
