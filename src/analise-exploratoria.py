#%%
# Análise Exploratória dos Dados
# Tabelas de Frequência, Média, Mediana, Moda, Variância, 
# Desvio Padrão, Coeficiente de Variação, Quartis, Boxplot e Histograma.

import duckdb
con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')

# tabela empresas
# cnpj_basico
# razao_social
# natureza_juridica
# qualificacao_responsavel
# capital_social
# porte_empresa
# ente_federativo_responsavel

# frequencia de natureza_juridica
sql = """SELECT 
    natureza_juridica,
    count(*)/1000000 as total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/empresas.parquet')
GROUP BY natureza_juridica
ORDER BY total DESC;"""
con.execute(sql)
print(con.fetchall())

plot = con.execute(sql).df().plot.bar(x='natureza_juridica', y='total', legend=False, figsize=(10, 6))
plot.set_xlabel('Natureza Jurídica (milhões de registros)')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Natureza Jurídica')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show() 
# %%
# frequencia de qualificacao_responsavel
sql = """SELECT 
    qualificacao_responsavel,
    count(*)/1000000 as total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/empresas.parquet')
GROUP BY qualificacao_responsavel
ORDER BY total DESC;"""
con.execute(sql)
print(con.fetchall())

plot = con.execute(sql).df().plot.bar(x='qualificacao_responsavel', y='total', legend=False, figsize=(10, 6))
plot.set_xlabel('Qualificação do Responsável (milhões de registros)')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Qualificação do Responsável')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show() 
# %%
# frequencia de porte_empresa
sql = """SELECT 
    porte_empresa,
    count(*)/1000000 as total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/empresas.parquet')
GROUP BY porte_empresa
ORDER BY total DESC;"""
con.execute(sql)
print(con.fetchall())

plot = con.execute(sql).df().plot.bar(x='porte_empresa', y='total', legend=False, figsize=(10, 6))
plot.set_xlabel('Porte da Empresa (milhões de registros)')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Porte da Empresa')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show() 

# %%
# frequencia de ente_federativo_responsavel
sql = """SELECT 
    ente_federativo_responsavel,
    count(*) as total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/empresas.parquet')
where ente_federativo_responsavel is not null
GROUP BY ente_federativo_responsavel
ORDER BY total DESC
limit 50;"""
con.execute(sql)
print(con.fetchall())

plot = con.execute(sql).df().plot.bar(x='ente_federativo_responsavel', y='total', legend=False, figsize=(10, 6))
plot.set_xlabel('Ente Federativo Responsável')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Ente Federativo Responsável (Top 50)')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show() 

# %%
# As informações de ente_federativo_responsavel precisam ser normalizadas, pois existem variações de grafia e abreviações.
# Sair de:
# SAO PAULO
# SAO PAULO - SP
# SANTO ANDRE - SP
# e chegar em:
# SP → total consolidado
# RJ → total consolidado
# ...
# BR → total da União

# Criar a VIEW normalizada
sql = """
CREATE OR REPLACE VIEW vw_empresas_por_estado AS
WITH estados AS (
    SELECT * FROM (VALUES
        ('ACRE', 'AC'),
        ('ALAGOAS', 'AL'),
        ('AMAPA', 'AP'),
        ('AMAZONAS', 'AM'),
        ('BAHIA', 'BA'),
        ('CEARA', 'CE'),
        ('DISTRITO FEDERAL', 'DF'),
        ('ESPIRITO SANTO', 'ES'),
        ('GOIAS', 'GO'),
        ('MARANHAO', 'MA'),
        ('MATO GROSSO', 'MT'),
        ('MATO GROSSO DO SUL', 'MS'),
        ('MINAS GERAIS', 'MG'),
        ('PARA', 'PA'),
        ('PARAIBA', 'PB'),
        ('PARANA', 'PR'),
        ('PERNAMBUCO', 'PE'),
        ('PIAUI', 'PI'),
        ('RIO DE JANEIRO', 'RJ'),
        ('RIO GRANDE DO NORTE', 'RN'),
        ('RIO GRANDE DO SUL', 'RS'),
        ('RONDONIA', 'RO'),
        ('RORAIMA', 'RR'),
        ('SANTA CATARINA', 'SC'),
        ('SAO PAULO', 'SP'),
        ('SERGIPE', 'SE'),
        ('TOCANTINS', 'TO')
    ) AS t(nome, uf)
),

base AS (
    SELECT 
        CASE 
            WHEN UPPER(TRIM(ente_federativo_responsavel)) LIKE '% - %' 
                THEN split_part(UPPER(TRIM(ente_federativo_responsavel)), ' - ', 2)
            ELSE UPPER(TRIM(ente_federativo_responsavel))
        END AS estado_raw
    FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/empresas.parquet')
    WHERE ente_federativo_responsavel IS NOT NULL
)

SELECT 
    CASE 
        WHEN estado_raw = 'UNIÃO' THEN 'BR'
        ELSE COALESCE(e.uf, estado_raw)
    END AS estado,
    COUNT(*) AS total
FROM base b
LEFT JOIN estados e 
    ON b.estado_raw = e.nome
GROUP BY estado;
"""
con.execute(sql)

# %%
# Consultar a VIEW normalizada
sql = """SELECT * FROM vw_empresas_por_estado
ORDER BY total DESC;"""
con.execute(sql)
print(con.fetchall())   
#%%
# gráfico de barras
plot = con.execute(sql).df().plot.bar(x='estado', y='total', legend=False, figsize=(10, 6))
plot.set_xlabel('Estado')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Empresas por Estado')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show()

# %%
# Agrupando por região
sql = """SELECT 
    CASE 
        WHEN estado IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
        WHEN estado IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
        WHEN estado IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
        WHEN estado IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
        WHEN estado IN ('PR', 'RS', 'SC') THEN 'Sul'
        WHEN estado = 'BR' THEN 'Brasil'
        ELSE 'Desconecido'
    END AS regiao,
    SUM(total) AS total
FROM vw_empresas_por_estado
GROUP BY regiao
ORDER BY total DESC;"""
con.execute(sql)
print(con.fetchall())
#%%
# gráfico de barras por região
plot = con.execute(sql).df().plot.bar(x='regiao', y='total', legend=False, figsize=(10, 6))
plot.set_xlabel('Região')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Empresas por Região')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show()
#%%
sql = """SELECT 
    *
FROM vw_empresas_por_estado
;"""
con.execute(sql)   

print(con.fetchall())

# %%
