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
ORDER BY total DESC
;"""
con.execute(sql)   

print(con.fetchall())

# %%
# gráffico de barras por estado
plot = con.execute(sql).df().plot.bar(x='estado', y='total', legend=False, figsize=(10, 6))
plot.set_xlabel('Estado')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Empresas por Estado')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show() 
# %%
# Análise de empresas por estado (agrupando por situacao_cadastral)
# A consulta agrupa os dados fazendo um join entre a tabela de estabelecimentos e a tabela de empresas 
# para obter o número de empresas por situação cadastral. Depois agrupa por UF e situação cadastral, 
# e ordena pelo total de empresas.
sql = """SELECT 
    c.uf AS estado,
    c.situacao_cadastral,
    COUNT(*)/1000000 AS total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos.parquet') AS c
JOIN read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/empresas.parquet') AS e
    ON c.cnpj_basico = e.cnpj_basico
GROUP BY c.uf, c.situacao_cadastral
ORDER BY total DESC, c.uf, c.situacao_cadastral;"""
con.execute(sql)
print(con.fetchall())

# %%
# gráfico de barras por total, estado e situação cadastral
plot = con.execute(sql).df().pivot(index='estado', columns='situacao_cadastral', values='total').plot.bar(stacked=True, figsize=(10, 6))
plot.set_xlabel('Estado')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Estabelecimentos de Empresas por Estado e Situação Cadastral (Em milhões)')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.legend(title='Situação Cadastral', bbox_to_anchor=(1.05,   1), loc='upper left')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show()
# %%
import matplotlib.pyplot as plt
import numpy as np

# Executa a query
con.execute(sql)
data = con.fetchall()  # já vem no formato: [(uf, codigo, total), ...]

# Preservar ordem de aparição
ufs = []
codigos = []
for uf, codigo, _ in data:
    if uf not in ufs:
        ufs.append(uf)
    if codigo not in codigos:
        codigos.append(codigo)

# Montar estrutura agrupada
grouped = {uf: {} for uf in ufs}
for uf, codigo, total in data:
    grouped[uf][codigo] = total

# Posições no eixo X
x = np.arange(len(ufs))
width = 0.8 / len(codigos)

plt.figure()

# Plot: mesma cor por código
for i, codigo in enumerate(codigos):
    values = [grouped[uf].get(codigo, 0) for uf in ufs]
    plt.bar(x + i * width, values, width=width, label=codigo)

# Ajustes visuais
plt.xlabel("UF")
plt.ylabel("Total")
plt.title("Totais por UF (barras lado a lado por código)")
plt.xticks(x + width * (len(codigos) - 1) / 2, ufs)
plt.legend(title="Código")

plt.tight_layout()
plt.show()
# %%
# Análise de quantas empresas foram abertas por ano (agrupando por ano de abertura) por uf

sql = """SELECT 
    c.uf AS estado,
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d')) AS ano_abertura,
    COUNT(*) AS total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos.parquet') AS c
GROUP BY 
    c.uf, 
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d'))
ORDER BY c.uf, ano_abertura;"""
con.execute(sql)
print(con.fetchall())

# %%
# gráfico de barras por total, estado e ano de abertura
plot = con.execute(sql).df().pivot(index='estado', columns='ano_abertura', values='total').plot.bar(stacked=True, figsize=(10, 6))
plot.set_xlabel('Estado')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Estabelecimentos de Empresas por Estado e Ano de Abertura')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.legend(title='Ano de Abertura', bbox_to_anchor=(1.05, 1), loc='upper left')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show()
# %%
# gráfico de linha para o estado de SP com total por ano de abertura
sql = """SELECT 
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d')) AS ano_abertura,
    COUNT(*) AS total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos.parquet') AS c
WHERE c.uf = 'SP'
GROUP BY 
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d'))
ORDER BY ano_abertura;"""
con.execute(sql)
print(con.fetchall())  

# plot = con.execute(sql).df().plot.line(x='ano_abertura', y='total', legend=False, figsize=(10, 6))
plot = con.execute(sql).df().plot(
    x='ano_abertura',
    y='total',
    marker='o',  # pontos reais
    legend=False,
    figsize=(10, 6)
)
plot.set_xlabel('Ano de Abertura')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Estabelecimentos de Empresas em SP por Ano de Abertura')
# 🔥 remove notação científica
plot.ticklabel_format(style='plain', axis='y')
plot.grid()
plot.figure.tight_layout()
plot.figure.show()
# %%
sql = """SELECT 
    c.uf AS estado,
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d')) AS ano_abertura,
    COUNT(*) AS total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos.parquet') AS c
WHERE 
    c.data_inicio_atividade IS NOT NULL
    AND LENGTH(c.data_inicio_atividade) = 8
GROUP BY 
    c.uf,
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d'))
ORDER BY c.uf, ano_abertura;"""
con.execute(sql)
print(con.fetchall())  

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Carrega tudo uma vez
df = con.execute(sql).df()

# Loop por estado
for uf in df['estado'].unique():
    df_uf = df[df['estado'] == uf]

    plot = df_uf.plot(
        x='ano_abertura',
        y='total',
        marker='o',
        legend=False,
        figsize=(10, 6)
    )

    plot.set_xlabel('Ano de Abertura')
    plot.set_ylabel('Frequência')
    plot.set_title(f'Frequência de Estabelecimentos por Ano - {uf}')

    # remove notação científica
    plot.ticklabel_format(style='plain', axis='y')

    # formatação opcional (milhar)
    plot.yaxis.set_major_formatter(
        ticker.StrMethodFormatter('{x:,.0f}')
    )

    plot.grid()
    plot.figure.tight_layout()
    plot.figure.show()

# %%
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Carrega os dados (mesma query ajustada)
df = con.execute(sql).df()

plt.figure(figsize=(12, 7))

top_ufs = (
    df.groupby('estado')['total']
    .sum()
    .nlargest(5)
    .index
)

for uf in top_ufs:
    df_uf = df[df['estado'] == uf]
    
    plt.plot(
        df_uf['ano_abertura'],
        df_uf['total'],
        marker='o',
        label=uf
    )

## Loop para plotar cada UF
#for uf in df['estado'].unique()[:5]:  # limitar a 5 UFs para visualização
#    df_uf = df[df['estado'] == uf]
#    
#    plt.plot(
#        df_uf['ano_abertura'],
#        df_uf['total'],
#        marker='o',
#        label=uf
#    )

# Ajustes
plt.xlabel('Ano de Abertura')
plt.ylabel('Frequência')
plt.title('Frequência de Estabelecimentos por Ano (todas as UFs)')

# legenda com várias colunas (melhora visual)
plt.legend(title='UF', bbox_to_anchor=(1.05, 1), loc='upper left', ncol=2)

# remove notação científica
plt.ticklabel_format(style='plain', axis='y')

# formatação de milhar
plt.gca().yaxis.set_major_formatter(
    ticker.StrMethodFormatter('{x:,.0f}')
)

plt.grid()
plt.tight_layout()
plt.show()
# %%
import pandas as pd

# Top 5 UFs por volume total
top_ufs = (
    df.groupby('estado')['total']
    .sum()
    .nlargest(5)
    .index
)

resultados = []

for uf in top_ufs:
    df_uf = df[df['estado'] == uf].sort_values('ano_abertura')
    
    vi = df_uf.iloc[0]['total']
    vf = df_uf.iloc[-1]['total']
    n = df_uf['ano_abertura'].nunique() - 1

    if vi > 0 and n > 0:
        cagr = (vf / vi) ** (1/n) - 1
    else:
        cagr = None

    resultados.append((uf, cagr))

# Exibir
for uf, taxa in resultados:
    print(f"{uf}: {taxa:.2%}" if taxa else f"{uf}: N/A")
# %%
# Cálculo do CAGR para todas as UFs
# O CAGR (Compound Annual Growth Rate) é uma medida de crescimento anual composta.
# Indica a taxa de crescimento média anual de um valor ao longo de um período de tempo.
# A fórmula do CAGR é:
# CAGR = (VF / VI)^(1/n) - 1
# Onde:
# VF = Valor Final (total de estabelecimentos no último ano)
# VI = Valor Inicial (total de estabelecimentos no primeiro ano)
# n = número de períodos (anos) entre o valor inicial e o valor final  

import pandas as pd

resultados = []

for uf in df['estado'].unique():
    df_uf = df[df['estado'] == uf].sort_values('ano_abertura')
    
    # garante que há dados suficientes
    if len(df_uf) < 2:
        continue

    vi = df_uf.iloc[0]['total']
    vf = df_uf.iloc[-1]['total']
    n = df_uf['ano_abertura'].nunique() - 1

    if vi > 0 and n > 0:
        cagr = (vf / vi) ** (1/n) - 1
        resultados.append((uf, cagr))

# transformar em DataFrame para facilitar análise
df_cagr = pd.DataFrame(resultados, columns=['estado', 'cagr'])

# ordenar do maior crescimento para o menor
df_cagr = df_cagr.sort_values(by='cagr', ascending=False)
df_cagr['cagr_%'] = df_cagr['cagr'] * 100
print(df_cagr)

print(df_cagr.head(5)) # em percentual

print(df_cagr.head(5)) # as que mais cresceram em termos percentuais

print(df_cagr.tail(5)) # as que menos cresceram (ou mais diminuíram) em termos percentuais
# %%
# grafico de barras do cagr_% por estado
plot = df_cagr.plot.bar(x='estado', y='cagr_%', legend=False, figsize=(10, 6))
plot.set_xlabel('Estado')
plot.set_ylabel('CAGR (%)')
plot.set_title('Taxa de Crescimento Anual Composta (CAGR) do Total de Estabelecimentos por Estado')
plot.set_xticklabels(plot.get_xticklabels(), rotation=45, ha='right')
plot.grid(axis='y')
plot.figure.tight_layout()
plot.figure.show()
# %%
# Análise de quantas empresas de cada natureza jurídica foram abertas
# (agrupando por ano de abertura) por uf
sql = """SELECT 
    c.uf AS estado,
    e.natureza_juridica,
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d')) AS ano_abertura,
    COUNT(*) AS total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos.parquet') AS c
JOIN read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/empresas.parquet') AS e
    ON c.cnpj_basico = e.cnpj_basico
WHERE 
    c.data_inicio_atividade IS NOT NULL
    AND LENGTH(c.data_inicio_atividade) = 8
GROUP BY 
    c.uf,
    e.natureza_juridica,
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d'))
ORDER BY c.uf, e.natureza_juridica, ano_abertura;"""
con.execute(sql)
print(con.fetchall())
# %%
# gráfico de barras por total, estado, natureza jurídica e ano de abertura por estado
plot = con.execute(sql).df().pivot_table(index='ano_abertura', columns=['estado', 'natureza_juridica'], values='total', aggfunc='sum').plot(stacked=True, figsize=(12, 8))
plot.set_xlabel('Ano de Abertura')
plot.set_ylabel('Frequência')
plot.set_title('Frequência de Estabelecimentos por Ano, Estado e Natureza Jurídica')
plot.legend(title='Estado e Natureza Jurídica', bbox_to_anchor=(1.05, 1), loc='upper left')
plot.grid()
plot.figure.tight_layout()
plot.figure.show() 

# %%
import matplotlib.pyplot as plt

df = con.execute(sql).df()

# loop por estado
for uf in df['estado'].unique():
    
    df_uf = df[df['estado'] == uf].sort_values('ano_abertura')

    # 🔥 filtrar TOP 5 naturezas jurídicas desse estado
    top_naturezas = (
        df_uf.groupby('natureza_juridica')['total']
        .sum()
        .nlargest(5)
        .index
    )

    df_uf = df_uf[df_uf['natureza_juridica'].isin(top_naturezas)]

    # pivot e plot
    plot = (
        df_uf
        .pivot_table(
            index='ano_abertura',
            columns='natureza_juridica',
            values='total',
            aggfunc='sum'
        )
        .plot(
            kind='bar',
            stacked=False,
            figsize=(12, 8)
        )
    )

    plot.set_xlabel('Ano de Abertura')
    plot.set_ylabel('Frequência')
    plot.set_title(f'Frequência por Ano e Natureza Jurídica (Top 10) - {uf}')

    plot.legend(
        title='Natureza Jurídica',
        bbox_to_anchor=(1.05, 1),
        loc='upper left'
    )

    plot.set_xticklabels(plot.get_xticklabels(), rotation=45)

    plot.grid(axis='y')
    plot.figure.tight_layout()
    plot.figure.show()


# %%
# gráfico de barras por total, estado, CNAE e ano de abertura por estado
import matplotlib.pyplot as plt

sql = """SELECT 
    c.uf AS estado,
    c.cnae_fiscal_principal AS cnae_fiscal,
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d')) AS ano_abertura,
    COUNT(*) AS total
FROM read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/estabelecimentos.parquet') AS c
JOIN read_parquet('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/parquet/empresas.parquet') AS e
    ON c.cnpj_basico = e.cnpj_basico
WHERE 
    c.data_inicio_atividade IS NOT NULL
    AND LENGTH(c.data_inicio_atividade) = 8
GROUP BY 
    c.uf,
    c.cnae_fiscal_principal,
    EXTRACT(YEAR FROM STRPTIME(c.data_inicio_atividade, '%Y%m%d'))
ORDER BY c.uf, c.cnae_fiscal_principal, ano_abertura;"""

# Executa a query uma única vez
df = con.execute(sql).df()

# loop por estado
for uf in df['estado'].unique():
    
    df_uf = df[df['estado'] == uf].sort_values('ano_abertura')

    # 🔥 TOP 5 CNAEs por UF (com base no total acumulado)
    top_cnaes = (
        df_uf.groupby('cnae_fiscal')['total']
        .sum()
        .nlargest(5)
        .index
    )

    df_uf = df_uf[df_uf['cnae_fiscal'].isin(top_cnaes)]

    # pivot para gráfico
    plot = (
        df_uf
        .pivot_table(
            index='ano_abertura',
            columns='cnae_fiscal',
            values='total',
            aggfunc='sum'
        )
        .plot(
            kind='bar',
            stacked=False,  # 👈 lado a lado
            figsize=(12, 8)
        )
    )

    plot.set_xlabel('Ano de Abertura')
    plot.set_ylabel('Frequência')
    plot.set_title(f'Top 5 CNAEs por Ano - {uf}')

    plot.legend(
        title='CNAE',
        bbox_to_anchor=(1.05, 1),
        loc='upper left'
    )

    plot.set_xticklabels(plot.get_xticklabels(), rotation=45)

    plot.grid(axis='y')
    plot.figure.tight_layout()
    plot.figure.show()

# %%
