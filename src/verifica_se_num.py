#%%
# Verificar se TODOS os valores são só dígitos
#
# SELECT COUNT(*) AS invalidos
# FROM sua_tabela
# WHERE NOT regexp_matches(seu_campo, '^[0-9]{14}$');

import duckdb

con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')

query = """
SELECT 
    COUNT(*) AS total,
    SUM(CASE 
        WHEN cnpj_basico IS NULL OR NOT regexp_matches(cnpj_basico, '^[0-9]+$') 
        THEN 1 ELSE 0 
    END) AS invalidos
FROM raw.empresas
"""

total, invalidos = con.execute(query).fetchone()

percentual = (invalidos / total) * 100 if total else 0

print(f"Total: {total:,}")
print(f"Inválidos: {invalidos:,}")
print(f"Percentual: {percentual:.2f}%")

# %%
import pandas as pd

df = con.execute(query).df()
print(df)


# %%
# Plotar o resultado usando Matplotlib
import matplotlib.pyplot as plt
# Híbrido pesado → DuckDB
df = con.execute("""
SELECT natureza_juridica, COUNT(*)/1000000 as total
FROM raw.empresas
GROUP BY natureza_juridica
order by total desc
limit 10
""").df()

# leve → pandas
df.plot(kind='bar', x='natureza_juridica', y='total', legend=True)


# Plotando
ax = df.plot(
    kind='bar',
    x='natureza_juridica',
    y='total',
    legend=False,
    figsize=(10,6),
    color='skyblue'
)

# Títulos e rótulos
ax.set_ylabel("Total de empresas (milhões)")  # indica que os valores estão em milhares
ax.set_xlabel("Natureza Jurídica")
ax.set_title("Top 10 Naturezas Jurídicas por número de empresas")
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
# %%

query = """
SELECT 
    e.cnae_fiscal_principal,
    e.situacao_cadastral,
    COUNT(*) AS total
FROM raw.estabelecimentos e
JOIN raw.empresas emp
    ON e.cnpj_basico = emp.cnpj_basico
WHERE emp.natureza_juridica = '2135'
GROUP BY 
    e.cnae_fiscal_principal,
    e.situacao_cadastral
ORDER BY total DESC
LIMIT 20
"""

df = con.execute(query).df()

print(df)
df.plot(
    kind='bar',
    x='cnae_fiscal_principal',
    y='total',
    legend=True,
    figsize=(12,6),
    color='coral'
)
# %%
import duckdb

con = duckdb.connect('/home/rogerio/Área de Trabalho/DadosAbertosCNPJ/data/cnpj.duckdb')

query = """
SELECT 
    e.cnae_fiscal_principal,
    c.descricao AS cnae_descricao,
    e.situacao_cadastral,
    emp.natureza_juridica,
    nj.descricao AS natureza_descricao,
    COUNT(*) AS total
FROM raw.estabelecimentos e
JOIN raw.empresas emp
    ON e.cnpj_basico = emp.cnpj_basico
LEFT JOIN raw.cnae c
    ON e.cnae_fiscal_principal = c.codigo
LEFT JOIN raw.natureza_juridica nj
    ON emp.natureza_juridica = nj.codigo
where e.situacao_cadastral = '02' -- Ativa
GROUP BY 
    e.cnae_fiscal_principal,
    c.descricao,
    e.situacao_cadastral,
    emp.natureza_juridica,
    nj.descricao
ORDER BY total DESC
LIMIT 20
"""

resultados = con.execute(query).fetchall()

for row in resultados:
    cnae_fiscal_principal, cnae_descricao, situacao_cadastral, natureza_juridica, natureza_descricao, total = row

    print(f"CNAE: {cnae_fiscal_principal} - {cnae_descricao}")
    print(f"Situação Cadastral: {situacao_cadastral}")
    print(f"Natureza Jurídica: {natureza_juridica} - {natureza_descricao}")
    print(f"Total: {total:,}")
    print("-" * 80)# %%

# %%
import pandas as pd
import matplotlib.pyplot as plt

colunas = [
    'cnae',
    'cnae_descricao',
    'situacao',
    'natureza',
    'natureza_descricao',
    'total'
]

df = pd.DataFrame(resultados, columns=colunas)

df = df.sort_values(by='total', ascending=True)
df['total_mil'] = df['total'] / 1000

# cria uma coluna 'label' com no máximo 50 caracteres
df['label'] = df['cnae_descricao'].apply(lambda x: x[:50] + '...' if len(x) > 50 else x)

# plota usando a nova coluna
plt.figure(figsize=(10,6))

bars = plt.barh(df['label'], df['total_mil'], color='skyblue')

# adiciona valores nas barras
for bar in bars:
    width = bar.get_width()
    plt.text(width, bar.get_y() + bar.get_height()/2,
             f'{width:,.0f}',
             va='center')

plt.xlabel("Quantidade (milhares)")
plt.title("Top CNAEs - Empresário Individual (Ativos)")

plt.tight_layout()
plt.show()
# %%
