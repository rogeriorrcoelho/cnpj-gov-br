# 📂 Diretório de Dados (`data/`)

Diretório de armazenamento de arquivos de dados utilizados no projeto.
Na raiz do diretório data é utilizado um banco de dados local em DuckDB (cnpj.duckdb), responsável por apoiar o processamento e a conversão dos dados.
Os arquivos aqui presentes **não são versionados pelo Git** (ver `.gitignore`), pois são grandes e podem ser obtidos do portal gov.br 

---

## 📁 Estrutura

```
data/
├── csv/
├── csv_utf8/
└── parquet/
```

### 🔹 `csv/`

Armazena arquivos no formato CSV **originais**, conforme obtidos das fontes de dados.

* Codificação pode variar (ex: `latin1`, `utf-8`, etc.)
* Não realizar alterações manuais nesses arquivos
* Servem como fonte bruta para processamento

---

### 🔹 `csv_utf8/`

Contém versões dos arquivos CSV convertidos para **UTF-8**.

* Padronização de encoding para evitar erros de leitura
* Utilizados em pipelines de processamento
* Gerados a partir dos arquivos da pasta `csv/`

---

### 🔹 `parquet/`

Armazena arquivos no formato **Parquet**.

* Formato colunar otimizado para performance
* Utilizado para análise e processamento eficiente
* Gerado a partir dos dados em `csv_utf8/`

---

🗄️ Banco de Dados Local
cnpj.duckdb

Arquivo localizado na raiz do projeto:

./cnpj.duckdb
Banco de dados local utilizado com DuckDB
Usado para leitura, transformação e exportação dos dados
Responsável pela conversão dos arquivos para formato Parquet
Pode ser recriado a qualquer momento a partir dos dados brutos

---

🔄 Fluxo de Dados

O fluxo de processamento:

csv/ → csv_utf8/ → DuckDB (cnpj.duckdb) → parquet/
Dados brutos são colocados em csv/
São convertidos para UTF-8 em csv_utf8/
São carregados no DuckDB (cnpj.duckdb)
São transformados e exportados para parquet/

