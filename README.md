# 🏢 Grupo SBF - Analytics Engineer Case

> **Case Técnico 2**: Pipeline de dados com arquitetura Medallion (Trusted → Refined)

## 📋 Índice

- [Sobre o Projeto](#sobre-o-projeto)
- [Arquitetura](#arquitetura)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Configuração do Ambiente](#configuração-do-ambiente)
- [Como Executar](#como-executar)
- [Modelo de Dados](#modelo-de-dados)
- [Governança e LGPD](#governança-e-lgpd)
- [Orquestração](#orquestração)
- [Validações e Qualidade](#validações-e-qualidade)
- [Monitoramento](#monitoramento)
- [Autora](#autora)

---

## 🎯 Sobre o Projeto

Este projeto implementa um pipeline de dados completo para análise de vendas do Grupo SBF, seguindo as melhores práticas de engenharia de dados:

- **Arquitetura Medallion** (Trusted → Refined)
- **Governança de Dados** com conformidade LGPD
- **Orquestração** via Apache Airflow
- **Validações** automáticas de qualidade
- **Performance** otimizada com índices e particionamento

### Objetivos

1. Ingerir dados de vendas de múltiplas fontes (CSV)
2. Criar camada Trusted com dados brutos validados
3. Criar camada Refined com agregações e métricas de negócio
4. Garantir conformidade com LGPD (pseudonimização de dados sensíveis)
5. Orquestrar todo o pipeline com Airflow

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│                    FONTES DE DADOS                      │
│  (CSV: pedido, pedido_item, produto, marca, meta, etc) │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│              CAMADA TRUSTED (Silver)                    │
│  • Dados normalizados (3NF)   │
│  • Validações de integridade                            │
│  • Logs de auditoria                                    │
│  • Conformidade LGPD (PII separado)                     │
│                                                          │
│  Tabelas Mestres:                                       │
│  ├── marca                                              │
│  ├── produto                                            │
│  ├── categoria (hierárquica)                            │
│  ├── geografia (cidade, UF, região)                     │
│  ├── canal (online, offline, marketplace)               │
│  ├── campanha (marketing)                               │
│  ├── data (dimensão temporal)                           │
│  ├── feriados                                           │
│  └── tipo_meta                                          │
│                                                          │
│  Transações/Eventos:                                    │
│  ├── pedido                                             │
│  ├── pedido_item                                        │
│  ├── pedido_status_historico (event log)                │
│  ├── produto_preco_historico (event log)                │
│  ├── metas (multidimensional)                           │
│  ├── cliente_pii (dados sensíveis)                      │
│  ├── cliente_pseudo (dados pseudonimizados)             │
│  └── log_ingestao (auditoria)                           │
│                                                          │
│  Governança:                                            │
│  ├── metadados_carga                                    │
│  ├── qualidade_dados                                    │
│  └── auditoria_mudancas                                 │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│              CAMADA REFINED (Gold)                      │
│  • Star Schema (dim_/fato_ prefixos)                    │
│  • SCD Type 2 para histórico                            │
│  • Agregações e métricas de negócio                     │
│  • Dados prontos para consumo analítico                 │
│                                                          │
│  Dimensões (SCD Type 2):                                │
│  ├── dim_produto                                        │
│  ├── dim_geografia                                      │
│  ├── dim_canal                                          │
│  ├── dim_campanha                                       │
│  ├── dim_cliente (RFM, segmentação)                     │
│  └── dim_tempo                                          │
│                                                          │
│  Fatos:                                                 │
│  └── fato_vendas (fato principal)                       │
│                                                          │
│  Marts Analíticos:                                      │
│  ├── mais_vendidos_mensal_estado                        │
│  ├── performance_mensal_marca                           │
│  ├── top10_best_sellers_regiao                          │
│  └── kpis_vendas (views materializadas)                 │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│              CONSUMO (BI / Analytics)                   │
│  • Dashboards                                           │
│  • Relatórios                                           │
│  • APIs                                                 │
└─────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Versão | Uso |
|-----------|--------|-----|
| **Python** | 3.10+ | Linguagem principal |
| **PostgreSQL** | 14+ | Banco de dados (RDS) |
| **Apache Airflow** | 2.10.2 | Orquestração |
| **SQLAlchemy** | 1.4.52 | ORM e conexão com DB |
| **Pandas** | 2.2.3 | Manipulação de dados |
| **psycopg2** | 2.9.9 | Driver PostgreSQL |

---

## 📁 Estrutura do Projeto

```
sbf_case_ae_db/
├── README.md                          # 📖 Este arquivo
├── .env                               # 🔐 Template de variáveis de ambiente
├── .gitignore                         # 🚫 Arquivos ignorados pelo Git
├── requirements.txt                   # 📦 Dependências Python
├── run_pipeline.sh                    # 🚀 Script de execução do pipeline
│
├── data/                              # 📊 Dados de entrada/saída
│   └── trusted/                       # CSVs fonte
│       ├── marca.csv
│       ├── produto.csv
│       ├── pedido.csv
│       ├── pedido_item.csv
│       ├── data.csv
│       └── meta.csv
│
├── script/                            # 🐍 Scripts Python
│   ├── ddl.sql                        # 📝 DDL completo do banco
│   │
│   ├── ingestao/                      # 📥 Ingestão (Trusted)
│   │   └── load_data_rds.py           # Carga de CSVs → Trusted
│   │
│   ├── transformacao/                 # 🔄 Transformação (Refined)
│   │   └── transform_refined.py       # Criação de tabelas Refined
│   │
│   └── validacao/                     # ✅ Validações de qualidade
│       ├── validate_trusted.py        # Validações camada Trusted
│       └── validate_refined.py        # Validações camada Refined
│
├── dags/                              # 🌀 DAGs do Airflow
│   └── sbf_pipeline_dag.py            # DAG principal do pipeline

```

---

## ⚙️ Pré-requisitos

Antes de começar, certifique-se de ter instalado:

- **Python 3.10+**
- **PostgreSQL 14+** (ou acesso a instância RDS)
- **Git**
- **pip** (gerenciador de pacotes Python)

### Opcional (para orquestração)
- **Apache Airflow 2.10+**
- **Docker** (para executar Airflow em container)

---

## 🔧 Configuração do Ambiente

### 1. Clone o Repositório

```bash
git clone <url-do-repositorio>
cd sbf_case_ae_db
```

### 2. Crie e Ative o Ambiente Virtual

**Linux/Mac:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows:**
```powershell
python -m venv .venv
.venv\Scripts\activate
```

### 3. Instale as Dependências

```bash
pip install -r requirements.txt
```

### 4. Configure as Variáveis de Ambiente

Copie o arquivo `.env.example` para `.env`:

```bash
cp .env.example .env
```

Edite o arquivo `.env` com suas credenciais:

```ini
# Conexão com PostgreSQL (RDS ou local)
DB_HOST=seu-endpoint-rds.amazonaws.com
DB_PORT=5432
DB_NAME=sbf_case_ae
DB_USER=seu_usuario
DB_PASS=sua_senha

# Configurações opcionais
DATE_LANG=pt_BR
```

### 5. Crie o Banco de Dados

Execute o DDL para criar as estruturas:

```bash
psql -h $DB_HOST -U $DB_USER -d postgres -c "CREATE DATABASE sbf_case_ae;"
psql -h $DB_HOST -U $DB_USER -d sbf_case_ae -f script/ddl.sql
```

---

## 🚀 Como Executar

### Opção 1: Executar Pipeline Completo (via Shell Script)

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

### Opção 2: Executar Etapas Individualmente

**1. Ingestão (Trusted):**
```bash
python script/ingestao/load_data_rds.py
```

**2. Transformação (Refined):**
```bash
python script/transformacao/transform_refined.py
```

**3. Validações:**
```bash
python script/validacao/validate_trusted.py
python script/validacao/validate_refined.py
```

### Opção 3: Orquestração via Airflow

**1. Inicialize o Airflow:**
```bash
airflow db init
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

**2. Inicie os serviços:**
```bash
airflow webserver -p 8080 &
airflow scheduler &
```

**3. Acesse o Airflow:**
- URL: http://localhost:8080
- Usuário: admin
- Senha: admin

**4. Ative a DAG `sbf_pipeline_dag`**

---

## 📊 Modelo de Dados

### Camada TRUSTED (Schema: `trusted`) - Silver

**Características:**
- Dados normalizados (3NF) 
- Granularidade: 1 linha = 1 evento/transação
- Fonte da verdade 

#### Tabelas Mestres Normalizadas

**Tabela: `marca`**
Marcas de produtos.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `nome` | VARCHAR(100) | Nome da marca |
| `criado_em` | TIMESTAMP | Data de criação |
| `atualizado_em` | TIMESTAMP | Última atualização |

**Tabela: `produto`**
Catálogo de produtos.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `id_marca` | INTEGER | FK → marca.id |
| `id_categoria` | INTEGER | FK → categoria.id_categoria |
| `nome` | VARCHAR(255) | Nome do produto |
| `descricao` | TEXT | Descrição detalhada |
| `criado_em` | TIMESTAMP | Data de criação |
| `atualizado_em` | TIMESTAMP | Última atualização |

**Tabela: `categoria`** 
Hierarquia de categorias (Departamento > Categoria > Subcategoria).

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_categoria` | INTEGER | PK - Identificador único |
| `nome` | VARCHAR(100) | Nome da categoria |
| `id_categoria_pai` | INTEGER | FK → categoria.id_categoria |
| `nivel` | INTEGER | Nível hierárquico (1-3) |
| `caminho_completo` | VARCHAR(500) | Caminho completo (ex: "Calçados > Tênis > Tênis Corrida") |

**Tabela: `geografia`** 
Dados geográficos completos (cidade, UF, região, coordenadas).

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_geografia` | INTEGER | PK - Identificador único |
| `cidade` | VARCHAR(100) | Nome da cidade |
| `uf` | CHAR(2) | Unidade Federativa |
| `regiao` | VARCHAR(50) | Região (Sudeste, Sul, etc.) |
| `cep` | VARCHAR(10) | CEP |
| `latitude` | NUMERIC(10,7) | Coordenada latitude |
| `longitude` | NUMERIC(10,7) | Coordenada longitude |

**Tabela: `canal`** 
Canais de vendas (online, offline, marketplace).

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_canal` | INTEGER | PK - Identificador único |
| `nome` | VARCHAR(100) | Nome do canal |
| `tipo` | VARCHAR(50) | Tipo (Online, Offline, Marketplace) |
| `plataforma` | VARCHAR(100) | Plataforma (ex: "Site", "App", "Loja Física") |

**Tabela: `campanha`** 
Campanhas de marketing.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_campanha` | INTEGER | PK - Identificador único |
| `nome` | VARCHAR(200) | Nome da campanha |
| `tipo` | VARCHAR(50) | Tipo (Email, SMS, Digital, etc.) |
| `data_inicio` | DATE | Data de início |
| `data_fim` | DATE | Data de fim |
| `investimento` | NUMERIC(12,2) | Investimento total |

**Tabela: `data`** (Dimensão Temporal)
Dimensão de datas enriquecida.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `data` | DATE | PK - Data |
| `ano` | INTEGER | Ano |
| `mes` | INTEGER | Mês (1-12) |
| `dia` | INTEGER | Dia (1-31) |
| `trimestre` | INTEGER | Trimestre (1-4) |
| `semestre` | INTEGER | Semestre (1-2) |
| `eh_feriado` | BOOLEAN | É feriado? (sincronizado com `feriado`) |
| `nome_feriado` | VARCHAR(100) | Nome do feriado (sincronizado com `feriado`) |
| `tipo_feriado` | VARCHAR(50) | Tipo do feriado (sincronizado com `feriado`) |
| `descricao` | VARCHAR(50) | Descrição (ex: "Segunda-feira, 01 Janeiro 2024") |

**Tabela: `feriado`** 
Feriados nacionais, estaduais, municipais e comerciais.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `data` | DATE | FK → data.data (UNIQUE) |
| `nome` | VARCHAR(100) | Nome do feriado |
| `tipo` | VARCHAR(50) | Tipo (Nacional, Estadual, Municipal, Comercial) |
| `uf` | CHAR(2) | UF (NULL = nacional) |
| `cidade` | VARCHAR(100) | Cidade (NULL = não municipal) |
| `criado_em` | TIMESTAMP | Data de criação |

**Relacionamento:**
- `feriado.data` → `data.data` (FK com sincronização automática via trigger)
- Quando um feriado é inserido/atualizado, `trusted.data` é atualizado automaticamente

#### Tabelas de Transações

**Tabela: `pedido`**
Pedidos realizados (atualizada com FKs para novas dimensões).

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `data` | DATE | Data do pedido |
| `status` | VARCHAR(50) | Status (FINALIZADO, CANCELADO) |
| `id_geografia_entrega` | INTEGER | FK → geografia.id_geografia |
| `id_canal` | INTEGER | FK → canal.id_canal |
| `id_campanha` | INTEGER | FK → campanha.id_campanha |
| `vlr_total` | NUMERIC(12,2) | Valor total do pedido |
| `vlr_desconto` | NUMERIC(12,2) | Valor do desconto |
| `vlr_frete` | NUMERIC(12,2) | Valor do frete |
| `cliente_id_hash` | CHAR(64) | Hash do cliente (LGPD) |

**Tabela: `pedido_item`**
Itens dos pedidos.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `id_pedido` | INTEGER | FK → pedido.id |
| `id_produto` | INTEGER | FK → produto.id |
| `flg_cancelado` | CHAR(1) | Item cancelado? (S/N) |
| `qtd_produto` | INTEGER | Quantidade |
| `vlr_unitario` | NUMERIC(10,2) | Valor unitário |

**Tabela: `pedido_status_historico`** 
Event log de mudanças de status de pedidos.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `id_pedido` | INTEGER | FK → pedido.id |
| `status_anterior` | VARCHAR(50) | Status anterior |
| `status_novo` | VARCHAR(50) | Status novo |
| `data_mudanca` | TIMESTAMP | Data/hora da mudança |

**Tabela: `produto_preco_historico`** 
Event log de mudanças de preço de produtos.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `id_produto` | INTEGER | FK → produto.id |
| `preco_anterior` | NUMERIC(10,2) | Preço anterior |
| `preco_novo` | NUMERIC(10,2) | Preço novo |
| `data_inicio` | DATE | Data de início da vigência |
| `data_fim` | DATE | Data de fim da vigência (NULL = atual) |

**Tabela: `metas`** 
Metas flexíveis e multidimensionais.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `id_tipo_meta` | INTEGER | FK → tipo_meta.id |
| `id_marca` | INTEGER | FK → marca.id (opcional) |
| `id_categoria` | INTEGER | FK → categoria.id_categoria (opcional) |
| `id_canal` | INTEGER | FK → canal.id_canal (opcional) |
| `id_geografia` | INTEGER | FK → geografia.id_geografia (opcional) |
| `periodo_inicio` | DATE | Período de início |
| `periodo_fim` | DATE | Período de fim |
| `valor` | NUMERIC(12,2) | Valor da meta |

### Camada REFINED (Schema: `refined`) - Gold

**Características:**
- Star Schema (dimensões + fatos) - **COM prefixos dim_/fato_**
- SCD Type 2 para histórico (valid_from, valid_to)
- Agregações pré-calculadas para performance
- Dados prontos para BI/Analytics

#### Dimensões (SCD Type 2)

**Tabela: `dim_produto`**
Dimensão de produtos com histórico.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `id_produto_trusted` | INTEGER | FK → trusted.produto.id |
| `nome` | VARCHAR(255) | Nome do produto |
| `id_marca` | INTEGER | FK → dim_marca.id |
| `id_categoria` | INTEGER | FK → dim_categoria.id |
| `preco_sugerido` | NUMERIC(10,2) | Preço sugerido |
| `valid_from` | TIMESTAMP | Início da vigência |
| `valid_to` | TIMESTAMP | Fim da vigência (NULL = atual) |
| `is_current` | BOOLEAN | Registro atual? |

**Tabela: `dim_geografia`**
Dimensão geográfica com histórico.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `id_geografia_trusted` | INTEGER | FK → trusted.geografia.id_geografia |
| `cidade` | VARCHAR(100) | Nome da cidade |
| `uf` | CHAR(2) | Unidade Federativa |
| `regiao` | VARCHAR(50) | Região |
| `valid_from` | TIMESTAMP | Início da vigência |
| `valid_to` | TIMESTAMP | Fim da vigência (NULL = atual) |
| `is_current` | BOOLEAN | Registro atual? |

**Tabela: `dim_cliente`**
Dimensão de clientes com segmentação RFM.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `cliente_id_hash` | CHAR(64) | Hash do cliente (LGPD) |
| `segmento` | VARCHAR(50) | Segmento RFM (VIP, Regular, etc.) |
| `score_rfm` | INTEGER | Score RFM (1-5) |
| `recencia_dias` | INTEGER | Recência em dias |
| `frequencia_compras` | INTEGER | Frequência de compras |
| `valor_total` | NUMERIC(12,2) | Valor total (LTV) |
| `ticket_medio` | NUMERIC(10,2) | Ticket médio |
| `data_snapshot` | DATE | Data do snapshot |
| `valid_from` | TIMESTAMP | Início da vigência |
| `valid_to` | TIMESTAMP | Fim da vigência (NULL = atual) |
| `is_current` | BOOLEAN | Registro atual? |

#### Fatos

**Tabela: `fato_vendas`**
Fato principal de vendas (Star Schema).

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | PK - Identificador único |
| `id_tempo` | INTEGER | FK → dim_tempo.id |
| `id_canal` | INTEGER | FK → dim_canal.id |
| `id_campanha` | INTEGER | FK → dim_campanha.id |
| `id_geografia` | INTEGER | FK → dim_geografia.id |
| `id_cliente` | INTEGER | FK → dim_cliente.id |
| `id_produto` | INTEGER | FK → dim_produto.id |
| `qtd_vendida` | INTEGER | Quantidade vendida |
| `vlr_total` | NUMERIC(12,2) | Valor total |
| `vlr_desconto` | NUMERIC(12,2) | Valor do desconto |
| `vlr_frete` | NUMERIC(12,2) | Valor do frete |

#### Marts Analíticos

**Tabela: `mais_vendidos_mensal_estado`**
TOP produtos mais vendidos por estado/mês.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `mes_ano` | DATE | Mês/Ano de referência |
| `sgl_uf_entrega` | CHAR(2) | Estado |
| `id_produto` | INTEGER | ID do produto |
| `nome_produto` | VARCHAR(255) | Nome do produto |
| `total_qtd` | INTEGER | Quantidade total vendida |
| `posicao` | INTEGER | Ranking (1 = mais vendido) |

#### View Materializada: `top10_best_sellers_regiao` 
Top 10 produtos mais vendidos por **região geográfica** e mês 

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `mes_referencia` | DATE | Primeiro dia do mês |
| `ano` | INTEGER | Ano |
| `mes` | INTEGER | Mês (1-12) |
| `regiao` | VARCHAR | Região (Sudeste, Sul, Nordeste, etc.) |
| `ranking` | INTEGER | Ranking na região (1 = mais vendido) |
| `nome_produto` | VARCHAR | Nome do produto |
| `marca` | VARCHAR | Marca do produto |
| `qtd_vendida` | INTEGER | Quantidade total vendida |
| `qtd_pedidos` | INTEGER | Número de pedidos |
| `receita_total` | NUMERIC | Receita total gerada |
| `preco_medio_unitario` | NUMERIC | Preço médio por unidade |
| `participacao_pct` | NUMERIC | % de participação nas vendas da região |

**Uso:**
```sql
-- Top 10 do Sudeste no último mês
SELECT * FROM refined.top10_best_sellers_regiao
WHERE regiao = 'Sudeste' AND ranking <= 10
ORDER BY ranking;
```

**Documentação:** Ver `RESPOSTA_TOP10_BEST_SELLERS.md` e `script/CONSULTA_TOP10_PRODUTOS_BEST_SELLERS.sql`

#### Tabela: `performance_mensal_marca`
Performance de vendas vs meta por marca.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ano` | INTEGER | Ano |
| `mes` | INTEGER | Mês |
| `id` | INTEGER | ID da marca |
| `nome_marca` | VARCHAR(100) | Nome da marca |
| `vlr_total_vendido` | NUMERIC(14,2) | Total vendido |
| `vlr_meta` | NUMERIC(14,2) | Meta do período |
| `perc_atingimento_meta` | NUMERIC(5,2) | % de atingimento |

---

## 🔒 Governança e LGPD

O projeto implementa **conformidade com a LGPD** através de:

### Pseudonimização de Dados Sensíveis

- **Tabela `cliente_pii`**: Armazena dados pessoais identificáveis (PII)
  - Nome completo, CPF, email, telefone, endereço
  - Acesso restrito e auditado

- **Tabela `cliente_pseudo`**: Dados pseudonimizados
  - Hash SHA-256 do ID do cliente
  - Usada em todas as análises

### Auditoria

- **Tabela `log_ingestao`**: Registra todas as cargas de dados
  - Quem executou
  - Quando executou
  - Quantos registros foram inseridos

### Dicionário de Dados

- **Tabela `dicionario_de_dados`**: Documenta todas as colunas
  - Descrição do campo
  - Exemplos de valores

---

## 🌀 Orquestração

A DAG do Airflow (`sbf_pipeline_dag`) executa o pipeline completo:

```python
trusted_ingest → refined_transform → validate_refined
```

### Configurações da DAG

- **Schedule**: Diário (`@daily`)
- **Retries**: 1 tentativa
- **Retry Delay**: 5 minutos
- **Catchup**: Desabilitado

### Tasks

1. **trusted_ingest**: Carga de CSVs → Trusted
2. **refined_transform**: Criação de tabelas Refined
3. **validate_refined**: Validações de qualidade

---

## ✅ Validações e Qualidade

### Validações Automáticas (Trusted)

- ✅ Contagem de registros por tabela
- ✅ Verificação de integridade referencial (FKs)
- ✅ Detecção de valores nulos em campos obrigatórios
- ✅ Verificação de duplicatas
- ✅ Validação de formatos (UF, datas)

### Validações Automáticas (Refined)

- ✅ Consistência de agregações
- ✅ Verificação de totais
- ✅ Validação de rankings
- ✅ Detecção de anomalias

---

## 📈 Monitoramento

### Logs

Todos os scripts geram logs estruturados:

```
🟢 Iniciando carga: pedido
✅ pedido carregada com sucesso: 1500 linhas.
🟢 Gerando tabela refined.mais_vendidos_mensal_estado...
✅ Tabela refined.mais_vendidos_mensal_estado criada com sucesso.
```

### Métricas Disponíveis

- Tempo de execução por etapa
- Volume de dados processados
- Taxa de erro por tabela
- Performance de queries

---

## 👩‍💻 Autora

**Camila Macedo**
- Analytics Engineer
- Caso técnico para o Grupo SBF


# Caso_Tecnico_Grupo_SBF
