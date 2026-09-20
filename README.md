# Projeto OLTP, Arquitetura Medalhão e OLAP

Projeto de Engenharia de Dados desenvolvido para simular um fluxo completo de vendas em tempo real, desde a geração dos eventos até sua disponibilização em um modelo analítico OLAP.

A solução utiliza Apache Kafka para mensageria, PostgreSQL como banco transacional e analítico, JSONL e Parquet para implementação da arquitetura Medalhão e Python para produtores, consumidores, pipelines e automações.

# 1 Quick Start

git clone kafka-postres-medallion-olap
cd kafka-postgres-medallion-olap

# 2 Crie um ambiente virtual

python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3 Instale as dependências

pip install -r requirements.txt

# 4 Criar os arquivos de ambiente

Copy-Item ".env.example" ".env"
Copy-Item ".env.test.example" ".env.test"

# 5 Subir a infraestrutura

docker compose up -d

# 6 Inicialize o banco

python -m database.init_database

# 7 Execute em tempo real
# terminal 1
python -m consumer.consumer

# terminal 2
python -m bronze.consumer_bronze

# terminal 3
python -m producer.producer

# 8 Execute o pipeline batch
python -m scripts.run_pipeline

# 9 Valide o resultado
python -m scripts.validate_pipeline

---

## Arquitetura

```text
                         ┌─────────────────┐
                         │    Producer     │
                         │     Python      │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │  Apache Kafka   │
                         │ vendas-tempo-real│
                         └───────┬─────────┘
                                 │
                  ┌──────────────┴───────────────┐
                  │                              │
                  ▼                              ▼
        ┌──────────────────┐          ┌──────────────────┐
        │ Consumer OLTP    │          │ Consumer Bronze  │
        │ Python           │          │ Python           │
        └────────┬─────────┘          └────────┬─────────┘
                 │                             │
                 ▼                             ▼
        ┌──────────────────┐          ┌──────────────────┐
        │ PostgreSQL OLTP  │          │ Bronze - JSONL   │
        └──────────────────┘          └────────┬─────────┘
                                              │
                                              ▼
                                     ┌──────────────────┐
                                     │ Silver - Parquet │
                                     └────────┬─────────┘
                                              │
                                              ▼
                                     ┌──────────────────┐
                                     │ Gold - Parquet   │
                                     └────────┬─────────┘
                                              │
                                              ▼
                                     ┌──────────────────┐
                                     │ PostgreSQL OLAP  │
                                     │ Modelo Estrela   │
                                     └──────────────────┘
```

---

## Tecnologias utilizadas

- Python 3.11
- Apache Kafka
- PostgreSQL 16
- Docker / Docker Compose
- Pandas
- PyArrow
- kafka-python
- psycopg2
- SQLite
- JSON / JSONL
- Parquet
- python-dotenv

---

## Estrutura do projeto

```text
projeto_consumer/
│
├── bronze/
│   ├── __init__.py
│   ├── consumer_bronze.py
│   └── inicializar_controle.py
│
├── consumer/
│   ├── __init__.py
│   └── consumer.py
│
├── producer/
│   ├── __init__.py
│   ├── producer.py
│   └── dados/
│       ├── produtos.json
│       ├── formas_pagamento.json
│       ├── canais.json
│       └── lojas.json
│
├── silver/
│   ├── __init__.py
│   └── bronze_to_silver.py
│
├── gold/
│   ├── __init__.py
│   ├── silver_to_gold.py
│   └── agregacoes_gold.py
│
├── olap/
│   ├── __init__.py
│   └── gold_to_olap.py
│
├── config/
│   ├── __init__.py
│   └── settings.py
│
├── database/
│   ├── __init__.py
│   ├── init_database.py
│   └── sql/
│       ├── 01_create_schemas.sql
│       ├── 02_create_oltp_reference_tables.sql
│       ├── 03_create_oltp_transaction_tables.sql
│       ├── 04_create_control_tables.sql
│       ├── 05_insert_initial_data.sql
│       ├── 06_create_oltp_indexes.sql
│       ├── 07_create_olap_dimensions.sql
│       ├── 08_create_olap_fact.sql
│       └── 09_create_olap_indexes.sql
│
├── scripts/
│   ├── __init__.py
│   ├── setup_test_environment.py
│   ├── run_pipeline.py
│   └── validate_pipeline.py
│
├── docker-compose.yaml
├── .env
├── .env.test
├── .gitignore
├── requirements.txt
└── README.md
```

Os dados da arquitetura Medalhão são mantidos fora da pasta do código.

Exemplo:

```text
C:\projeto_dados
├── bronze
├── silver
├── gold
└── controle
```

Essa separação evita bloqueios e sincronização indevida de arquivos Parquet/JSONL quando o código está armazenado em OneDrive.

---

## Instalação

### 1. Criar ambiente virtual

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Instalar dependências

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Subir infraestrutura

```powershell
docker compose up -d
```

Verifique:

```powershell
docker compose ps
```

O PostgreSQL do projeto utiliza externamente:

```text
localhost:5433
```

O Kafka utiliza:

```text
localhost:9092
```

---

## Configuração

As configurações são centralizadas em:

```text
config/settings.py
```

### `.env`

Exemplo de ambiente principal:

```env
DATA_DIR=C:\projeto_dados

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_VENDAS=vendas-tempo-real

KAFKA_GROUP_OLTP=grupo-consumer-vendas-postgres
KAFKA_GROUP_BRONZE=grupo-consumer-vendas-bronze

POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=projeto_vendas
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

BRONZE_MAX_REGISTROS_POR_ARQUIVO=10000
```

### `.env.test`

Exemplo de ambiente isolado de teste:

```env
DATA_DIR=C:\projeto_dados_teste

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_VENDAS=vendas-tempo-real-teste

KAFKA_GROUP_OLTP=grupo-consumer-vendas-postgres-teste
KAFKA_GROUP_BRONZE=grupo-consumer-vendas-bronze-teste

POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=projeto_vendas_teste
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

BRONZE_MAX_REGISTROS_POR_ARQUIVO=10000
```

Para usar o ambiente de teste no PowerShell:

```powershell
$env:ENV_FILE=".env.test"
```

Para voltar ao ambiente padrão:

```powershell
Remove-Item Env:ENV_FILE -ErrorAction SilentlyContinue
```

---

## Inicialização automática do banco

Não é necessário executar os SQLs manualmente.

```powershell
python -m database.init_database
```

O inicializador cria/verifica o banco e executa, em ordem:

```text
01_create_schemas.sql
02_create_oltp_reference_tables.sql
03_create_oltp_transaction_tables.sql
04_create_control_tables.sql
05_insert_initial_data.sql
06_create_oltp_indexes.sql
07_create_olap_dimensions.sql
08_create_olap_fact.sql
09_create_olap_indexes.sql
```

Os scripts foram preparados para permitir reexecução sem duplicar estrutura ou dados iniciais.

---

## OLTP

Principais tabelas:

```text
oltp.cliente
oltp.loja
oltp.categoria
oltp.produto
oltp.forma_pagamento
oltp.canal
oltp.venda
oltp.item_venda
```

Tabelas técnicas:

```text
controle.evento_processado
controle.erro_processamento
```

O consumer OLTP confirma o offset Kafka somente após o commit no PostgreSQL.

---

## Arquitetura Medalhão

### Bronze

A Bronze preserva o evento original e adiciona metadata técnica.

O controle físico usa:

```text
topic + partition + offset
```

### Silver

A Silver:

- lê apenas offsets novos;
- tipa e valida dados;
- aplica regras de qualidade;
- remove duplicidade física;
- remove duplicidade lógica por `id_evento`;
- grava Parquet particionado por ano/mês.

### Gold

Estrutura:

```text
gold/
├── fato_vendas
├── vendas_diarias
├── vendas_por_loja
├── vendas_por_produto
└── vendas_por_categoria
```

---

## OLAP

Dimensões:

```text
olap.dim_data
olap.dim_cliente
olap.dim_loja
olap.dim_categoria
olap.dim_produto
olap.dim_canal
olap.dim_forma_pagamento
```

Fato:

```text
olap.fato_vendas
```

A fato utiliza `id_evento` único para preservar idempotência.

---

## Executando o fluxo em tempo real

Execute os comandos a partir da raiz do projeto.

### Terminal 1

```powershell
python -m consumer.consumer
```

### Terminal 2

```powershell
python -m bronze.consumer_bronze
```

### Terminal 3

```powershell
python -m producer.producer
```

Interrompa o producer com `Ctrl+C`.

---

## Executando o pipeline batch

Individualmente:

```powershell
python -m silver.bronze_to_silver
python -m gold.silver_to_gold
python -m gold.agregacoes_gold
python -m olap.gold_to_olap
```

Ou:

```powershell
python -m scripts.run_pipeline
```

---

## Teste isolado

Ative:

```powershell
$env:ENV_FILE=".env.test"
```

Prepare:

```powershell
python -m scripts.setup_test_environment
```

Depois, em três terminais separados, execute o consumer OLTP, consumer Bronze e producer, sempre com `ENV_FILE=.env.test`.

Após gerar eventos:

```powershell
python -m scripts.run_pipeline
python -m scripts.validate_pipeline
```

Durante o desenvolvimento, um teste isolado produziu:

```text
21 vendas OLTP
21 itens OLTP
21 eventos processados
21 registros OLAP
```

---

## Validação SQL

```sql
SELECT COUNT(*) AS vendas
FROM oltp.venda;

SELECT COUNT(*) AS itens
FROM oltp.item_venda;

SELECT COUNT(*) AS eventos
FROM controle.evento_processado;

SELECT
    COUNT(*) AS registros,
    SUM(valor_total) AS receita,
    SUM(quantidade) AS quantidade
FROM olap.fato_vendas;
```

---

## Idempotência

- Bronze: `topic + partition + offset`
- Silver: `id_evento`
- OLTP: `controle.evento_processado`
- OLAP: `UNIQUE(id_evento)`

---

## Execução como módulos

Use:

```powershell
python -m producer.producer
```

Evite:

```powershell
python producer\producer.py
```

porque o projeto compartilha configurações por `config.settings`.

---

## Objetivos técnicos demonstrados

- arquitetura orientada a eventos;
- Kafka;
- consumer groups;
- controle de offsets;
- processamento incremental;
- idempotência;
- transações PostgreSQL;
- OLTP;
- arquitetura Medalhão;
- JSONL;
- Parquet;
- particionamento;
- checkpoints SQLite;
- modelagem dimensional;
- modelo estrela;
- surrogate keys;
- OLAP;
- configuração por ambiente;
- bootstrap automático do banco;
- reconciliação ponta a ponta;
- Docker.

---

## Possíveis evoluções

- Dead Letter Queue;
- logging estruturado;
- observabilidade;
- métricas;
- Gold incremental;
- carga OLAP em lote;
- SCD Tipo 2;
- testes automatizados;
- CI/CD;
- Airflow;
- cloud storage;
- Schema Registry;
- Avro / Protobuf;
- data quality framework;
- Power BI.

---

## Autor

Projeto desenvolvido como estudo prático de Engenharia de Dados, integrando sistemas transacionais, streaming, Data Lake, arquitetura Medalhão e Data Warehouse.
