import os
from pathlib import Path

from dotenv import load_dotenv



# base do projeto


BASE_DIR = Path(__file__).resolve().parents[1]

ENV_FILE = os.getenv(
    "ENV_FILE",
    ".env"
)

ENV_PATH = (
    BASE_DIR
    / ENV_FILE
)

load_dotenv(
    dotenv_path=ENV_PATH,
    override=True
)





# producer e dados auxiliares


PRODUCER_DIR = (
    BASE_DIR
    / "producer"
)

PRODUCER_DADOS_DIR = (
    PRODUCER_DIR
    / "dados"
)

PRODUTOS_JSON_PATH = (
    PRODUCER_DADOS_DIR
    / "produtos.json"
)

FORMAS_PAGAMENTO_JSON_PATH = (
    PRODUCER_DADOS_DIR
    / "formas_pagamento.json"
)

CANAIS_JSON_PATH = (
    PRODUCER_DADOS_DIR
    / "canais.json"
)

LOJAS_JSON_PATH = (
    PRODUCER_DADOS_DIR
    / "lojas.json"
)



# diretório dos dados



DATA_DIR = Path(
    os.getenv(
        "DATA_DIR",
        BASE_DIR / "data"
    )
)

BRONZE_DIR = (
    DATA_DIR
    / "bronze"
)

BRONZE_VENDAS_DIR = (
    BRONZE_DIR
    / "vendas"
)

SILVER_DIR = (
    DATA_DIR
    / "silver"
)

SILVER_VENDAS_DIR = (
    SILVER_DIR
    / "vendas"
)

GOLD_DIR = (
    DATA_DIR
    / "gold"
)

GOLD_FATO_VENDAS_DIR = (
    GOLD_DIR
    / "fato_vendas"
)

GOLD_VENDAS_DIARIAS_DIR = (
    GOLD_DIR
    / "vendas_diarias"
)

GOLD_VENDAS_LOJA_DIR = (
    GOLD_DIR
    / "vendas_por_loja"
)

GOLD_VENDAS_PRODUTO_DIR = (
    GOLD_DIR
    / "vendas_por_produto"
)

GOLD_VENDAS_CATEGORIA_DIR = (
    GOLD_DIR
    / "vendas_por_categoria"
)

CONTROLE_DIR = (
    DATA_DIR
    / "controle"
)

BRONZE_SQLITE_PATH = (
    CONTROLE_DIR
    / "bronze_offsets.db"
)

SILVER_SQLITE_PATH = (
    CONTROLE_DIR
    / "silver_controle.db"
)



# kafka


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC_VENDAS = os.getenv(
    "KAFKA_TOPIC_VENDAS",
    "vendas-tempo-real"
)

KAFKA_GROUP_OLTP = os.getenv(
    "KAFKA_GROUP_OLTP",
    "grupo-consumer-vendas-postgres"
)

KAFKA_GROUP_BRONZE = os.getenv(
    "KAFKA_GROUP_BRONZE",
    "grupo-consumer-vendas-bronze"
)



# postgres


POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = int(
    os.getenv(
        "POSTGRES_PORT",
        "5433"
    )
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "projeto_vendas"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "postgres"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "postgres"
)

POSTGRES_ADMIN_DB = os.getenv(
    "POSTGRES_ADMIN_DB",
    "postgres"
)



# pgadmin


PGADMIN_EMAIL = os.getenv(
    "PGADMIN_EMAIL",
    "admin@admin.com"
)

PGADMIN_PASSWORD = os.getenv(
    "PGADMIN_PASSWORD",
    "admin"
)



# bronze


BRONZE_MAX_REGISTROS_POR_ARQUIVO = int(
    os.getenv(
        "BRONZE_MAX_REGISTROS_POR_ARQUIVO",
        "10000"
    )
)



# outras configurações


TIMEZONE_PADRAO = os.getenv(
    "TIMEZONE_PADRAO",
    "UTC"
)

LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "INFO"
)