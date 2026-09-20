import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config.settings import (
    BRONZE_VENDAS_DIR,
    SILVER_VENDAS_DIR,
    CONTROLE_DIR,
    SILVER_SQLITE_PATH,
)



# Aliases locais


BRONZE_DIR = BRONZE_VENDAS_DIR
SILVER_DIR = SILVER_VENDAS_DIR
SQLITE_PATH = SILVER_SQLITE_PATH


# Estrutura esperada


COLUNAS_EVENTO = [
    "id_evento",
    "versao_evento",
    "tipo_evento",
    "origem",
    "id_venda",
    "id_cliente",
    "id_loja",
    "nome_loja",
    "cidade_loja",
    "estado_loja",
    "data_hora",
    "id_produto",
    "produto",
    "categoria",
    "quantidade",
    "valor_unitario",
    "valor_total",
    "id_forma_pagamento",
    "forma_pagamento",
    "id_canal",
    "canal",
]

COLUNAS_METADATA = [
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "ingested_at",
]



# Controle SQLite


def conectar_controle() -> sqlite3.Connection:
    """
    Cria conexão com o SQLite utilizado para
    controle incremental da camada Silver.
    """

    CONTROLE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    return sqlite3.connect(
        SQLITE_PATH
    )


def criar_tabela_controle(
    conexao: sqlite3.Connection
) -> None:
    """
    Cria a tabela de checkpoint da Silver.

    O checkpoint é controlado por:
        topic + partition_kafka
    """

    conexao.execute(
        """
        CREATE TABLE IF NOT EXISTS controle_offset (
            topic TEXT NOT NULL,
            partition_kafka INTEGER NOT NULL,
            ultimo_offset INTEGER NOT NULL,
            atualizado_em TEXT NOT NULL,

            PRIMARY KEY (
                topic,
                partition_kafka
            )
        );
        """
    )

    conexao.commit()


def obter_ultimo_offset(
    conexao: sqlite3.Connection,
    topic: str,
    partition: int
) -> int:
    """
    Retorna o último offset já processado para
    determinado tópico e partição.

    Quando ainda não existe checkpoint,
    retorna -1.
    """

    cursor = conexao.execute(
        """
        SELECT ultimo_offset
        FROM controle_offset
        WHERE topic = ?
          AND partition_kafka = ?;
        """,
        (
            topic,
            partition
        )
    )

    resultado = cursor.fetchone()

    if resultado is None:
        return -1

    return int(
        resultado[0]
    )


def atualizar_offset(
    conexao: sqlite3.Connection,
    topic: str,
    partition: int,
    offset: int
) -> None:
    """
    Atualiza o maior offset processado pela Silver.
    """

    atualizado_em = datetime.now(
        timezone.utc
    ).isoformat()

    conexao.execute(
        """
        INSERT INTO controle_offset (
            topic,
            partition_kafka,
            ultimo_offset,
            atualizado_em
        )
        VALUES (?, ?, ?, ?)

        ON CONFLICT (
            topic,
            partition_kafka
        )

        DO UPDATE SET
            ultimo_offset = excluded.ultimo_offset,
            atualizado_em = excluded.atualizado_em;
        """,
        (
            topic,
            partition,
            offset,
            atualizado_em
        )
    )

    conexao.commit()



# Descoberta da Bronze


def listar_arquivos_bronze() -> list[Path]:
    """
    Localiza todos os arquivos JSONL existentes
    na camada Bronze.
    """

    if not BRONZE_DIR.exists():
        return []

    return sorted(
        BRONZE_DIR.rglob(
            "*.jsonl"
        )
    )



# Leitura incremental da Bronze


def ler_registros_incrementais(
    arquivos: list[Path],
    conexao: sqlite3.Connection
) -> list[dict]:
    """
    Lê os registros da Bronze cujo offset Kafka seja
    maior que o checkpoint atual da Silver.
    """

    registros = []

    for caminho in arquivos:
        print(
            f"Lendo: {caminho}"
        )

        with caminho.open(
            mode="r",
            encoding="utf-8"
        ) as arquivo:

            for numero_linha, linha in enumerate(
                arquivo,
                start=1
            ):
                linha = linha.strip()

                if not linha:
                    continue

                try:
                    registro = json.loads(
                        linha
                    )

                except json.JSONDecodeError as erro:
                    print(
                        f"JSON inválido: "
                        f"{caminho.name} "
                        f"linha={numero_linha}: "
                        f"{erro}"
                    )

                    continue

                metadata = registro.get(
                    "metadata",
                    {}
                )

                topic = metadata.get(
                    "topic"
                )

                partition = metadata.get(
                    "partition"
                )

                offset = metadata.get(
                    "offset"
                )

                if (
                    topic is None
                    or partition is None
                    or offset is None
                ):
                    print(
                        f"Registro sem metadata Kafka: "
                        f"{caminho.name}, "
                        f"linha={numero_linha}"
                    )

                    continue

                ultimo_offset = obter_ultimo_offset(
                    conexao=conexao,
                    topic=str(topic),
                    partition=int(partition)
                )

                if int(offset) <= ultimo_offset:
                    continue

                registros.append(
                    registro
                )

    return registros



# Flatten


def achatar_registros(
    registros: list[dict]
) -> pd.DataFrame:
    """
    Converte o formato:

        metadata
        evento

    em um DataFrame tabular.
    """

    linhas = []

    for registro in registros:
        metadata = registro.get(
            "metadata",
            {}
        )

        evento = registro.get(
            "evento",
            {}
        )

        linha = {}

        for coluna in COLUNAS_EVENTO:
            linha[coluna] = evento.get(
                coluna
            )

        for coluna in COLUNAS_METADATA:
            linha[coluna] = metadata.get(
                coluna
            )

        linhas.append(
            linha
        )

    return pd.DataFrame(
        linhas
    )



# Tratamento da Silver


def tratar_dataframe(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Realiza:
    - tipagem
    - validação
    - limpeza
    - deduplicação física
    - deduplicação lógica
    - criação das partições Silver
    """

    if df.empty:
        return df

    df = df.copy()


    # Datas


    df["data_hora"] = pd.to_datetime(
        df["data_hora"],
        errors="coerce",
        utc=True
    )

    df["kafka_timestamp"] = pd.to_datetime(
        df["kafka_timestamp"],
        errors="coerce",
        utc=True
    )

    df["ingested_at"] = pd.to_datetime(
        df["ingested_at"],
        errors="coerce",
        utc=True
    )


    # Inteiros


    colunas_inteiras = [
        "versao_evento",
        "id_cliente",
        "id_loja",
        "id_produto",
        "quantidade",
        "id_forma_pagamento",
        "id_canal",
        "partition",
        "offset",
    ]

    for coluna in colunas_inteiras:
        df[coluna] = pd.to_numeric(
            df[coluna],
            errors="coerce"
        ).astype(
            "Int64"
        )


    # Valores numéricos


    colunas_valores = [
        "valor_unitario",
        "valor_total"
    ]

    for coluna in colunas_valores:
        df[coluna] = pd.to_numeric(
            df[coluna],
            errors="coerce"
        )


    # Strings


    colunas_string = [
        "id_evento",
        "tipo_evento",
        "origem",
        "id_venda",
        "nome_loja",
        "cidade_loja",
        "estado_loja",
        "produto",
        "categoria",
        "forma_pagamento",
        "canal",
        "topic",
    ]

    for coluna in colunas_string:
        df[coluna] = (
            df[coluna]
            .astype("string")
            .str.strip()
        )


    # Campos obrigatórios


    df = df[
        df["id_evento"].notna()
        & df["id_venda"].notna()
        & df["data_hora"].notna()
        & df["id_cliente"].notna()
        & df["id_loja"].notna()
        & df["id_produto"].notna()
        & df["quantidade"].notna()
        & df["valor_unitario"].notna()
        & df["valor_total"].notna()
        & df["topic"].notna()
        & df["partition"].notna()
        & df["offset"].notna()
    ].copy()


    # Regras numéricas


    df = df[
        (df["quantidade"] > 0)
        & (df["valor_unitario"] > 0)
        & (df["valor_total"] > 0)
    ].copy()

    # Ordenação física Kafka


    df = df.sort_values(
        by=[
            "topic",
            "partition",
            "offset"
        ]
    )


    # Deduplicação física Kafka


    df = df.drop_duplicates(
        subset=[
            "topic",
            "partition",
            "offset"
        ],
        keep="first"
    )


    # Deduplicação lógica


    df = df.drop_duplicates(
        subset=[
            "id_evento"
        ],
        keep="first"
    )


    # Partições Silver


    df["ano"] = (
        df["data_hora"]
        .dt.year
        .astype("Int64")
    )

    df["mes"] = (
        df["data_hora"]
        .dt.month
        .astype("Int64")
    )

    return df



# Offsets físicos processados


def obter_offsets_processados(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Obtém os offsets físicos Kafka recebidos.

    Não realiza deduplicação por id_evento porque
    o checkpoint representa leitura física da Bronze,
    e não unicidade lógica da Silver.
    """

    if df.empty:
        return df

    df_offsets = df[
        [
            "topic",
            "partition",
            "offset"
        ]
    ].copy()

    df_offsets["topic"] = (
        df_offsets["topic"]
        .astype("string")
        .str.strip()
    )

    df_offsets["partition"] = pd.to_numeric(
        df_offsets["partition"],
        errors="coerce"
    ).astype(
        "Int64"
    )

    df_offsets["offset"] = pd.to_numeric(
        df_offsets["offset"],
        errors="coerce"
    ).astype(
        "Int64"
    )

    df_offsets = df_offsets[
        df_offsets["topic"].notna()
        & df_offsets["partition"].notna()
        & df_offsets["offset"].notna()
    ].copy()

    return df_offsets


# Silver existente


def ler_ids_silver_existentes() -> set[str]:
    """
    Lê apenas id_evento da Silver existente para impedir
    duplicidade lógica em execuções incrementais futuras.
    """

    if not SILVER_DIR.exists():
        return set()

    arquivos = list(
        SILVER_DIR.rglob(
            "*.parquet"
        )
    )

    if not arquivos:
        return set()

    ids = set()

    for arquivo in arquivos:
        df = pd.read_parquet(
            arquivo,
            columns=[
                "id_evento"
            ]
        )

        ids.update(
            df["id_evento"]
            .dropna()
            .astype(str)
            .tolist()
        )

    return ids


def remover_eventos_ja_existentes(
    df: pd.DataFrame,
    ids_existentes: set[str]
) -> pd.DataFrame:
    """
    Remove eventos já persistidos anteriormente
    na Silver.
    """

    if df.empty:
        return df

    if not ids_existentes:
        return df

    return df[
        ~df["id_evento"]
        .astype(str)
        .isin(
            ids_existentes
        )
    ].copy()



# Escrita da Silver


def salvar_silver(
    df: pd.DataFrame
) -> None:
    """
    Grava novos registros Silver em Parquet,
    particionados por ano e mês.
    """

    if df.empty:
        print(
            "Nenhum registro novo para gravar."
        )

        return

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_parquet(
        SILVER_DIR,
        engine="pyarrow",
        index=False,
        partition_cols=[
            "ano",
            "mes"
        ]
    )



# Atualização dos checkpoints


def atualizar_checkpoints(
    conexao: sqlite3.Connection,
    df: pd.DataFrame
) -> None:
    """
    Atualiza o checkpoint com o maior offset físico
    processado para cada topic + partition.
    """

    if df.empty:
        return

    offsets = (
        df.groupby(
            [
                "topic",
                "partition"
            ],
            dropna=False
        )["offset"]
        .max()
        .reset_index()
    )

    for _, registro in offsets.iterrows():
        atualizar_offset(
            conexao=conexao,
            topic=str(
                registro["topic"]
            ),
            partition=int(
                registro["partition"]
            ),
            offset=int(
                registro["offset"]
            )
        )



# Pipeline principal


def executar_pipeline() -> None:
    """
    Executa o pipeline incremental Bronze → Silver.
    """

    print("=" * 80)
    print(
        "Pipeline Bronze → Silver iniciado"
    )
    print(
        f"Bronze: {BRONZE_DIR}"
    )
    print(
        f"Silver: {SILVER_DIR}"
    )
    print(
        f"Controle: {SQLITE_PATH}"
    )
    print("=" * 80)

    conexao = conectar_controle()

    criar_tabela_controle(
        conexao
    )

    try:
        arquivos = listar_arquivos_bronze()

        print(
            f"Arquivos Bronze encontrados: "
            f"{len(arquivos)}"
        )

        if not arquivos:
            print(
                "Nenhum arquivo Bronze encontrado."
            )

            return

        registros = ler_registros_incrementais(
            arquivos=arquivos,
            conexao=conexao
        )

        print(
            f"Registros incrementais encontrados: "
            f"{len(registros)}"
        )

        if not registros:
            print(
                "Nenhum offset novo para processar."
            )

            return

        df = achatar_registros(
            registros
        )

        print(
            f"Registros antes do tratamento: "
            f"{len(df)}"
        )


        # Checkpoint físico


        df_offsets = obter_offsets_processados(
            df
        )


        # Transformação Silver


        df = tratar_dataframe(
            df
        )

        print(
            f"Registros após tratamento: "
            f"{len(df)}"
        )


        # Deduplicação contra histórico Silver


        ids_existentes = (
            ler_ids_silver_existentes()
        )

        df = remover_eventos_ja_existentes(
            df=df,
            ids_existentes=ids_existentes
        )

        print(
            f"Registros novos para Silver: "
            f"{len(df)}"
        )


        # Persistência Silver


        salvar_silver(
            df
        )

        # Atualização do checkpoint

        
        # O checkpoint utiliza os offsets físicos recebidos
        # da Bronze, mesmo quando o evento lógico foi removido
        # por deduplicação.


        atualizar_checkpoints(
            conexao=conexao,
            df=df_offsets
        )

        print("=" * 80)
        print(
            "Pipeline Bronze → Silver concluído."
        )
        print("=" * 80)

    except Exception:
        conexao.rollback()
        raise

    finally:
        conexao.close()



# point


if __name__ == "__main__":
    executar_pipeline()