from pathlib import Path

import pandas as pd
import shutil
import time

from config.settings import (
    SILVER_VENDAS_DIR,
    GOLD_FATO_VENDAS_DIR,
)



# Aliases locais


SILVER_DIR = SILVER_VENDAS_DIR
GOLD_DIR = GOLD_FATO_VENDAS_DIR



# Leitura da Silver


def listar_arquivos_silver() -> list[Path]:
    """
    Lista todos os arquivos Parquet da Silver.
    """

    if not SILVER_DIR.exists():
        return []

    return sorted(
        SILVER_DIR.rglob(
            "*.parquet"
        )
    )


def ler_silver() -> pd.DataFrame:
    """
    Lê os arquivos físicos da Silver.

    Fazemos a leitura arquivo a arquivo para evitar
    o problema de leitura direta da raiz particionada.
    """

    arquivos = listar_arquivos_silver()

    if not arquivos:
        return pd.DataFrame()

    dataframes = []

    for arquivo in arquivos:

        print(
            f"Lendo Silver: {arquivo}"
        )

        df = pd.read_parquet(
            arquivo
        )

        dataframes.append(
            df
        )

    return pd.concat(
        dataframes,
        ignore_index=True
    )



# Transformação Gold


def construir_fato_vendas(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Constrói a fato analítica de vendas
    a partir da Silver.
    """

    if df.empty:
        return df

    df = df.copy()


    # Data


    df["data_hora"] = pd.to_datetime(
        df["data_hora"],
        errors="coerce",
        utc=True
    )

    df["data"] = (
        df["data_hora"]
        .dt.date
    )

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

    df["dia"] = (
        df["data_hora"]
        .dt.day
        .astype("Int64")
    )


    # Ordenação das colunas


    colunas_gold = [
        "id_evento",
        "id_venda",
        "data_hora",
        "data",
        "ano",
        "mes",
        "dia",
        "id_cliente",
        "id_loja",
        "nome_loja",
        "cidade_loja",
        "estado_loja",
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

    colunas_existentes = [
        coluna
        for coluna in colunas_gold
        if coluna in df.columns
    ]

    df = df[
        colunas_existentes
    ].copy()


    # Segurança contra duplicidade

    df = (
        df
        .sort_values(
            by=[
                "data_hora",
                "id_venda",
            ]
        )
        .drop_duplicates(
            subset=[
                "id_evento"
            ],
            keep="first"
        )
    )

    return df



# Limpeza da Gold anterior


def limpar_gold_existente() -> None:
    """
    Remove a Gold anterior com pequenas tentativas
    de retry.

    Isso é útil em Windows quando algum arquivo
    fica temporariamente bloqueado.
    """

    if not GOLD_DIR.exists():
        return

    tentativas = 5

    for tentativa in range(
        1,
        tentativas + 1
    ):

        try:

            shutil.rmtree(
                GOLD_DIR
            )

            print(
                "Gold anterior removida."
            )

            return

        except PermissionError:

            if tentativa == tentativas:
                raise

            print(
                f"Diretório Gold bloqueado. "
                f"Tentativa "
                f"{tentativa}/{tentativas}..."
            )

            time.sleep(
                1
            )



# Escrita Gold


def salvar_gold(
    df: pd.DataFrame
) -> None:
    """
    Salva a Gold em Parquet,
    particionada por ano e mês.
    """

    if df.empty:

        print(
            "Nenhum registro disponível para Gold."
        )

        return

    limpar_gold_existente()

    GOLD_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_parquet(
        GOLD_DIR,
        engine="pyarrow",
        index=False,
        partition_cols=[
            "ano",
            "mes",
        ]
    )



# Pipeline principal


def executar_pipeline() -> None:

    print("=" * 80)

    print(
        "Pipeline Silver → Gold iniciado"
    )

    print(
        f"Silver: {SILVER_DIR}"
    )

    print(
        f"Gold: {GOLD_DIR}"
    )

    print("=" * 80)

    df_silver = ler_silver()

    print(
        f"Registros Silver encontrados: "
        f"{len(df_silver)}"
    )

    if df_silver.empty:

        print(
            "Nenhum dado disponível na Silver."
        )

        return

    df_gold = construir_fato_vendas(
        df_silver
    )

    print(
        f"Registros Gold gerados: "
        f"{len(df_gold)}"
    )

    salvar_gold(
        df_gold
    )

    print("=" * 80)

    print(
        "Pipeline Silver → Gold concluído."
    )

    print("=" * 80)



# Entry point


if __name__ == "__main__":
    executar_pipeline()