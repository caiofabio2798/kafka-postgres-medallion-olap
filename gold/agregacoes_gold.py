from pathlib import Path

import pandas as pd
import shutil
import time

from config.settings import (
    GOLD_FATO_VENDAS_DIR,
    GOLD_DIR,
)



# Aliases locais


FATO_VENDAS_DIR = GOLD_FATO_VENDAS_DIR



# Leitura da fato_vendas


def listar_arquivos_fato() -> list[Path]:

    if not FATO_VENDAS_DIR.exists():
        return []

    return sorted(
        FATO_VENDAS_DIR.rglob(
            "*.parquet"
        )
    )


def ler_fato_vendas() -> pd.DataFrame:

    arquivos = listar_arquivos_fato()

    if not arquivos:
        return pd.DataFrame()

    dataframes = []

    for arquivo in arquivos:

        print(
            f"Lendo fato: {arquivo}"
        )

        dataframes.append(
            pd.read_parquet(
                arquivo
            )
        )

    return pd.concat(
        dataframes,
        ignore_index=True
    )



# Preparação comum


def preparar_fato(
    df: pd.DataFrame
) -> pd.DataFrame:

    if df.empty:
        return df

    df = df.copy()

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

    return df



# Agregação 1 - Vendas diárias


def gerar_vendas_diarias(
    df: pd.DataFrame
) -> pd.DataFrame:

    agregado = (
        df.groupby(
            [
                "data",
                "ano",
                "mes",
            ],
            as_index=False
        )
        .agg(
            receita_total=(
                "valor_total",
                "sum"
            ),
            quantidade_itens=(
                "quantidade",
                "sum"
            ),
            quantidade_vendas=(
                "id_venda",
                "nunique"
            ),
            clientes_unicos=(
                "id_cliente",
                "nunique"
            )
        )
    )

    agregado["ticket_medio"] = (
        agregado["receita_total"]
        / agregado["quantidade_vendas"]
    )

    return agregado



# Agregação 2 - Vendas por loja


def gerar_vendas_por_loja(
    df: pd.DataFrame
) -> pd.DataFrame:

    agregado = (
        df.groupby(
            [
                "data",
                "ano",
                "mes",
                "id_loja",
                "nome_loja",
                "cidade_loja",
                "estado_loja",
            ],
            as_index=False
        )
        .agg(
            receita_total=(
                "valor_total",
                "sum"
            ),
            quantidade_itens=(
                "quantidade",
                "sum"
            ),
            quantidade_vendas=(
                "id_venda",
                "nunique"
            ),
            clientes_unicos=(
                "id_cliente",
                "nunique"
            )
        )
    )

    agregado["ticket_medio"] = (
        agregado["receita_total"]
        / agregado["quantidade_vendas"]
    )

    return agregado



# Agregação 3 - Vendas por produto


def gerar_vendas_por_produto(
    df: pd.DataFrame
) -> pd.DataFrame:

    agregado = (
        df.groupby(
            [
                "data",
                "ano",
                "mes",
                "id_produto",
                "produto",
                "categoria",
            ],
            as_index=False
        )
        .agg(
            receita_total=(
                "valor_total",
                "sum"
            ),
            quantidade_itens=(
                "quantidade",
                "sum"
            ),
            quantidade_vendas=(
                "id_venda",
                "nunique"
            ),
            clientes_unicos=(
                "id_cliente",
                "nunique"
            )
        )
    )

    agregado["preco_medio"] = (
        agregado["receita_total"]
        / agregado["quantidade_itens"]
    )

    return agregado



# Agregação 4 - Vendas por categoria


def gerar_vendas_por_categoria(
    df: pd.DataFrame
) -> pd.DataFrame:

    agregado = (
        df.groupby(
            [
                "data",
                "ano",
                "mes",
                "categoria",
            ],
            as_index=False
        )
        .agg(
            receita_total=(
                "valor_total",
                "sum"
            ),
            quantidade_itens=(
                "quantidade",
                "sum"
            ),
            quantidade_vendas=(
                "id_venda",
                "nunique"
            ),
            clientes_unicos=(
                "id_cliente",
                "nunique"
            ),
            produtos_distintos=(
                "id_produto",
                "nunique"
            )
        )
    )

    agregado["ticket_medio"] = (
        agregado["receita_total"]
        / agregado["quantidade_vendas"]
    )

    return agregado



# Limpeza de diretórios


def limpar_diretorio(
    diretorio: Path
) -> None:
    """
    Remove um diretório existente com retry para contornar
    bloqueios temporários do Windows.
    """

    if not diretorio.exists():
        return

    tentativas = 5

    for tentativa in range(
        1,
        tentativas + 1
    ):

        try:

            shutil.rmtree(
                diretorio
            )

            print(
                f"Diretório anterior removido: "
                f"{diretorio.name}"
            )

            return

        except PermissionError:

            if tentativa == tentativas:
                raise

            print(
                f"Diretório {diretorio.name} bloqueado. "
                f"Tentativa {tentativa}/{tentativas}..."
            )

            time.sleep(
                1
            )


# Escrita


def salvar_agregacao(
    df: pd.DataFrame,
    nome: str
) -> None:

    if df.empty:

        print(
            f"Nenhum dado para {nome}."
        )

        return

    destino = (
        GOLD_DIR
        / nome
    )

    limpar_diretorio(
        destino
    )

    destino.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_parquet(
        destino,
        engine="pyarrow",
        index=False,
        partition_cols=[
            "ano",
            "mes"
        ]
    )

    print(
        f"{nome}: "
        f"{len(df)} registros gravados."
    )


# Pipeline principal


def executar_pipeline() -> None:

    print("=" * 80)
    print(
        "Agregações Gold iniciadas"
    )
    print(
        f"Origem: {FATO_VENDAS_DIR}"
    )
    print(
        f"Destino Gold: {GOLD_DIR}"
    )
    print("=" * 80)

    df = ler_fato_vendas()

    print(
        f"Registros fato_vendas: "
        f"{len(df)}"
    )

    if df.empty:

        print(
            "Nenhum dado encontrado."
        )

        return

    df = preparar_fato(
        df
    )

    vendas_diarias = gerar_vendas_diarias(
        df
    )

    vendas_por_loja = gerar_vendas_por_loja(
        df
    )

    vendas_por_produto = gerar_vendas_por_produto(
        df
    )

    vendas_por_categoria = gerar_vendas_por_categoria(
        df
    )

    salvar_agregacao(
        vendas_diarias,
        "vendas_diarias"
    )

    salvar_agregacao(
        vendas_por_loja,
        "vendas_por_loja"
    )

    salvar_agregacao(
        vendas_por_produto,
        "vendas_por_produto"
    )

    salvar_agregacao(
        vendas_por_categoria,
        "vendas_por_categoria"
    )

    print("=" * 80)
    print(
        "Agregações Gold concluídas."
    )
    print("=" * 80)


if __name__ == "__main__":
    executar_pipeline()