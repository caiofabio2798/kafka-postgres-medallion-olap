from pathlib import Path

import pandas as pd
import psycopg2

from config.settings import (
    GOLD_FATO_VENDAS_DIR,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)



# Alias local


GOLD_FATO_DIR = GOLD_FATO_VENDAS_DIR



# Conexão


def conectar_postgres():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )



# Leitura da Gold


def listar_arquivos_gold() -> list[Path]:

    if not GOLD_FATO_DIR.exists():
        return []

    return sorted(
        GOLD_FATO_DIR.rglob(
            "*.parquet"
        )
    )


def ler_gold() -> pd.DataFrame:

    arquivos = listar_arquivos_gold()

    if not arquivos:
        return pd.DataFrame()

    dataframes = []

    for arquivo in arquivos:

        print(
            f"Lendo Gold: {arquivo}"
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



# Preparação


def preparar_dataframe(
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

    return df



# DIM DATA


def carregar_dim_data(
    conexao,
    df: pd.DataFrame
) -> None:

    datas = (
        df["data"]
        .dropna()
        .drop_duplicates()
        .sort_values()
    )

    with conexao.cursor() as cursor:

        for data in datas:

            timestamp = pd.Timestamp(
                data
            )

            sk_data = int(
                timestamp.strftime(
                    "%Y%m%d"
                )
            )

            dia = timestamp.day
            mes = timestamp.month
            ano = timestamp.year

            trimestre = (
                (mes - 1) // 3
            ) + 1

            semestre = (
                1
                if mes <= 6
                else 2
            )

            dia_semana = (
                timestamp.dayofweek + 1
            )

            nomes_dia = {
                1: "Segunda-feira",
                2: "Terça-feira",
                3: "Quarta-feira",
                4: "Quinta-feira",
                5: "Sexta-feira",
                6: "Sábado",
                7: "Domingo"
            }

            nomes_mes = {
                1: "Janeiro",
                2: "Fevereiro",
                3: "Março",
                4: "Abril",
                5: "Maio",
                6: "Junho",
                7: "Julho",
                8: "Agosto",
                9: "Setembro",
                10: "Outubro",
                11: "Novembro",
                12: "Dezembro"
            }

            fim_de_semana = (
                dia_semana
                in [6, 7]
            )

            cursor.execute(
                """
                INSERT INTO olap.dim_data (
                    sk_data,
                    data,
                    dia,
                    mes,
                    ano,
                    nome_mes,
                    trimestre,
                    semestre,
                    dia_semana,
                    nome_dia_semana,
                    fim_de_semana
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (sk_data)
                DO NOTHING;
                """,
                (
                    sk_data,
                    data,
                    dia,
                    mes,
                    ano,
                    nomes_mes[mes],
                    trimestre,
                    semestre,
                    dia_semana,
                    nomes_dia[dia_semana],
                    fim_de_semana
                )
            )



# DIM CLIENTE


def carregar_dim_cliente(
    conexao,
    df: pd.DataFrame
) -> None:

    clientes = (
        df[
            [
                "id_cliente"
            ]
        ]
        .drop_duplicates()
    )

    with conexao.cursor() as cursor:

        for _, linha in clientes.iterrows():

            cursor.execute(
                """
                INSERT INTO olap.dim_cliente (
                    id_cliente
                )
                VALUES (%s)

                ON CONFLICT (id_cliente)

                DO UPDATE SET
                    ativo = TRUE;
                """,
                (
                    int(
                        linha["id_cliente"]
                    ),
                )
            )



# DIM LOJA


def carregar_dim_loja(
    conexao,
    df: pd.DataFrame
) -> None:

    lojas = (
        df[
            [
                "id_loja",
                "nome_loja",
                "cidade_loja",
                "estado_loja"
            ]
        ]
        .drop_duplicates(
            subset=[
                "id_loja"
            ]
        )
    )

    with conexao.cursor() as cursor:

        for _, linha in lojas.iterrows():

            cursor.execute(
                """
                INSERT INTO olap.dim_loja (
                    id_loja,
                    nome_loja,
                    cidade,
                    estado
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )

                ON CONFLICT (id_loja)

                DO UPDATE SET
                    nome_loja =
                        EXCLUDED.nome_loja,
                    cidade =
                        EXCLUDED.cidade,
                    estado =
                        EXCLUDED.estado,
                    ativo = TRUE;
                """,
                (
                    int(
                        linha["id_loja"]
                    ),
                    linha["nome_loja"],
                    linha["cidade_loja"],
                    linha["estado_loja"]
                )
            )



# DIM CATEGORIA


def carregar_dim_categoria(
    conexao,
    df: pd.DataFrame
) -> None:

    categorias = (
        df["categoria"]
        .dropna()
        .drop_duplicates()
        .sort_values()
    )

    with conexao.cursor() as cursor:

        for categoria in categorias:

            cursor.execute(
                """
                INSERT INTO olap.dim_categoria (
                    categoria
                )
                VALUES (%s)

                ON CONFLICT (categoria)

                DO UPDATE SET
                    ativo = TRUE;
                """,
                (
                    categoria,
                )
            )



# DIM PRODUTO


def carregar_dim_produto(
    conexao,
    df: pd.DataFrame
) -> None:

    produtos = (
        df[
            [
                "id_produto",
                "produto",
                "categoria"
            ]
        ]
        .drop_duplicates(
            subset=[
                "id_produto"
            ]
        )
    )

    with conexao.cursor() as cursor:

        for _, linha in produtos.iterrows():

            cursor.execute(
                """
                SELECT sk_categoria
                FROM olap.dim_categoria
                WHERE categoria = %s;
                """,
                (
                    linha["categoria"],
                )
            )

            sk_categoria = (
                cursor.fetchone()[0]
            )

            cursor.execute(
                """
                INSERT INTO olap.dim_produto (
                    id_produto,
                    produto,
                    sk_categoria
                )
                VALUES (
                    %s,
                    %s,
                    %s
                )

                ON CONFLICT (id_produto)

                DO UPDATE SET
                    produto =
                        EXCLUDED.produto,
                    sk_categoria =
                        EXCLUDED.sk_categoria,
                    ativo = TRUE;
                """,
                (
                    int(
                        linha["id_produto"]
                    ),
                    linha["produto"],
                    sk_categoria
                )
            )



# DIM CANAL


def carregar_dim_canal(
    conexao,
    df: pd.DataFrame
) -> None:

    canais = (
        df[
            [
                "id_canal",
                "canal"
            ]
        ]
        .drop_duplicates(
            subset=[
                "id_canal"
            ]
        )
    )

    with conexao.cursor() as cursor:

        for _, linha in canais.iterrows():

            cursor.execute(
                """
                INSERT INTO olap.dim_canal (
                    id_canal,
                    canal
                )
                VALUES (
                    %s,
                    %s
                )

                ON CONFLICT (id_canal)

                DO UPDATE SET
                    canal =
                        EXCLUDED.canal,
                    ativo = TRUE;
                """,
                (
                    int(
                        linha["id_canal"]
                    ),
                    linha["canal"]
                )
            )



# DIM FORMA PAGAMENTO


def carregar_dim_forma_pagamento(
    conexao,
    df: pd.DataFrame
) -> None:

    formas = (
        df[
            [
                "id_forma_pagamento",
                "forma_pagamento"
            ]
        ]
        .drop_duplicates(
            subset=[
                "id_forma_pagamento"
            ]
        )
    )

    with conexao.cursor() as cursor:

        for _, linha in formas.iterrows():

            cursor.execute(
                """
                INSERT INTO olap.dim_forma_pagamento (
                    id_forma_pagamento,
                    forma_pagamento
                )
                VALUES (
                    %s,
                    %s
                )

                ON CONFLICT (
                    id_forma_pagamento
                )

                DO UPDATE SET
                    forma_pagamento =
                        EXCLUDED.forma_pagamento,
                    ativo = TRUE;
                """,
                (
                    int(
                        linha[
                            "id_forma_pagamento"
                        ]
                    ),
                    linha[
                        "forma_pagamento"
                    ]
                )
            )



# Mapas de surrogate keys


def obter_mapa(
    conexao,
    tabela: str,
    coluna_natural: str,
    coluna_sk: str
) -> dict:

    with conexao.cursor() as cursor:

        cursor.execute(
            f"""
            SELECT
                {coluna_natural},
                {coluna_sk}
            FROM {tabela};
            """
        )

        return {
            linha[0]: linha[1]
            for linha in cursor.fetchall()
        }



# FATO


def carregar_fato(
    conexao,
    df: pd.DataFrame
) -> None:

    mapa_cliente = obter_mapa(
        conexao,
        "olap.dim_cliente",
        "id_cliente",
        "sk_cliente"
    )

    mapa_loja = obter_mapa(
        conexao,
        "olap.dim_loja",
        "id_loja",
        "sk_loja"
    )

    mapa_produto = obter_mapa(
        conexao,
        "olap.dim_produto",
        "id_produto",
        "sk_produto"
    )

    mapa_canal = obter_mapa(
        conexao,
        "olap.dim_canal",
        "id_canal",
        "sk_canal"
    )

    mapa_forma = obter_mapa(
        conexao,
        "olap.dim_forma_pagamento",
        "id_forma_pagamento",
        "sk_forma_pagamento"
    )

    with conexao.cursor() as cursor:

        for _, linha in df.iterrows():

            data = pd.Timestamp(
                linha["data"]
            )

            sk_data = int(
                data.strftime(
                    "%Y%m%d"
                )
            )

            id_cliente = int(
                linha["id_cliente"]
            )

            id_loja = int(
                linha["id_loja"]
            )

            id_produto = int(
                linha["id_produto"]
            )

            id_canal = int(
                linha["id_canal"]
            )

            id_forma_pagamento = int(
                linha["id_forma_pagamento"]
            )

            cursor.execute(
                """
                INSERT INTO olap.fato_vendas (
                    id_evento,
                    id_venda,

                    sk_data,
                    sk_cliente,
                    sk_loja,
                    sk_produto,
                    sk_canal,
                    sk_forma_pagamento,

                    data_hora,

                    quantidade,
                    valor_unitario,
                    valor_total
                )
                VALUES (
                    %s,
                    %s,

                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,

                    %s,

                    %s,
                    %s,
                    %s
                )

                ON CONFLICT (id_evento)
                DO NOTHING;
                """,
                (
                    linha["id_evento"],
                    linha["id_venda"],

                    sk_data,
                    mapa_cliente[
                        id_cliente
                    ],
                    mapa_loja[
                        id_loja
                    ],
                    mapa_produto[
                        id_produto
                    ],
                    mapa_canal[
                        id_canal
                    ],
                    mapa_forma[
                        id_forma_pagamento
                    ],

                    linha[
                        "data_hora"
                    ].to_pydatetime(),

                    float(
                        linha["quantidade"]
                    ),
                    float(
                        linha["valor_unitario"]
                    ),
                    float(
                        linha["valor_total"]
                    )
                )
            )



# Pipeline principal


def executar_pipeline() -> None:

    print("=" * 80)
    print(
        "Pipeline Gold → OLAP iniciado"
    )
    print(
        f"Origem Gold: {GOLD_FATO_DIR}"
    )
    print(
        f"PostgreSQL: "
        f"{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/"
        f"{POSTGRES_DB}"
    )
    print("=" * 80)

    df = ler_gold()

    print(
        f"Registros Gold encontrados: "
        f"{len(df)}"
    )

    if df.empty:

        print(
            "Nenhum dado Gold disponível."
        )

        return

    df = preparar_dataframe(
        df
    )

    conexao = conectar_postgres()

    conexao.autocommit = False

    try:

        print(
            "Carregando dim_data..."
        )

        carregar_dim_data(
            conexao,
            df
        )

        print(
            "Carregando dim_cliente..."
        )

        carregar_dim_cliente(
            conexao,
            df
        )

        print(
            "Carregando dim_loja..."
        )

        carregar_dim_loja(
            conexao,
            df
        )

        print(
            "Carregando dim_categoria..."
        )

        carregar_dim_categoria(
            conexao,
            df
        )

        print(
            "Carregando dim_produto..."
        )

        carregar_dim_produto(
            conexao,
            df
        )

        print(
            "Carregando dim_canal..."
        )

        carregar_dim_canal(
            conexao,
            df
        )

        print(
            "Carregando dim_forma_pagamento..."
        )

        carregar_dim_forma_pagamento(
            conexao,
            df
        )

        # Garante que todas as dimensões estejam
        # visíveis antes da resolução das SKs.
        conexao.commit()

        print(
            "Carregando fato_vendas..."
        )

        carregar_fato(
            conexao,
            df
        )

        conexao.commit()

        print("=" * 80)
        print(
            "Pipeline Gold → OLAP concluído."
        )
        print("=" * 80)

    except Exception as erro:

        conexao.rollback()

        print(
            f"Erro durante carga OLAP: "
            f"{erro}"
        )

        raise

    finally:

        conexao.close()


if __name__ == "__main__":
    executar_pipeline()