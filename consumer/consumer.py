import json
from contextlib import closing
from typing import Any

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from psycopg2.extensions import connection as Connection
from psycopg2.extras import Json

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_VENDAS,
    KAFKA_GROUP_OLTP,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)



# Aliases locais


KAFKA_GROUP_ID = KAFKA_GROUP_OLTP



# Validação do evento


CAMPOS_OBRIGATORIOS = {
    "id_evento",
    "id_venda",
    "id_cliente",
    "id_loja",
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
    "canal"
}


def validar_evento(
    evento: dict[str, Any]
) -> None:

    campos_ausentes = (
        CAMPOS_OBRIGATORIOS
        - evento.keys()
    )

    if campos_ausentes:

        raise ValueError(
            f"Campos obrigatórios ausentes: "
            f"{sorted(campos_ausentes)}"
        )

    if int(
        evento["quantidade"]
    ) <= 0:

        raise ValueError(
            "A quantidade precisa ser maior que zero."
        )

    if float(
        evento["valor_unitario"]
    ) <= 0:

        raise ValueError(
            "O valor unitário precisa ser maior que zero."
        )

    if float(
        evento["valor_total"]
    ) <= 0:

        raise ValueError(
            "O valor total precisa ser maior que zero."
        )



# PostgreSQL


def conectar_postgres() -> Connection:

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def evento_ja_processado(
    conexao: Connection,
    id_evento: str
) -> bool:

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM controle.evento_processado
                WHERE id_evento = %s
            );
            """,
            (
                id_evento,
            )
        )

        resultado = (
            cursor.fetchone()
        )

    return bool(
        resultado[0]
    )


def inserir_cliente(
    conexao: Connection,
    evento: dict[str, Any]
) -> None:

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO oltp.cliente (
                id_cliente
            )
            VALUES (%s)

            ON CONFLICT (id_cliente)
            DO NOTHING;
            """,
            (
                int(
                    evento["id_cliente"]
                ),
            )
        )


def inserir_categoria(
    conexao: Connection,
    evento: dict[str, Any]
) -> int:

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO oltp.categoria (
                nome_categoria
            )
            VALUES (%s)

            ON CONFLICT (nome_categoria)

            DO UPDATE SET
                nome_categoria =
                    EXCLUDED.nome_categoria

            RETURNING id_categoria;
            """,
            (
                evento["categoria"],
            )
        )

        resultado = (
            cursor.fetchone()
        )

    return int(
        resultado[0]
    )


def inserir_produto(
    conexao: Connection,
    evento: dict[str, Any],
    id_categoria: int
) -> None:

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO oltp.produto (
                id_produto,
                nome_produto,
                id_categoria,
                preco_atual
            )
            VALUES (
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT (id_produto)

            DO UPDATE SET
                nome_produto =
                    EXCLUDED.nome_produto,
                id_categoria =
                    EXCLUDED.id_categoria,
                preco_atual =
                    EXCLUDED.preco_atual;
            """,
            (
                int(
                    evento["id_produto"]
                ),
                evento["produto"],
                id_categoria,
                float(
                    evento["valor_unitario"]
                )
            )
        )


def inserir_forma_pagamento(
    conexao: Connection,
    evento: dict[str, Any]
) -> None:

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO oltp.forma_pagamento (
                id_forma_pagamento,
                nome_forma_pagamento
            )
            VALUES (
                %s,
                %s
            )

            ON CONFLICT (
                id_forma_pagamento
            )

            DO UPDATE SET
                nome_forma_pagamento =
                    EXCLUDED.nome_forma_pagamento;
            """,
            (
                int(
                    evento[
                        "id_forma_pagamento"
                    ]
                ),
                evento[
                    "forma_pagamento"
                ]
            )
        )


def inserir_canal(
    conexao: Connection,
    evento: dict[str, Any]
) -> None:

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO oltp.canal (
                id_canal,
                nome_canal
            )
            VALUES (
                %s,
                %s
            )

            ON CONFLICT (
                id_canal
            )

            DO UPDATE SET
                nome_canal =
                    EXCLUDED.nome_canal;
            """,
            (
                int(
                    evento["id_canal"]
                ),
                evento["canal"]
            )
        )


def inserir_venda(
    conexao: Connection,
    evento: dict[str, Any]
) -> None:

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO oltp.venda (
                id_venda,
                id_cliente,
                id_loja,
                id_forma_pagamento,
                id_canal,
                data_hora,
                valor_total
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT (
                id_venda
            )

            DO NOTHING;
            """,
            (
                evento["id_venda"],
                int(
                    evento["id_cliente"]
                ),
                int(
                    evento["id_loja"]
                ),
                int(
                    evento[
                        "id_forma_pagamento"
                    ]
                ),
                int(
                    evento["id_canal"]
                ),
                evento["data_hora"],
                float(
                    evento["valor_total"]
                )
            )
        )


def inserir_item_venda(
    conexao: Connection,
    evento: dict[str, Any]
) -> None:

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO oltp.item_venda (
                id_venda,
                id_produto,
                quantidade,
                valor_unitario,
                valor_total
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT (
                id_venda,
                id_produto
            )

            DO NOTHING;
            """,
            (
                evento["id_venda"],
                int(
                    evento["id_produto"]
                ),
                int(
                    evento["quantidade"]
                ),
                float(
                    evento["valor_unitario"]
                ),
                float(
                    evento["valor_total"]
                )
            )
        )


def registrar_evento_processado(
    conexao: Connection,
    evento: dict[str, Any],
    topico: str,
    particao: int,
    offset: int
) -> None:

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO controle.evento_processado (
                id_evento,
                id_venda,
                topico,
                particao,
                offset_kafka
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT
            DO NOTHING;
            """,
            (
                evento["id_evento"],
                evento["id_venda"],
                topico,
                particao,
                offset
            )
        )


def registrar_erro(
    evento: dict[str, Any],
    topico: str,
    particao: int,
    offset: int,
    erro: Exception
) -> None:

    try:

        with closing(
            conectar_postgres()
        ) as conexao:

            with conexao.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO controle.erro_processamento (
                        topico,
                        particao,
                        offset_kafka,
                        mensagem,
                        tipo_erro,
                        descricao_erro
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    );
                    """,
                    (
                        topico,
                        particao,
                        offset,
                        Json(evento),
                        type(erro).__name__,
                        str(erro)
                    )
                )

            conexao.commit()

    except Exception as erro_log:

        print(
            f"Não foi possível registrar "
            f"o erro no banco: "
            f"{erro_log}"
        )



# Processamento do evento


def processar_evento(
    conexao: Connection,
    evento: dict[str, Any],
    topico: str,
    particao: int,
    offset: int
) -> str:

    validar_evento(
        evento
    )

    if evento_ja_processado(
        conexao,
        evento["id_evento"]
    ):

        return "duplicado"

    inserir_cliente(
        conexao,
        evento
    )

    inserir_forma_pagamento(
        conexao,
        evento
    )

    inserir_canal(
        conexao,
        evento
    )

    id_categoria = inserir_categoria(
        conexao,
        evento
    )

    inserir_produto(
        conexao,
        evento,
        id_categoria
    )

    inserir_venda(
        conexao,
        evento
    )

    inserir_item_venda(
        conexao,
        evento
    )

    registrar_evento_processado(
        conexao,
        evento,
        topico,
        particao,
        offset
    )

    return "processado"



# Kafka Consumer


def criar_consumer() -> KafkaConsumer:

    return KafkaConsumer(
        KAFKA_TOPIC_VENDAS,

        bootstrap_servers=(
            KAFKA_BOOTSTRAP_SERVERS
        ),

        group_id=(
            KAFKA_GROUP_ID
        ),

        auto_offset_reset="earliest",

        enable_auto_commit=False,

        value_deserializer=lambda valor: (
            json.loads(
                valor.decode(
                    "utf-8"
                )
            )
        )
    )



# Execução


def executar_consumer() -> None:

    consumer = criar_consumer()

    print("=" * 80)

    print(
        "Consumer PostgreSQL iniciado"
    )

    print(
        f"Kafka: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Tópico: "
        f"{KAFKA_TOPIC_VENDAS}"
    )

    print(
        f"Grupo: "
        f"{KAFKA_GROUP_ID}"
    )

    print(
        f"PostgreSQL: "
        f"{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/"
        f"{POSTGRES_DB}"
    )

    print("=" * 80)

    try:

        with closing(
            conectar_postgres()
        ) as conexao:

            conexao.autocommit = False

            for mensagem in consumer:

                evento = mensagem.value

                try:

                    resultado = processar_evento(
                        conexao=conexao,
                        evento=evento,
                        topico=mensagem.topic,
                        particao=mensagem.partition,
                        offset=mensagem.offset
                    )

                    conexao.commit()

                    # Confirma o offset somente
                    # depois do commit no PostgreSQL.
                    consumer.commit()

                    print("-" * 80)

                    if resultado == "duplicado":

                        print(
                            "Evento já processado "
                            "anteriormente."
                        )

                    else:

                        print(
                            "Evento persistido "
                            "com sucesso."
                        )

                    print(
                        f"Evento: "
                        f"{evento['id_evento']}"
                    )

                    print(
                        f"Venda: "
                        f"{evento['id_venda']}"
                    )

                    print(
                        f"Cliente: "
                        f"{evento['id_cliente']}"
                    )

                    print(
                        f"Produto: "
                        f"{evento['produto']}"
                    )

                    print(
                        f"Valor: R$ "
                        f"{float(evento['valor_total']):.2f}"
                    )

                    print(
                        f"Offset confirmado: "
                        f"{mensagem.offset}"
                    )

                except Exception as erro:

                    conexao.rollback()

                    print("-" * 80)

                    print(
                        f"Erro ao processar "
                        f"offset "
                        f"{mensagem.offset}: "
                        f"{erro}"
                    )

                    registrar_erro(
                        evento=evento,
                        topico=mensagem.topic,
                        particao=mensagem.partition,
                        offset=mensagem.offset,
                        erro=erro
                    )

                    # Durante os testes, confirmamos
                    # o offset do erro para o consumer
                    # não ficar preso na mesma mensagem.
                    consumer.commit()

    except KeyboardInterrupt:

        print(
            "\nEncerramento solicitado "
            "pelo usuário."
        )

    except (
        KafkaError,
        psycopg2.Error
    ) as erro:

        print(
            f"Erro de infraestrutura: "
            f"{erro}"
        )

        raise

    finally:

        consumer.close()

        print(
            "Consumer encerrado."
        )



# point

if __name__ == "__main__":
    executar_consumer()