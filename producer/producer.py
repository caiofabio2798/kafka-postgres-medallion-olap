import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_VENDAS,
    PRODUTOS_JSON_PATH,
    FORMAS_PAGAMENTO_JSON_PATH,
    CANAIS_JSON_PATH,
    LOJAS_JSON_PATH,
)



# Aliases locais


ARQUIVO_PRODUTOS = PRODUTOS_JSON_PATH

ARQUIVO_FORMAS_PAGAMENTO = (
    FORMAS_PAGAMENTO_JSON_PATH
)

ARQUIVO_CANAIS = CANAIS_JSON_PATH

ARQUIVO_LOJAS = LOJAS_JSON_PATH



# Leitura dos arquivos JSON


def carregar_lista_json(
    caminho: Path,
    nome_dados: str,
    campos_obrigatorios: set[str]
) -> list[dict[str, Any]]:
    """
    Carrega uma lista de objetos de um arquivo JSON
    e valida os campos obrigatórios.
    """

    if not caminho.exists():

        raise FileNotFoundError(
            f"Arquivo de {nome_dados} "
            f"não encontrado: {caminho}"
        )

    try:

        with caminho.open(
            mode="r",
            encoding="utf-8"
        ) as arquivo:

            dados = json.load(
                arquivo
            )

    except json.JSONDecodeError as erro:

        raise ValueError(
            f"O arquivo de {nome_dados} "
            f"contém um JSON inválido: {erro}"
        ) from erro

    if not isinstance(
        dados,
        list
    ):

        raise ValueError(
            f"O arquivo de {nome_dados} "
            "deve conter uma lista."
        )

    if not dados:

        raise ValueError(
            f"O arquivo de {nome_dados} "
            "está vazio."
        )

    for indice, registro in enumerate(
        dados
    ):

        if not isinstance(
            registro,
            dict
        ):

            raise ValueError(
                f"O registro {indice} de "
                f"{nome_dados} deve ser "
                "um objeto JSON."
            )

        campos_ausentes = (
            campos_obrigatorios
            - registro.keys()
        )

        if campos_ausentes:

            raise ValueError(
                f"O registro {indice} de "
                f"{nome_dados} não possui "
                f"os campos: "
                f"{sorted(campos_ausentes)}"
            )

    return dados



# Carregamento dos dados auxiliares


PRODUTOS = carregar_lista_json(
    caminho=ARQUIVO_PRODUTOS,
    nome_dados="produtos",
    campos_obrigatorios={
        "id_produto",
        "produto",
        "categoria",
        "preco"
    }
)

FORMAS_PAGAMENTO = carregar_lista_json(
    caminho=ARQUIVO_FORMAS_PAGAMENTO,
    nome_dados="formas de pagamento",
    campos_obrigatorios={
        "id_forma_pagamento",
        "forma_pagamento"
    }
)

CANAIS = carregar_lista_json(
    caminho=ARQUIVO_CANAIS,
    nome_dados="canais",
    campos_obrigatorios={
        "id_canal",
        "canal"
    }
)

LOJAS = carregar_lista_json(
    caminho=ARQUIVO_LOJAS,
    nome_dados="lojas",
    campos_obrigatorios={
        "id_loja",
        "nome_loja",
        "cidade",
        "estado"
    }
)



# Configuração do Kafka Producer


producer = KafkaProducer(

    bootstrap_servers=(
        KAFKA_BOOTSTRAP_SERVERS
    ),

    key_serializer=lambda chave: (
        str(
            chave
        ).encode(
            "utf-8"
        )
    ),

    value_serializer=lambda valor: (
        json.dumps(
            valor,
            ensure_ascii=False
        ).encode(
            "utf-8"
        )
    ),

    acks="all",

    retries=5
)



# Geração da venda


def gerar_venda() -> dict[str, Any]:
    """
    Gera um evento simulado de venda.
    """

    produto = random.choice(
        PRODUTOS
    )

    forma_pagamento = random.choice(
        FORMAS_PAGAMENTO
    )

    canal = random.choice(
        CANAIS
    )

    loja = random.choice(
        LOJAS
    )

    quantidade = random.randint(
        1,
        5
    )

    valor_unitario = round(
        float(
            produto["preco"]
        ),
        2
    )

    valor_total = round(
        valor_unitario
        * quantidade,
        2
    )

    evento = {
        "id_evento": str(
            uuid.uuid4()
        ),

        "versao_evento": 1,

        "tipo_evento": (
            "venda_realizada"
        ),

        "origem": (
            "simulador_vendas"
        ),

        "id_venda": str(
            uuid.uuid4()
        ),

        "id_cliente": random.randint(
            1000,
            9999
        ),

        "id_loja": int(
            loja["id_loja"]
        ),

        "nome_loja": (
            loja["nome_loja"]
        ),

        "cidade_loja": (
            loja["cidade"]
        ),

        "estado_loja": (
            loja["estado"]
        ),

        "data_hora": datetime.now(
            timezone.utc
        ).isoformat(),

        "id_produto": int(
            produto["id_produto"]
        ),

        "produto": (
            produto["produto"]
        ),

        "categoria": (
            produto["categoria"]
        ),

        "quantidade": (
            quantidade
        ),

        "valor_unitario": (
            valor_unitario
        ),

        "valor_total": (
            valor_total
        ),

        "id_forma_pagamento": int(
            forma_pagamento[
                "id_forma_pagamento"
            ]
        ),

        "forma_pagamento": (
            forma_pagamento[
                "forma_pagamento"
            ]
        ),

        "id_canal": int(
            canal["id_canal"]
        ),

        "canal": (
            canal["canal"]
        )
    }

    return evento



# Envio para o Kafka


def enviar_venda(
    evento: dict[str, Any]
) -> None:
    """
    Envia o evento para o tópico Kafka.
    """

    try:

        future = producer.send(
            topic=KAFKA_TOPIC_VENDAS,
            key=evento[
                "id_cliente"
            ],
            value=evento
        )

        metadata = future.get(
            timeout=10
        )

        print("-" * 80)

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
            f"Loja: "
            f"{evento['id_loja']} - "
            f"{evento['nome_loja']}"
        )

        print(
            f"Produto: "
            f"{evento['id_produto']} - "
            f"{evento['produto']}"
        )

        print(
            f"Quantidade: "
            f"{evento['quantidade']}"
        )

        print(
            f"Pagamento: "
            f"{evento['forma_pagamento']}"
        )

        print(
            f"Canal: "
            f"{evento['canal']}"
        )

        print(
            f"Valor total: R$ "
            f"{evento['valor_total']:.2f}"
        )

        print(
            f"Tópico: "
            f"{metadata.topic}"
        )

        print(
            f"Partição: "
            f"{metadata.partition}"
        )

        print(
            f"Offset: "
            f"{metadata.offset}"
        )

    except KafkaError as erro:

        print(
            f"Erro ao enviar o evento "
            f"para o Kafka: {erro}"
        )

        raise



# Execução principal


def executar_producer() -> None:

    print("=" * 80)

    print(
        "Producer iniciado"
    )

    print(
        f"Broker Kafka: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Tópico: "
        f"{KAFKA_TOPIC_VENDAS}"
    )

    print(
        f"Arquivo produtos: "
        f"{ARQUIVO_PRODUTOS}"
    )

    print(
        f"Arquivo formas pagamento: "
        f"{ARQUIVO_FORMAS_PAGAMENTO}"
    )

    print(
        f"Arquivo canais: "
        f"{ARQUIVO_CANAIS}"
    )

    print(
        f"Arquivo lojas: "
        f"{ARQUIVO_LOJAS}"
    )

    print(
        f"Produtos carregados: "
        f"{len(PRODUTOS)}"
    )

    print(
        f"Formas de pagamento carregadas: "
        f"{len(FORMAS_PAGAMENTO)}"
    )

    print(
        f"Canais carregados: "
        f"{len(CANAIS)}"
    )

    print(
        f"Lojas carregadas: "
        f"{len(LOJAS)}"
    )

    print("=" * 80)

    try:

        while True:

            venda = gerar_venda()

            enviar_venda(
                venda
            )

            time.sleep(
                2
            )

    except KeyboardInterrupt:

        print(
            "\nEncerramento solicitado "
            "pelo usuário."
        )

    except Exception as erro:

        print(
            f"\nErro durante a execução "
            f"do producer: {erro}"
        )

        raise

    finally:

        print(
            "Finalizando mensagens pendentes..."
        )

        producer.flush()

        producer.close()

        print(
            "Producer encerrado."
        )



# point


if __name__ == "__main__":
    executar_producer()