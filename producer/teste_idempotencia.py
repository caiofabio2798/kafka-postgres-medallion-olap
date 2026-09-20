import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv(override=True)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC_VENDAS = os.getenv(
    "KAFKA_TOPIC_VENDAS",
    "vendas-tempo-real"
)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    key_serializer=lambda chave: str(
        chave
    ).encode("utf-8"),

    value_serializer=lambda valor: json.dumps(
        valor,
        ensure_ascii=False
    ).encode("utf-8"),

    acks="all"
)


evento = {
    "id_evento": "41cde858-5366-4b2e-b3e9-fe1725b36be4",
    "versao_evento": 1,
    "tipo_evento": "venda_realizada",
    "origem": "teste_idempotencia",

    "id_venda": "624ec461-957b-4539-8e0e-e98be89d7d62",

    "id_cliente": 2751,

    "id_loja": 1,
    "nome_loja": "Loja 1",
    "cidade_loja": "São Paulo",
    "estado_loja": "SP",

    "data_hora": "2026-08-12T01:00:00+00:00",

    "id_produto": 7,
    "produto": "Frango kg",
    "categoria": "Açougue",

    "quantidade": 3,
    "valor_unitario": 16.90,
    "valor_total": 50.70,

    "id_forma_pagamento": 4,
    "forma_pagamento": "Pix",

    "id_canal": 2,
    "canal": "App"
}


future = producer.send(
    topic=KAFKA_TOPIC_VENDAS,
    key=evento["id_cliente"],
    value=evento
)


metadata = future.get(timeout=10)


print("Evento duplicado enviado.")
print(f"Evento: {evento['id_evento']}")
print(f"Venda: {evento['id_venda']}")
print(f"Offset novo Kafka: {metadata.offset}")


producer.flush()
producer.close()