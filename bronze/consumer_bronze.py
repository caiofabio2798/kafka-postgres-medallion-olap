import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config.settings import (
    BRONZE_VENDAS_DIR,
    CONTROLE_DIR,
    BRONZE_SQLITE_PATH,
    BRONZE_MAX_REGISTROS_POR_ARQUIVO,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_VENDAS,
    KAFKA_GROUP_BRONZE,
)



# Aliases locais


BRONZE_DIR = BRONZE_VENDAS_DIR

SQLITE_PATH = BRONZE_SQLITE_PATH

MAX_REGISTROS_POR_ARQUIVO = (
    BRONZE_MAX_REGISTROS_POR_ARQUIVO
)

KAFKA_GROUP_ID = KAFKA_GROUP_BRONZE



# SQLite


def conectar_sqlite() -> sqlite3.Connection:
    """
    cria conexão com sqlite responsável pelo
    controle de idempotência da bronze.
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
    Cria a tabela de controle da Bronze.
    """

    conexao.execute(
        """
        CREATE TABLE IF NOT EXISTS eventos_bronze (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            topic TEXT NOT NULL,
            partition_kafka INTEGER NOT NULL,
            offset_kafka INTEGER NOT NULL,

            id_evento TEXT,
            id_venda TEXT,

            arquivo TEXT NOT NULL,
            processado_em TEXT NOT NULL,

            UNIQUE (
                topic,
                partition_kafka,
                offset_kafka
            )
        );
        """
    )

    conexao.commit()


def evento_ja_processado(
    conexao: sqlite3.Connection,
    topic: str,
    partition: int,
    offset: int
) -> bool:
    """
    Verifica se o offset Kafka já foi persistido
    na Bronze.
    """

    cursor = conexao.execute(
        """
        SELECT 1
        FROM eventos_bronze
        WHERE topic = ?
          AND partition_kafka = ?
          AND offset_kafka = ?
        LIMIT 1;
        """,
        (
            topic,
            partition,
            offset
        )
    )

    return cursor.fetchone() is not None


def registrar_evento_processado(
    conexao: sqlite3.Connection,
    mensagem,
    arquivo_destino: Path,
    data_processamento: datetime
) -> None:
    """
    Registra a mensagem já persistida na Bronze.
    """

    evento = mensagem.value

    id_evento = None
    id_venda = None

    if isinstance(evento, dict):

        id_evento = evento.get(
            "id_evento"
        )

        id_venda = evento.get(
            "id_venda"
        )

    conexao.execute(
        """
        INSERT INTO eventos_bronze (
            topic,
            partition_kafka,
            offset_kafka,
            id_evento,
            id_venda,
            arquivo,
            processado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (
            mensagem.topic,
            mensagem.partition,
            mensagem.offset,
            id_evento,
            id_venda,
            str(arquivo_destino),
            data_processamento.isoformat()
        )
    )

    conexao.commit()



# Kafka Consumer


def criar_consumer() -> KafkaConsumer:
    """
    Cria o consumer exclusivo da camada Bronze.
    """

    return KafkaConsumer(
        KAFKA_TOPIC_VENDAS,

        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        group_id=KAFKA_GROUP_ID,

        auto_offset_reset="earliest",

        enable_auto_commit=False,

        value_deserializer=lambda valor: json.loads(
            valor.decode("utf-8")
        )
    )



# Particionamento


def obter_diretorio_particao(
    data_ingestao: datetime
) -> Path:
    """
    Cria o diretório particionado por:
    ano / mês / dia.
    """

    diretorio = (
        BRONZE_DIR
        / f"ano={data_ingestao.year}"
        / f"mes={data_ingestao.month:02d}"
        / f"dia={data_ingestao.day:02d}"
    )

    diretorio.mkdir(
        parents=True,
        exist_ok=True
    )

    return diretorio



# Controle dos arquivos


def contar_linhas(
    arquivo: Path
) -> int:
    """
    Conta quantas linhas existem em um JSONL.
    """

    if not arquivo.exists():
        return 0

    with arquivo.open(
        mode="r",
        encoding="utf-8"
    ) as f:

        return sum(
            1
            for _ in f
        )


def listar_arquivos_part(
    diretorio: Path
) -> list[Path]:
    """
    Lista os arquivos part-*.jsonl existentes
    na partição.
    """

    return sorted(
        diretorio.glob(
            "part-*.jsonl"
        )
    )


def gerar_nome_arquivo(
    numero: int
) -> str:
    """
    Gera nomes dos arquivos
    """

    return (
        f"part-{numero:06d}.jsonl"
    )


def obter_arquivo_destino(
    diretorio: Path
) -> Path:
    """
    Retorna o arquivo atual da partição.

    Caso o arquivo tenha atingido o limite de
    registros, cria o próximo arquivo.
    """

    arquivos = listar_arquivos_part(
        diretorio
    )

    if not arquivos:

        return (
            diretorio
            / gerar_nome_arquivo(1)
        )

    ultimo_arquivo = arquivos[-1]

    quantidade = contar_linhas(
        ultimo_arquivo
    )

    if quantidade < MAX_REGISTROS_POR_ARQUIVO:

        return ultimo_arquivo

    nome = ultimo_arquivo.stem

    numero_atual = int(
        nome.split("-")[1]
    )

    proximo_numero = (
        numero_atual + 1
    )

    return (
        diretorio
        / gerar_nome_arquivo(
            proximo_numero
        )
    )



# Construção do registro Bronze


def construir_registro_bronze(
    mensagem,
    data_ingestao: datetime
) -> dict:
    """
    Preserva o evento original e adiciona
    metadados técnicos.
    """

    kafka_timestamp = None

    if mensagem.timestamp is not None:

        kafka_timestamp = datetime.fromtimestamp(
            mensagem.timestamp / 1000,
            tz=timezone.utc
        ).isoformat()

    return {
        "metadata": {
            "topic": mensagem.topic,
            "partition": mensagem.partition,
            "offset": mensagem.offset,
            "kafka_timestamp": kafka_timestamp,
            "ingested_at": data_ingestao.isoformat()
        },

        "evento": mensagem.value
    }



# Persistência Bronze


def salvar_evento_bronze(
    mensagem
) -> tuple[Path, datetime]:
    """
    Persiste uma mensagem Kafka em JSONL.
    """

    data_ingestao = datetime.now(
        timezone.utc
    )

    diretorio = obter_diretorio_particao(
        data_ingestao
    )

    arquivo_destino = obter_arquivo_destino(
        diretorio
    )

    registro = construir_registro_bronze(
        mensagem,
        data_ingestao
    )

    with arquivo_destino.open(
        mode="a",
        encoding="utf-8"
    ) as arquivo:

        arquivo.write(
            json.dumps(
                registro,
                ensure_ascii=False
            )
        )

        arquivo.write("\n")

        arquivo.flush()

        os.fsync(
            arquivo.fileno()
        )

    return (
        arquivo_destino,
        data_ingestao
    )



# Processamento


def processar_mensagem(
    conexao_sqlite: sqlite3.Connection,
    mensagem
) -> str:
    """
    Processa uma mensagem com controle
    de idempotência.
    """

    if evento_ja_processado(
        conexao=conexao_sqlite,
        topic=mensagem.topic,
        partition=mensagem.partition,
        offset=mensagem.offset
    ):

        return "duplicado"

    arquivo_destino, data_ingestao = (
        salvar_evento_bronze(
            mensagem
        )
    )

    registrar_evento_processado(
        conexao=conexao_sqlite,
        mensagem=mensagem,
        arquivo_destino=arquivo_destino,
        data_processamento=data_ingestao
    )

    return "processado"



# Execução


def executar_consumer_bronze() -> None:
    """
    Executa continuamente o consumer Bronze.
    """

    consumer = criar_consumer()

    conexao_sqlite = conectar_sqlite()

    criar_tabela_controle(
        conexao_sqlite
    )

    print("=" * 80)
    print(
        "Consumer Bronze iniciado"
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
        f"Destino Bronze: "
        f"{BRONZE_DIR}"
    )

    print(
        f"Controle SQLite: "
        f"{SQLITE_PATH}"
    )

    print(
        f"Máximo por arquivo: "
        f"{MAX_REGISTROS_POR_ARQUIVO}"
    )

    print("=" * 80)

    try:

        for mensagem in consumer:

            try:

                resultado = processar_mensagem(
                    conexao_sqlite,
                    mensagem
                )

                consumer.commit()

                print("-" * 80)

                if resultado == "duplicado":

                    print(
                        "Evento já existente na Bronze."
                    )

                else:

                    print(
                        "Evento persistido na Bronze."
                    )

                print(
                    f"Tópico: "
                    f"{mensagem.topic}"
                )

                print(
                    f"Partição: "
                    f"{mensagem.partition}"
                )

                print(
                    f"Offset: "
                    f"{mensagem.offset}"
                )

                if isinstance(
                    mensagem.value,
                    dict
                ):

                    print(
                        f"Evento: "
                        f"{mensagem.value.get('id_evento')}"
                    )

                    print(
                        f"Venda: "
                        f"{mensagem.value.get('id_venda')}"
                    )

            except sqlite3.IntegrityError:

                conexao_sqlite.rollback()

                consumer.commit()

                print(
                    f"Offset "
                    f"{mensagem.offset} "
                    "já registrado."
                )

            except Exception as erro:

                conexao_sqlite.rollback()

                print("-" * 80)

                print(
                    f"Erro ao processar "
                    f"offset={mensagem.offset}: "
                    f"{erro}"
                )


    except KeyboardInterrupt:

        print(
            "\nEncerramento solicitado pelo usuário."
        )

    except KafkaError as erro:

        print(
            f"Erro de infraestrutura Kafka: "
            f"{erro}"
        )

        raise

    finally:

        conexao_sqlite.close()

        consumer.close()

        print(
            "Consumer Bronze encerrado."
        )



# point


if __name__ == "__main__":
    executar_consumer_bronze()