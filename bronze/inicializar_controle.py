import json
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]

BRONZE_DIR = (
    BASE_DIR
    / "data"
    / "bronze"
    / "vendas"
)

CONTROLE_DIR = (
    BASE_DIR
    / "data"
    / "controle"
)

SQLITE_PATH = (
    CONTROLE_DIR
    / "bronze_offsets.db"
)


CONTROLE_DIR.mkdir(
    parents=True,
    exist_ok=True
)


conexao = sqlite3.connect(
    SQLITE_PATH
)


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


arquivos = list(
    BRONZE_DIR.rglob(
        "*.jsonl"
    )
)


total_lidos = 0
total_inseridos = 0


for arquivo_jsonl in arquivos:

    print(
        f"Lendo: {arquivo_jsonl}"
    )

    with arquivo_jsonl.open(
        mode="r",
        encoding="utf-8"
    ) as arquivo:

        for linha in arquivo:

            linha = linha.strip()

            if not linha:
                continue

            total_lidos += 1

            registro = json.loads(
                linha
            )

            metadata = registro[
                "metadata"
            ]

            evento = registro.get(
                "evento",
                {}
            )

            cursor = conexao.execute(
                """
                INSERT OR IGNORE INTO eventos_bronze (
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
                    metadata["topic"],
                    metadata["partition"],
                    metadata["offset"],
                    evento.get(
                        "id_evento"
                    ),
                    evento.get(
                        "id_venda"
                    ),
                    str(
                        arquivo_jsonl
                    ),
                    metadata[
                        "ingested_at"
                    ]
                )
            )

            if cursor.rowcount == 1:
                total_inseridos += 1


conexao.commit()
conexao.close()


print("=" * 80)
print(
    f"Registros lidos: "
    f"{total_lidos}"
)
print(
    f"Registros inseridos: "
    f"{total_inseridos}"
)
print(
    f"SQLite: {SQLITE_PATH}"
)
print("=" * 80)