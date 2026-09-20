import os
import shutil
from pathlib import Path

from config.settings import (
    DATA_DIR,
    POSTGRES_DB,
)

from database.init_database import (
    inicializar_banco,
)


def limpar_data_dir() -> None:
    if DATA_DIR.exists():
        shutil.rmtree(
            DATA_DIR
        )

    if "teste" not in str(DATA_DIR).lower():
        raise RuntimeError(
            "DATA_DIR não parece ser de teste. "
            "Operação cancelada."
    )

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    print(
        f"Data Lake de teste preparado: "
        f"{DATA_DIR}"
    )


def executar_setup() -> None:
    print("=" * 80)
    print("Preparação do ambiente de teste")
    print("=" * 80)

    print(
        f"Banco: {POSTGRES_DB}"
    )

    print(
        f"Data Lake: {DATA_DIR}"
    )

    limpar_data_dir()

    inicializar_banco()

    print("=" * 80)
    print(
        "Ambiente de teste preparado."
    )
    print("=" * 80)


if __name__ == "__main__":
    executar_setup()