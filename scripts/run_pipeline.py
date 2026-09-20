from silver.bronze_to_silver import (
    executar_pipeline as executar_silver,
)

from gold.silver_to_gold import (
    executar_pipeline as executar_gold,
)

from gold.agregacoes_gold import (
    executar_pipeline as executar_agregacoes,
)

from olap.gold_to_olap import (
    executar_pipeline as executar_olap,
)


def executar_pipeline_completo() -> None:
    print("=" * 80)
    print("Pipeline batch iniciado")
    print("=" * 80)

    print("\n[1/4] Bronze → Silver")
    executar_silver()

    print("\n[2/4] Silver → Gold")
    executar_gold()

    print("\n[3/4] Agregações Gold")
    executar_agregacoes()

    print("\n[4/4] Gold → OLAP")
    executar_olap()

    print("=" * 80)
    print("Pipeline batch concluído.")
    print("=" * 80)


if __name__ == "__main__":
    executar_pipeline_completo()