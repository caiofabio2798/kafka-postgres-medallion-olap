import psycopg2

from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


def conectar_postgres():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def executar_contagem(
    conexao,
    consulta: str
) -> int:

    with conexao.cursor() as cursor:
        cursor.execute(
            consulta
        )

        resultado = cursor.fetchone()

        return int(
            resultado[0]
        )


def executar_validacao() -> None:

    conexao = conectar_postgres()

    try:
        vendas = executar_contagem(
            conexao,
            """
            SELECT COUNT(*)
            FROM oltp.venda;
            """
        )

        itens = executar_contagem(
            conexao,
            """
            SELECT COUNT(*)
            FROM oltp.item_venda;
            """
        )

        eventos = executar_contagem(
            conexao,
            """
            SELECT COUNT(*)
            FROM controle.evento_processado;
            """
        )

        fatos = executar_contagem(
            conexao,
            """
            SELECT COUNT(*)
            FROM olap.fato_vendas;
            """
        )

        with conexao.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(
                        SUM(valor_total),
                        0
                    ),
                    COALESCE(
                        SUM(quantidade),
                        0
                    )
                FROM olap.fato_vendas;
                """
            )

            receita, quantidade = (
                cursor.fetchone()
            )

        print("=" * 80)
        print("Validação do pipeline")
        print("=" * 80)

        print(
            f"OLTP vendas:              {vendas}"
        )

        print(
            f"OLTP itens:               {itens}"
        )

        print(
            f"Eventos processados:      {eventos}"
        )

        print(
            f"OLAP fato_vendas:         {fatos}"
        )

        print(
            f"Receita OLAP:             "
            f"R$ {float(receita):,.2f}"
        )

        print(
            f"Quantidade OLAP:          "
            f"{float(quantidade):,.0f}"
        )

        print("-" * 80)

        oltp_ok = (
            vendas
            == itens
            == eventos
        )

        olap_ok = (
            fatos == vendas
        )

        if oltp_ok:
            print(
                "[OK] OLTP consistente."
            )
        else:
            print(
                "[ERRO] Contagens OLTP divergentes."
            )

        if olap_ok:
            print(
                "[OK] OLAP reconciliado com OLTP."
            )
        else:
            print(
                "[ERRO] OLAP divergente do OLTP."
            )

        print("=" * 80)

        if not (
            oltp_ok
            and olap_ok
        ):
            raise RuntimeError(
                "Validação do pipeline falhou."
            )

        print(
            "Pipeline validado com sucesso."
        )

    finally:
        conexao.close()


if __name__ == "__main__":
    executar_validacao()