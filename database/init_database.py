from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as Connection

from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)



# Diretórios


DATABASE_DIR = Path(__file__).resolve().parent

SQL_DIR = (
    DATABASE_DIR
    / "sql"
)



# Banco administrativo PostgreSQL


POSTGRES_ADMIN_DB = "postgres"



# Ordem de execução dos scripts


SCRIPTS_SQL = [
    "01_create_schemas.sql",
    "02_create_oltp_reference_tables.sql",
    "03_create_oltp_transaction_tables.sql",
    "04_create_control_tables.sql",
    "05_insert_initial_data.sql",
    "06_create_oltp_indexes.sql",
    "07_create_olap_dimensions.sql",
    "08_create_olap_fact.sql",
    "09_create_olap_indexes.sql",
]


# Conexões


def conectar_postgres_admin() -> Connection:
    """
    Conecta ao banco padrão 'postgres'.

    Essa conexão é utilizada somente para verificar
    e criar o banco do projeto.
    """

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_ADMIN_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def conectar_banco_projeto() -> Connection:
    """
    Conecta ao banco principal do projeto.
    """

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )



# Validação dos arquivos


def validar_scripts_sql() -> None:
    """
    Verifica se todos os arquivos SQL necessários
    existem antes de iniciar qualquer alteração
    no banco.
    """

    print(
        "Validando arquivos SQL..."
    )

    arquivos_ausentes = []

    for nome_script in SCRIPTS_SQL:

        caminho = (
            SQL_DIR
            / nome_script
        )

        if not caminho.exists():

            arquivos_ausentes.append(
                str(caminho)
            )

    if arquivos_ausentes:

        mensagem = (
            "Os seguintes scripts SQL "
            "não foram encontrados:\n"
            + "\n".join(
                arquivos_ausentes
            )
        )

        raise FileNotFoundError(
            mensagem
        )

    print(
        f"{len(SCRIPTS_SQL)} scripts SQL encontrados."
    )



# Criação do banco


def banco_existe(
    conexao: Connection
) -> bool:
    """
    Verifica se o banco configurado em POSTGRES_DB
    já existe.
    """

    with conexao.cursor() as cursor:

        cursor.execute(
            """
            SELECT 1
            FROM pg_database
            WHERE datname = %s;
            """,
            (
                POSTGRES_DB,
            )
        )

        return (
            cursor.fetchone()
            is not None
        )


def criar_banco_se_necessario() -> None:
    """
    Cria o banco do projeto quando ele ainda
    não existe.
    """

    conexao = (
        conectar_postgres_admin()
    )

    # CREATE DATABASE não pode executar
    # dentro de uma transação.
    conexao.autocommit = True

    try:

        if banco_existe(
            conexao
        ):

            print(
                f"Banco '{POSTGRES_DB}' "
                "já existe."
            )

            return

        print(
            f"Criando banco "
            f"'{POSTGRES_DB}'..."
        )

        with conexao.cursor() as cursor:

            cursor.execute(
                sql.SQL(
                    "CREATE DATABASE {}"
                ).format(
                    sql.Identifier(
                        POSTGRES_DB
                    )
                )
            )

        print(
            f"Banco '{POSTGRES_DB}' "
            "criado com sucesso."
        )

    finally:

        conexao.close()



# Execução de SQL


def carregar_sql(
    caminho: Path
) -> str:
    """
    Lê um arquivo SQL.
    """

    return caminho.read_text(
        encoding="utf-8"
    )


def executar_script_sql(
    conexao: Connection,
    caminho: Path
) -> None:
    """
    Executa um arquivo SQL utilizando a conexão
    fornecida.
    """

    sql_script = carregar_sql(
        caminho
    )

    if not sql_script.strip():

        raise ValueError(
            f"O arquivo SQL está vazio: "
            f"{caminho}"
        )

    with conexao.cursor() as cursor:

        cursor.execute(
            sql_script
        )



# Execução de todos os scripts


def executar_scripts_sql() -> None:
    """
    Executa os scripts SQL na ordem configurada.

    Cada arquivo possui sua própria transação.

    Se um arquivo falhar:
    - rollback daquele arquivo;
    - pipeline interrompido;
    - scripts posteriores não são executados.
    """

    conexao = (
        conectar_banco_projeto()
    )

    conexao.autocommit = False

    try:

        total_scripts = len(
            SCRIPTS_SQL
        )

        for indice, nome_script in enumerate(
            SCRIPTS_SQL,
            start=1
        ):

            caminho = (
                SQL_DIR
                / nome_script
            )

            print("-" * 80)

            print(
                f"[{indice}/{total_scripts}] "
                f"Executando "
                f"{nome_script}"
            )

            try:

                executar_script_sql(
                    conexao=conexao,
                    caminho=caminho
                )

                conexao.commit()

                print(
                    f"[OK] "
                    f"{nome_script}"
                )

            except Exception as erro:

                conexao.rollback()

                print(
                    f"[ERRO] "
                    f"{nome_script}"
                )

                print(
                    f"Detalhes: {erro}"
                )

                raise

    finally:

        conexao.close()



# Inicialização completa


def inicializar_banco() -> None:
    """
    Inicializa completamente o banco do projeto.
    """

    print("=" * 80)

    print(
        "Inicialização do banco de dados"
    )

    print("=" * 80)

    print(
        f"Servidor PostgreSQL: "
        f"{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}"
    )

    print(
        f"Banco destino: "
        f"{POSTGRES_DB}"
    )

    print(
        f"Usuário: "
        f"{POSTGRES_USER}"
    )

    print(
        f"Diretório SQL: "
        f"{SQL_DIR}"
    )

    print("=" * 80)


    # 1. Validação dos arquivos
  

    validar_scripts_sql()


    # 2. Banco


    criar_banco_se_necessario()


    # 3. Estrutura


    executar_scripts_sql()

    print("=" * 80)

    print(
        "Banco inicializado com sucesso."
    )

    print(
        f"Banco: {POSTGRES_DB}"
    )

    print("=" * 80)



# point


if __name__ == "__main__":
    inicializar_banco()