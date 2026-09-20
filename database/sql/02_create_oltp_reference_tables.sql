-- =========================================================
-- 02 - TABELAS DE REFERÊNCIA / MESTRE DO OLTP
-- =========================================================

CREATE TABLE IF NOT EXISTS oltp.cliente (
    id_cliente INTEGER PRIMARY KEY,
    data_cadastro TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS oltp.loja (
    id_loja INTEGER PRIMARY KEY,
    nome_loja VARCHAR(100) NOT NULL,
    cidade VARCHAR(100),
    estado CHAR(2),
    data_cadastro TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS oltp.categoria (
    id_categoria SERIAL PRIMARY KEY,
    nome_categoria VARCHAR(100) NOT NULL UNIQUE,
    data_cadastro TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS oltp.forma_pagamento (
    id_forma_pagamento INTEGER PRIMARY KEY,
    nome_forma_pagamento VARCHAR(50) NOT NULL UNIQUE,
    data_cadastro TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS oltp.canal (
    id_canal INTEGER PRIMARY KEY,
    nome_canal VARCHAR(50) NOT NULL UNIQUE,
    data_cadastro TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS oltp.produto (
    id_produto INTEGER PRIMARY KEY,
    nome_produto VARCHAR(150) NOT NULL UNIQUE,
    id_categoria INTEGER NOT NULL,
    preco_atual NUMERIC(12, 2) NOT NULL,
    data_cadastro TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_produto_categoria
        FOREIGN KEY (id_categoria)
        REFERENCES oltp.categoria(id_categoria),

    CONSTRAINT chk_produto_preco_positivo
        CHECK (preco_atual > 0)
);
