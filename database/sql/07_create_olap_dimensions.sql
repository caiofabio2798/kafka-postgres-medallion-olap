-- =========================================================
-- 07 - DIMENSÕES OLAP
-- =========================================================

CREATE TABLE IF NOT EXISTS olap.dim_data (
    sk_data INTEGER PRIMARY KEY,
    data DATE NOT NULL UNIQUE,
    dia INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    ano INTEGER NOT NULL,
    nome_mes VARCHAR(20) NOT NULL,
    trimestre INTEGER NOT NULL,
    semestre INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL,
    nome_dia_semana VARCHAR(20) NOT NULL,
    fim_de_semana BOOLEAN NOT NULL,

    CONSTRAINT chk_dim_data_mes
        CHECK (mes BETWEEN 1 AND 12),

    CONSTRAINT chk_dim_data_trimestre
        CHECK (trimestre BETWEEN 1 AND 4),

    CONSTRAINT chk_dim_data_semestre
        CHECK (semestre BETWEEN 1 AND 2),

    CONSTRAINT chk_dim_data_dia_semana
        CHECK (dia_semana BETWEEN 1 AND 7)
);


CREATE TABLE IF NOT EXISTS olap.dim_cliente (
    sk_cliente BIGSERIAL PRIMARY KEY,
    id_cliente INTEGER NOT NULL UNIQUE,
    data_inicio_vigencia TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN NOT NULL DEFAULT TRUE
);


CREATE TABLE IF NOT EXISTS olap.dim_loja (
    sk_loja BIGSERIAL PRIMARY KEY,
    id_loja INTEGER NOT NULL UNIQUE,
    nome_loja VARCHAR(100) NOT NULL,
    cidade VARCHAR(100),
    estado CHAR(2),
    data_inicio_vigencia TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN NOT NULL DEFAULT TRUE
);


CREATE TABLE IF NOT EXISTS olap.dim_categoria (
    sk_categoria BIGSERIAL PRIMARY KEY,
    categoria VARCHAR(100) NOT NULL UNIQUE,
    data_inicio_vigencia TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN NOT NULL DEFAULT TRUE
);


CREATE TABLE IF NOT EXISTS olap.dim_produto (
    sk_produto BIGSERIAL PRIMARY KEY,
    id_produto INTEGER NOT NULL UNIQUE,
    produto VARCHAR(150) NOT NULL,
    sk_categoria BIGINT NOT NULL,
    data_inicio_vigencia TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,

    CONSTRAINT fk_dim_produto_categoria
        FOREIGN KEY (sk_categoria)
        REFERENCES olap.dim_categoria(sk_categoria)
);


CREATE TABLE IF NOT EXISTS olap.dim_canal (
    sk_canal BIGSERIAL PRIMARY KEY,
    id_canal INTEGER NOT NULL UNIQUE,
    canal VARCHAR(50) NOT NULL,
    data_inicio_vigencia TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN NOT NULL DEFAULT TRUE
);


CREATE TABLE IF NOT EXISTS olap.dim_forma_pagamento (
    sk_forma_pagamento BIGSERIAL PRIMARY KEY,
    id_forma_pagamento INTEGER NOT NULL UNIQUE,
    forma_pagamento VARCHAR(50) NOT NULL,
    data_inicio_vigencia TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN NOT NULL DEFAULT TRUE
);
