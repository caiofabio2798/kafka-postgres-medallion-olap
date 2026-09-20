-- =========================================================
-- 08 - FATO OLAP
-- =========================================================

CREATE TABLE IF NOT EXISTS olap.fato_vendas (
    sk_venda BIGSERIAL PRIMARY KEY,
    id_evento UUID NOT NULL UNIQUE,
    id_venda UUID NOT NULL,
    sk_data INTEGER NOT NULL,
    sk_cliente BIGINT NOT NULL,
    sk_loja BIGINT NOT NULL,
    sk_produto BIGINT NOT NULL,
    sk_canal BIGINT NOT NULL,
    sk_forma_pagamento BIGINT NOT NULL,
    data_hora TIMESTAMPTZ NOT NULL,
    quantidade NUMERIC(12, 3) NOT NULL,
    valor_unitario NUMERIC(14, 2) NOT NULL,
    valor_total NUMERIC(14, 2) NOT NULL,
    data_carga TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fato_data
        FOREIGN KEY (sk_data)
        REFERENCES olap.dim_data(sk_data),

    CONSTRAINT fk_fato_cliente
        FOREIGN KEY (sk_cliente)
        REFERENCES olap.dim_cliente(sk_cliente),

    CONSTRAINT fk_fato_loja
        FOREIGN KEY (sk_loja)
        REFERENCES olap.dim_loja(sk_loja),

    CONSTRAINT fk_fato_produto
        FOREIGN KEY (sk_produto)
        REFERENCES olap.dim_produto(sk_produto),

    CONSTRAINT fk_fato_canal
        FOREIGN KEY (sk_canal)
        REFERENCES olap.dim_canal(sk_canal),

    CONSTRAINT fk_fato_forma_pagamento
        FOREIGN KEY (sk_forma_pagamento)
        REFERENCES olap.dim_forma_pagamento(sk_forma_pagamento),

    CONSTRAINT chk_fato_quantidade_positiva
        CHECK (quantidade > 0),

    CONSTRAINT chk_fato_valor_unitario_positivo
        CHECK (valor_unitario > 0),

    CONSTRAINT chk_fato_valor_total_positivo
        CHECK (valor_total > 0)
);
