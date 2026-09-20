-- =========================================================
-- 03 - TABELAS TRANSACIONAIS DO OLTP
-- =========================================================

CREATE TABLE IF NOT EXISTS oltp.venda (
    id_venda UUID PRIMARY KEY,
    id_cliente INTEGER NOT NULL,
    id_loja INTEGER NOT NULL,
    id_forma_pagamento INTEGER NOT NULL,
    id_canal INTEGER NOT NULL,
    data_hora TIMESTAMPTZ NOT NULL,
    valor_total NUMERIC(14, 2) NOT NULL,
    data_insercao TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_venda_cliente
        FOREIGN KEY (id_cliente)
        REFERENCES oltp.cliente(id_cliente),

    CONSTRAINT fk_venda_loja
        FOREIGN KEY (id_loja)
        REFERENCES oltp.loja(id_loja),

    CONSTRAINT fk_venda_forma_pagamento
        FOREIGN KEY (id_forma_pagamento)
        REFERENCES oltp.forma_pagamento(id_forma_pagamento),

    CONSTRAINT fk_venda_canal
        FOREIGN KEY (id_canal)
        REFERENCES oltp.canal(id_canal),

    CONSTRAINT chk_venda_valor_total_positivo
        CHECK (valor_total > 0)
);


CREATE TABLE IF NOT EXISTS oltp.item_venda (
    id_item BIGSERIAL PRIMARY KEY,
    id_venda UUID NOT NULL,
    id_produto INTEGER NOT NULL,
    quantidade NUMERIC(12, 3) NOT NULL,
    valor_unitario NUMERIC(12, 2) NOT NULL,
    valor_total NUMERIC(14, 2) NOT NULL,
    data_insercao TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_item_venda
        FOREIGN KEY (id_venda)
        REFERENCES oltp.venda(id_venda),

    CONSTRAINT fk_item_produto
        FOREIGN KEY (id_produto)
        REFERENCES oltp.produto(id_produto),

    CONSTRAINT uk_item_venda_produto
        UNIQUE (id_venda, id_produto),

    CONSTRAINT chk_item_quantidade_positiva
        CHECK (quantidade > 0),

    CONSTRAINT chk_item_valor_unitario_positivo
        CHECK (valor_unitario > 0),

    CONSTRAINT chk_item_valor_total_positivo
        CHECK (valor_total > 0)
);
