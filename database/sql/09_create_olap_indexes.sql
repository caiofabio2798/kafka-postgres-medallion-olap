-- =========================================================
-- 09 - ÍNDICES OLAP
-- =========================================================
-- id_evento já possui índice implícito por UNIQUE.

CREATE INDEX IF NOT EXISTS idx_dim_produto_categoria
ON olap.dim_produto (sk_categoria);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_id_venda
ON olap.fato_vendas (id_venda);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_data
ON olap.fato_vendas (sk_data);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_cliente
ON olap.fato_vendas (sk_cliente);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_loja
ON olap.fato_vendas (sk_loja);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_produto
ON olap.fato_vendas (sk_produto);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_canal
ON olap.fato_vendas (sk_canal);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_forma_pagamento
ON olap.fato_vendas (sk_forma_pagamento);

CREATE INDEX IF NOT EXISTS idx_fato_vendas_data_hora
ON olap.fato_vendas (data_hora);
