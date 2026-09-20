-- =========================================================
-- 06 - ÍNDICES OLTP E CONTROLE
-- =========================================================
-- Índices já gerados automaticamente por PK/UNIQUE não são repetidos.

CREATE INDEX IF NOT EXISTS idx_produto_categoria
ON oltp.produto (id_categoria);

CREATE INDEX IF NOT EXISTS idx_venda_data_hora
ON oltp.venda (data_hora);

CREATE INDEX IF NOT EXISTS idx_venda_cliente
ON oltp.venda (id_cliente);

CREATE INDEX IF NOT EXISTS idx_venda_loja
ON oltp.venda (id_loja);

CREATE INDEX IF NOT EXISTS idx_venda_canal
ON oltp.venda (id_canal);

CREATE INDEX IF NOT EXISTS idx_venda_forma_pagamento
ON oltp.venda (id_forma_pagamento);

CREATE INDEX IF NOT EXISTS idx_item_venda_produto
ON oltp.item_venda (id_produto);

CREATE INDEX IF NOT EXISTS idx_evento_processado_data
ON controle.evento_processado (processado_em);

CREATE INDEX IF NOT EXISTS idx_erro_processamento_criado_em
ON controle.erro_processamento (criado_em);

CREATE INDEX IF NOT EXISTS idx_erro_processamento_kafka
ON controle.erro_processamento (topico, particao, offset_kafka);
