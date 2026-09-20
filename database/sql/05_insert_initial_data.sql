-- =========================================================
-- 05 - DADOS INICIAIS / SEED
-- =========================================================
-- As lojas precisam existir previamente porque o consumer OLTP
-- referencia oltp.loja, mas não cria lojas dinamicamente.

INSERT INTO oltp.loja (
    id_loja,
    nome_loja,
    cidade,
    estado
)
VALUES
    (1, 'Loja 1', 'São Paulo', 'SP'),
    (2, 'Loja 2', 'Campinas', 'SP'),
    (3, 'Loja 3', 'Curitiba', 'PR'),
    (4, 'Loja 4', 'Maringá', 'PR'),
    (5, 'Loja 5', 'Londrina', 'PR'),
    (85, 'Loja 85', 'Campo Grande', 'MS')
ON CONFLICT (id_loja)
DO UPDATE SET
    nome_loja = EXCLUDED.nome_loja,
    cidade = EXCLUDED.cidade,
    estado = EXCLUDED.estado;


INSERT INTO oltp.forma_pagamento (
    id_forma_pagamento,
    nome_forma_pagamento
)
VALUES
    (1, 'Dinheiro'),
    (2, 'Cartão Crédito'),
    (3, 'Cartão Débito'),
    (4, 'Pix')
ON CONFLICT (id_forma_pagamento)
DO UPDATE SET
    nome_forma_pagamento = EXCLUDED.nome_forma_pagamento;


INSERT INTO oltp.canal (
    id_canal,
    nome_canal
)
VALUES
    (1, 'Loja Física'),
    (2, 'App'),
    (3, 'E-commerce')
ON CONFLICT (id_canal)
DO UPDATE SET
    nome_canal = EXCLUDED.nome_canal;
