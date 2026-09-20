-- =========================================================
-- 04 - TABELAS TÉCNICAS DE CONTROLE DO PIPELINE
-- =========================================================

CREATE TABLE IF NOT EXISTS controle.evento_processado (
    id_evento UUID PRIMARY KEY,
    id_venda UUID NOT NULL,
    topico VARCHAR(150) NOT NULL,
    particao INTEGER NOT NULL,
    offset_kafka BIGINT NOT NULL,
    processado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_topico_particao_offset
        UNIQUE (topico, particao, offset_kafka)
);


CREATE TABLE IF NOT EXISTS controle.erro_processamento (
    id_erro BIGSERIAL PRIMARY KEY,
    topico VARCHAR(150),
    particao INTEGER,
    offset_kafka BIGINT,
    mensagem JSONB,
    tipo_erro VARCHAR(150),
    descricao_erro TEXT,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
