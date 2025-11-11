
-- 1️⃣ Criação dos schemas
CREATE SCHEMA IF NOT EXISTS trusted;
CREATE SCHEMA IF NOT EXISTS refined;

-- =====================================================
-- 2️⃣ Tabelas TRUSTED (baseadas no modelo original + melhorias LGPD)
-- =====================================================

-- trusted.marca
CREATE TABLE IF NOT EXISTS trusted.marca (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- trusted.categoria (Hierárquica)
CREATE TABLE IF NOT EXISTS trusted.categoria (
    id_categoria SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL UNIQUE,
    id_categoria_pai INTEGER,
    nivel INTEGER NOT NULL CHECK (nivel BETWEEN 1 AND 3),
    caminho_completo VARCHAR(500),
    ativo BOOLEAN DEFAULT TRUE,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_categoria_pai FOREIGN KEY (id_categoria_pai) 
        REFERENCES trusted.categoria(id_categoria)
);

CREATE INDEX IF NOT EXISTS idx_categoria_pai ON trusted.categoria(id_categoria_pai);
CREATE INDEX IF NOT EXISTS idx_categoria_nivel ON trusted.categoria(nivel);

-- trusted.produto
CREATE TABLE IF NOT EXISTS trusted.produto (
    id SERIAL PRIMARY KEY,
    id_marca INTEGER NOT NULL,
    nome VARCHAR(255) NOT NULL,
    descricao TEXT DEFAULT 'Sem descrição disponível',
    id_categoria INTEGER,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_produto_marca FOREIGN KEY (id_marca) REFERENCES trusted.marca (id),
    CONSTRAINT fk_produto_categoria FOREIGN KEY (id_categoria) 
        REFERENCES trusted.categoria(id_categoria)
);


-- trusted.cliente_pii (dados pessoais)
CREATE TABLE IF NOT EXISTS trusted.cliente_pii (
    cliente_id SERIAL PRIMARY KEY,
    nome_full VARCHAR(255),
    cpf CHAR(11),
    email VARCHAR(255),
    telefone VARCHAR(50),
    endereco JSONB,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- trusted.cliente_pseudo (dados pseudonimizados)
CREATE TABLE IF NOT EXISTS trusted.cliente_pseudo (
    cliente_id INTEGER PRIMARY KEY REFERENCES trusted.cliente_pii(cliente_id),
    cliente_id_hash CHAR(64) UNIQUE
);

-- trusted.data (dimensão de tempo)
CREATE TABLE IF NOT EXISTS trusted.data (
    data DATE PRIMARY KEY,
    ano INTEGER,
    mes INTEGER,
    dia INTEGER,
    descricao VARCHAR(50)
);

-- trusted.pedido
CREATE TABLE IF NOT EXISTS trusted.pedido (
    id SERIAL PRIMARY KEY,
    data DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'FINALIZADO',
    sgl_uf_entrega CHAR(2),
    vlr_total NUMERIC(12,2),
    cliente_id_hash CHAR(64),
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pedido_cliente FOREIGN KEY (cliente_id_hash) REFERENCES trusted.cliente_pseudo (cliente_id_hash),
    CONSTRAINT fk_pedido_data FOREIGN KEY (data) REFERENCES trusted.data (data),
    CONSTRAINT chk_pedido_sgl_uf_entrega CHECK (sgl_uf_entrega IS NULL OR sgl_uf_entrega ~ '^[A-Z]{2}$')
);



-- trusted.pedido_item
CREATE TABLE IF NOT EXISTS trusted.pedido_item (
    id SERIAL PRIMARY KEY,
    id_pedido INTEGER NOT NULL,
    id_produto INTEGER NOT NULL,
    flg_cancelado CHAR(1) DEFAULT 'N',
    qtd_produto INTEGER,
    vlr_unitario NUMERIC(10,2),
    CONSTRAINT fk_pedidoitem_pedido FOREIGN KEY (id_pedido) REFERENCES trusted.pedido (id),
    CONSTRAINT fk_pedidoitem_produto FOREIGN KEY (id_produto) REFERENCES trusted.produto (id)
);

-- trusted.meta
CREATE TABLE IF NOT EXISTS trusted.meta (
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER,
    id_marca INTEGER NOT NULL,
    valor NUMERIC(12,2),
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_meta PRIMARY KEY (ano, mes, id_marca),
    CONSTRAINT fk_meta_marca FOREIGN KEY (id_marca) REFERENCES trusted.marca (id)
);

-- =====================================================
-- 3️⃣ Índices para performance
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_pedido_data_uf ON trusted.pedido (data, sgl_uf_entrega);
CREATE INDEX IF NOT EXISTS idx_pedido_item_produto ON trusted.pedido_item (id_produto);
CREATE INDEX IF NOT EXISTS idx_produto_marca ON trusted.produto (id_marca);
CREATE INDEX IF NOT EXISTS idx_produto_categoria ON trusted.produto (id_categoria);

-- =====================================================
-- 4️⃣ Tabelas REFINED (para consumo analítico)
-- =====================================================

CREATE TABLE IF NOT EXISTS refined.mais_vendidos_mensal_estado (
    mes_ano DATE,
    sgl_uf_entrega CHAR(2),
    id_produto INTEGER,
    nome_produto VARCHAR(255),
    total_qtd INTEGER,
    posicao INTEGER,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_mais_vendidos PRIMARY KEY (mes_ano, sgl_uf_entrega, id_produto)
);

CREATE TABLE IF NOT EXISTS refined.vendas_categoria_variacao (
    categoria VARCHAR(100),
    mes_ano DATE,
    total_qtd INTEGER,
    pct_variacao NUMERIC(6,2),
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS refined.kpis_vendas (
    mes_ano DATE,
    receita_bruta NUMERIC(14,2),
    ticket_medio NUMERIC(10,2),
    pct_cancelamento NUMERIC(5,2),
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 5️⃣ Tabelas auxiliares e de governança
-- =====================================================

CREATE TABLE IF NOT EXISTS trusted.log_ingestao (
    id SERIAL PRIMARY KEY,
    tabela VARCHAR(100),
    data_ingestao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario VARCHAR(50),
    qtd_registros INTEGER
);

CREATE TABLE IF NOT EXISTS trusted.dicionario_de_dados (
    tabela VARCHAR(100),
    coluna VARCHAR(100),
    descricao TEXT,
    exemplo TEXT
);

-- =====================================================
-- 6️⃣ Função e triggers de atualização automática de timestamps
-- =====================================================
CREATE OR REPLACE FUNCTION trusted.set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.atualizado_em = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    PERFORM 1 FROM pg_trigger WHERE tgname = 'trg_set_timestamp_marca';
    IF NOT FOUND THEN
        CREATE TRIGGER trg_set_timestamp_marca BEFORE UPDATE ON trusted.marca
        FOR EACH ROW EXECUTE FUNCTION trusted.set_timestamp();
    END IF;

    PERFORM 1 FROM pg_trigger WHERE tgname = 'trg_set_timestamp_produto';
    IF NOT FOUND THEN
        CREATE TRIGGER trg_set_timestamp_produto BEFORE UPDATE ON trusted.produto
        FOR EACH ROW EXECUTE FUNCTION trusted.set_timestamp();
    END IF;

    PERFORM 1 FROM pg_trigger WHERE tgname = 'trg_set_timestamp_pedido';
    IF NOT FOUND THEN
        CREATE TRIGGER trg_set_timestamp_pedido BEFORE UPDATE ON trusted.pedido
        FOR EACH ROW EXECUTE FUNCTION trusted.set_timestamp();
    END IF;

    PERFORM 1 FROM pg_trigger WHERE tgname = 'trg_set_timestamp_meta';
    IF NOT FOUND THEN
        CREATE TRIGGER trg_set_timestamp_meta BEFORE UPDATE ON trusted.meta
        FOR EACH ROW EXECUTE FUNCTION trusted.set_timestamp();
    END IF;
END $$;

-- =====================================================
-- 7️⃣ Tratamento de nulls e atualização da dimensão de data
-- =====================================================
ALTER TABLE trusted.produto ALTER COLUMN descricao SET DEFAULT 'Sem descrição disponível';
ALTER TABLE trusted.pedido ALTER COLUMN status SET DEFAULT 'FINALIZADO';

SET lc_time = 'pt_BR.UTF-8';
UPDATE trusted.data SET descricao = TO_CHAR(data, 'TMDay, DD TMMonth YYYY');
