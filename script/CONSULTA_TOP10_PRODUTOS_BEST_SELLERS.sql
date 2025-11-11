
WITH vendas_por_regiao AS (
    SELECT 
        DATE_TRUNC('month', p.data)::DATE as mes_referencia,
        EXTRACT(YEAR FROM p.data) as ano,
        EXTRACT(MONTH FROM p.data) as mes,
        COALESCE(g.regiao, 'Outros') as regiao,
        g.uf,
        pr.id as id_produto,
        pr.nome as nome_produto,
        m.nome as marca,
        SUM(pi.qtd_produto) as qtd_vendida,
        COUNT(DISTINCT p.id) as qtd_pedidos,
        SUM(pi.qtd_produto * pi.vlr_unitario) as receita_total
    FROM trusted.pedido p
    INNER JOIN trusted.pedido_item pi ON p.id = pi.id_pedido
    INNER JOIN trusted.produto pr ON pi.id_produto = pr.id
    INNER JOIN trusted.marca m ON pr.id_marca = m.id
    LEFT JOIN trusted.geografia g ON p.id_geografia_entrega = g.id_geografia
    WHERE pi.flg_cancelado = 'N'  -- Apenas itens não cancelados
      AND p.status = 'FINALIZADO'  -- Apenas pedidos finalizados
    GROUP BY 
        DATE_TRUNC('month', p.data),
        EXTRACT(YEAR FROM p.data),
        EXTRACT(MONTH FROM p.data),
        g.regiao,
        g.uf,
        pr.id,
        pr.nome,
        m.nome
),
ranking_produtos AS (
    SELECT 
        mes_referencia,
        ano,
        mes,
        regiao,
        id_produto,
        nome_produto,
        marca,
        qtd_vendida,
        qtd_pedidos,
        receita_total,
        RANK() OVER (
            PARTITION BY mes_referencia, regiao 
            ORDER BY qtd_vendida DESC
        ) as posicao_regiao,
        RANK() OVER (
            PARTITION BY mes_referencia 
            ORDER BY qtd_vendida DESC
        ) as posicao_geral
        
    FROM vendas_por_regiao
)
-- Top 10 produtos por mês e por região
SELECT 
    ano,
    mes,
    mes_referencia,
    regiao,
    posicao_regiao as ranking,
    nome_produto,
    marca,
    qtd_vendida,
    qtd_pedidos,
    ROUND(receita_total, 2) as receita_total,
    ROUND(receita_total / qtd_vendida, 2) as preco_medio_unitario,
    posicao_geral as ranking_nacional
    
FROM ranking_produtos
WHERE posicao_regiao <= 10  -- Top 10 por região

ORDER BY 
    mes_referencia DESC,
    regiao,
    posicao_regiao;

-- =====================================================
-- OPÇÃO 2: Top 10 por MÊS e por ESTADO (UF)
-- =====================================================
SELECT 
    mes_referencia,
    ano,
    mes,
    regiao,
    uf,
    ranking,
    nome_produto,
    qtd_vendida
FROM refined.top10_best_sellers_regiao
WHERE ranking <= 10  -- Top 10 por região
ORDER BY 
    mes_referencia DESC,
    regiao,
    uf,
    ranking;

-- =====================================================
-- OPÇÃO 3: Criar VIEW Materializada (Recomendado)
-- Para melhor performance em dashboards
-- =====================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS refined.top10_best_sellers_regiao AS
WITH vendas_por_regiao AS (
    SELECT 
        DATE_TRUNC('month', p.data)::DATE as mes_referencia,
        EXTRACT(YEAR FROM p.data) as ano,
        EXTRACT(MONTH FROM p.data) as mes,
        
        -- Região geográfica via tabela geografia
        COALESCE(g.regiao, 'Outros') as regiao,
        g.uf,
        pr.id as id_produto,
        pr.nome as nome_produto,
        m.nome as marca,
        
        SUM(pi.qtd_produto) as qtd_vendida,
        COUNT(DISTINCT p.id) as qtd_pedidos,
        SUM(pi.qtd_produto * pi.vlr_unitario) as receita_total,
        ROUND(AVG(pi.vlr_unitario), 2) as preco_medio_unitario
        
    FROM trusted.pedido p
    INNER JOIN trusted.pedido_item pi ON p.id = pi.id_pedido
    INNER JOIN trusted.produto pr ON pi.id_produto = pr.id
    INNER JOIN trusted.marca m ON pr.id_marca = m.id
    LEFT JOIN trusted.geografia g ON p.id_geografia_entrega = g.id_geografia
    
    WHERE pi.flg_cancelado = 'N'
      AND p.status = 'FINALIZADO'
    
    GROUP BY 
        DATE_TRUNC('month', p.data),
        EXTRACT(YEAR FROM p.data),
        EXTRACT(MONTH FROM p.data),
        g.regiao,
        g.uf,
        pr.id,
        pr.nome,
        m.nome
)
SELECT 
    mes_referencia,
    ano,
    mes,
    regiao,
    RANK() OVER (
        PARTITION BY mes_referencia, regiao 
        ORDER BY qtd_vendida DESC
    ) as ranking,
    nome_produto,
    marca,
    qtd_vendida,
    qtd_pedidos,
    receita_total,
    preco_medio_unitario    
FROM vendas_por_regiao;

-- Criar índices para performance
CREATE INDEX IF NOT EXISTS idx_top10_mes_regiao 
    ON refined.top10_best_sellers_regiao(mes_referencia, regiao, ranking);

CREATE INDEX IF NOT EXISTS idx_top10_produto 
    ON refined.top10_best_sellers_regiao(nome_produto);

-- Atualização: Refresh diário via Airflow
COMMENT ON MATERIALIZED VIEW refined.top10_best_sellers_regiao IS 
'Top 10 produtos mais vendidos por quantidade, por mês e por região geográfica. 
Refresh diário via Airflow. 
Stakeholder: Todos (análise de best sellers)';


-- =====================================================
-- EXEMPLOS DE USO
-- =====================================================

-- Exemplo 1: Top 10 do Sudeste em Janeiro/2024
SELECT * 
FROM refined.top10_best_sellers_regiao
WHERE mes_referencia = '2024-01-01'
  AND regiao = 'Sudeste'
  AND ranking <= 10
ORDER BY ranking;

-- Exemplo 2: Evolução de um produto específico
SELECT 
    mes_referencia,
    regiao,
    ranking,
    qtd_vendida,
    participacao_pct
FROM refined.top10_best_sellers_regiao
WHERE nome_produto LIKE '%Nike%'
ORDER BY mes_referencia DESC, regiao;

-- Exemplo 3: Comparação entre regiões no mesmo mês
SELECT 
    regiao,
    ranking,
    nome_produto,
    qtd_vendida
FROM refined.top10_best_sellers_regiao
WHERE mes_referencia = '2024-01-01'
  AND ranking <= 5
ORDER BY regiao, ranking;

-- Exemplo 4: Produtos que estão no Top 10 em TODAS as regiões
WITH produtos_top10 AS (
    SELECT DISTINCT
        mes_referencia,
        nome_produto,
        COUNT(DISTINCT regiao) as qtd_regioes
    FROM refined.top10_best_sellers_regiao
    WHERE ranking <= 10
    GROUP BY mes_referencia, nome_produto
    HAVING COUNT(DISTINCT regiao) = 5  -- 5 regiões
)
SELECT 
    mes_referencia,
    nome_produto,
    'Best Seller Nacional' as status
FROM produtos_top10
ORDER BY mes_referencia DESC;


-- =====================================================
-- FUNÇÃO: Refresh automático
-- =====================================================

CREATE OR REPLACE FUNCTION refined.fn_refresh_top10_best_sellers()
RETURNS void AS $$
BEGIN
    RAISE NOTICE 'Atualizando view de best sellers...';
    REFRESH MATERIALIZED VIEW refined.top10_best_sellers_regiao;
    RAISE NOTICE 'Best sellers atualizado com sucesso!';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refined.fn_refresh_top10_best_sellers IS 
'Atualiza a view materializada de top 10 best sellers. 
Executar diariamente via Airflow após carga de dados.';


-- =====================================================
-- VALIDAÇÃO DA QUERY
-- =====================================================

-- Verifica se há dados
SELECT 
    COUNT(*) as total_linhas,
    COUNT(DISTINCT mes_referencia) as meses_distintos,
    COUNT(DISTINCT regiao) as regioes_distintas,
    MIN(mes_referencia) as primeiro_mes,
    MAX(mes_referencia) as ultimo_mes
FROM refined.top10_best_sellers_regiao;

-- Verifica se o ranking está correto (deve ter no máximo 10 por mês/região)
SELECT 
    mes_referencia,
    regiao,
    MAX(ranking) as max_ranking,
    COUNT(*) as qtd_produtos
FROM refined.top10_best_sellers_regiao
GROUP BY mes_referencia, regiao
ORDER BY mes_referencia DESC;


-- =====================================================
-- FIM
-- Desenvolvido por: Camila Macedo
-- Para: Grupo SBF - Analytics Engineer Case
-- Objetivo: Responder pergunta dos stakeholders sobre
--           top 10 produtos best sellers por mês e região
-- =====================================================

