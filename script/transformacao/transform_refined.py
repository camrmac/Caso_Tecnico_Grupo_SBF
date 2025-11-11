import os
import textwrap
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# ==========================================================
# 🧩 Carregar variáveis de ambiente (.env)
# ==========================================================
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# ==========================================================
# 🔗 Conexão com o banco
# ==========================================================
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ==========================================================
# 🧠 Funções utilitárias de log
# ==========================================================
def log(msg: str):
    print(f"🟢 {msg}")

def erro(msg: str):
    print(f"❌ ERRO: {msg}")

# ==========================================================
# 📂 Criação automática dos schemas
# ==========================================================
def inicializar_schemas():
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS trusted;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS refined;"))
    log("📂 Schemas verificados/criados com sucesso.")

# ==========================================================
# 🥇 Tabela: mais_vendidos_mensal_estado
# ==========================================================
def carregar_best_sellers():
    log("Gerando tabela refined.mais_vendidos_mensal_estado...")

    query = text(textwrap.dedent("""
        DROP TABLE IF EXISTS refined.mais_vendidos_mensal_estado;
        CREATE TABLE refined.mais_vendidos_mensal_estado AS
        SELECT
            DATE_TRUNC('month', p.data)::DATE AS mes_ano,
            p.sgl_uf_entrega,
            i.id_produto,
            pr.nome AS nome_produto,
            SUM(i.qtd_produto) AS total_qtd,
            RANK() OVER (
                PARTITION BY DATE_TRUNC('month', p.data), p.sgl_uf_entrega
                ORDER BY SUM(i.qtd_produto) DESC
            ) AS posicao
        FROM trusted.pedido p
        JOIN trusted.pedido_item i ON i.id_pedido = p.id
        JOIN trusted.produto pr ON pr.id = i.id_produto
        GROUP BY DATE_TRUNC('month', p.data), p.sgl_uf_entrega, i.id_produto, pr.nome;
    """))

    with engine.begin() as conn:
        conn.execute(query)
    log("✅ Tabela refined.mais_vendidos_mensal_estado criada com sucesso.")

# ==========================================================
# 📊 Tabela: performance_mensal_marca
# ==========================================================
def carregar_performance_mensal():
    log("Gerando tabela refined.performance_mensal_marca...")

    query = text(textwrap.dedent("""
        DROP TABLE IF EXISTS refined.performance_mensal_marca;
        CREATE TABLE refined.performance_mensal_marca AS
        SELECT
            d.ano,
            d.mes,
            m.id,
            m.nome AS nome_marca,
            SUM(p.vlr_total) AS vlr_total_vendido,
            COALESCE(mt.valor, 0) AS vlr_meta,
            ROUND(
                (SUM(p.vlr_total) / NULLIF(mt.valor, 0)) * 100,
                2
            ) AS perc_atingimento_meta
        FROM trusted.pedido_item pi
        JOIN trusted.pedido p ON pi.id_pedido = p.id
        JOIN trusted.data d ON p.data = d.data
        JOIN trusted.produto pr ON pr.id = pi.id_produto
        JOIN trusted.marca m ON m.id = pr.id_marca
        LEFT JOIN trusted.meta mt
            ON mt.id_marca = m.id
            AND mt.ano = d.ano
            AND mt.mes = d.mes
        GROUP BY d.ano, d.mes, m.id, m.nome, mt.valor
        ORDER BY d.ano, d.mes, m.nome;
    """))

    with engine.begin() as conn:
        conn.execute(query)
    log("✅ Tabela refined.performance_mensal_marca criada com sucesso.")

# ==========================================================
# 📊 Tabela: KPIs consolidados de vendas
# ==========================================================
def carregar_kpis_vendas():
    log("Gerando tabela refined.kpis_vendas...")

    query = text(textwrap.dedent("""
        DROP TABLE IF EXISTS refined.kpis_vendas;
        CREATE TABLE refined.kpis_vendas AS
        SELECT
            DATE_TRUNC('month', p.data)::DATE AS mes_ano,
            COUNT(DISTINCT p.id) AS qtd_pedidos,
            SUM(p.vlr_total) AS receita_bruta,
            ROUND(AVG(p.vlr_total), 2) AS ticket_medio,
            COUNT(DISTINCT CASE WHEN p.status = 'CANCELADO' THEN p.id END) AS qtd_cancelamentos,
            ROUND(
                COUNT(DISTINCT CASE WHEN p.status = 'CANCELADO' THEN p.id END)::NUMERIC 
                / NULLIF(COUNT(DISTINCT p.id), 0) * 100, 
                2
            ) AS pct_cancelamento,
            COUNT(DISTINCT i.id_produto) AS qtd_produtos_distintos,
            SUM(i.qtd_produto) AS qtd_itens_vendidos
        FROM trusted.pedido p
        LEFT JOIN trusted.pedido_item i ON i.id_pedido = p.id AND i.flg_cancelado = 'N'
        GROUP BY DATE_TRUNC('month', p.data)
        ORDER BY mes_ano;
    """))

    with engine.begin() as conn:
        conn.execute(query)
    log("✅ Tabela refined.kpis_vendas criada com sucesso.")

# ==========================================================
# 🚫 Tabela: Análise de cancelamentos
# ==========================================================
def carregar_analise_cancelamentos():
    log("Gerando tabela refined.analise_cancelamentos...")

    query = text(textwrap.dedent("""
        DROP TABLE IF EXISTS refined.analise_cancelamentos;
        CREATE TABLE refined.analise_cancelamentos AS
        SELECT
            DATE_TRUNC('month', p.data)::DATE AS mes_ano,
            p.sgl_uf_entrega,
            m.nome AS marca,
            COUNT(DISTINCT p.id) AS qtd_pedidos_cancelados,
            SUM(p.vlr_total) AS vlr_total_cancelado,
            COUNT(DISTINCT i.id) AS qtd_itens_cancelados,
            ROUND(AVG(p.vlr_total), 2) AS ticket_medio_cancelado
        FROM trusted.pedido p
        JOIN trusted.pedido_item i ON i.id_pedido = p.id
        JOIN trusted.produto pr ON pr.id = i.id_produto
        JOIN trusted.marca m ON m.id = pr.id_marca
        WHERE p.status = 'CANCELADO' OR i.flg_cancelado = 'S'
        GROUP BY DATE_TRUNC('month', p.data), p.sgl_uf_entrega, m.nome
        ORDER BY mes_ano, qtd_pedidos_cancelados DESC;
    """))

    with engine.begin() as conn:
        conn.execute(query)
    log("✅ Tabela refined.analise_cancelamentos criada com sucesso.")

# ==========================================================
# 📈 Tabela: Variação de vendas por categoria
# ==========================================================
def carregar_vendas_categoria():
    log("Gerando tabela refined.vendas_categoria_variacao...")

    query = text(textwrap.dedent("""
        DROP TABLE IF EXISTS refined.vendas_categoria_variacao;
        CREATE TABLE refined.vendas_categoria_variacao AS
        WITH vendas_mensais AS (
            SELECT
                DATE_TRUNC('month', p.data)::DATE AS mes_ano,
                COALESCE(pr.categoria, 'Sem Categoria') AS categoria,
                SUM(i.qtd_produto) AS total_qtd,
                SUM(i.qtd_produto * i.vlr_unitario) AS total_valor
            FROM trusted.pedido p
            JOIN trusted.pedido_item i ON i.id_pedido = p.id
            JOIN trusted.produto pr ON pr.id = i.id_produto
            WHERE i.flg_cancelado = 'N'
            GROUP BY DATE_TRUNC('month', p.data), COALESCE(pr.categoria, 'Sem Categoria')
        ),
        vendas_com_lag AS (
            SELECT
                mes_ano,
                categoria,
                total_qtd,
                total_valor,
                LAG(total_qtd) OVER (PARTITION BY categoria ORDER BY mes_ano) AS qtd_mes_anterior,
                LAG(total_valor) OVER (PARTITION BY categoria ORDER BY mes_ano) AS valor_mes_anterior
            FROM vendas_mensais
        )
        SELECT
            mes_ano,
            categoria,
            total_qtd,
            total_valor,
            ROUND(
                CASE 
                    WHEN qtd_mes_anterior IS NOT NULL AND qtd_mes_anterior > 0 
                    THEN ((total_qtd - qtd_mes_anterior)::NUMERIC / qtd_mes_anterior * 100)
                    ELSE NULL
                END, 
                2
            ) AS pct_variacao_qtd,
            ROUND(
                CASE 
                    WHEN valor_mes_anterior IS NOT NULL AND valor_mes_anterior > 0 
                    THEN ((total_valor - valor_mes_anterior)::NUMERIC / valor_mes_anterior * 100)
                    ELSE NULL
                END, 
                2
            ) AS pct_variacao_valor
        FROM vendas_com_lag
        ORDER BY mes_ano DESC, total_valor DESC;
    """))

    with engine.begin() as conn:
        conn.execute(query)
    log("✅ Tabela refined.vendas_categoria_variacao criada com sucesso.")

# ==========================================================
# 🌍 Tabela: Análise por região (UF)
# ==========================================================
def carregar_analise_regional():
    log("Gerando tabela refined.analise_regional...")

    query = text(textwrap.dedent("""
        DROP TABLE IF EXISTS refined.analise_regional;
        CREATE TABLE refined.analise_regional AS
        SELECT
            DATE_TRUNC('month', p.data)::DATE AS mes_ano,
            p.sgl_uf_entrega,
            COUNT(DISTINCT p.id) AS qtd_pedidos,
            SUM(p.vlr_total) AS receita_total,
            ROUND(AVG(p.vlr_total), 2) AS ticket_medio,
            SUM(i.qtd_produto) AS qtd_itens,
            COUNT(DISTINCT i.id_produto) AS qtd_produtos_distintos,
            COUNT(DISTINCT pr.id_marca) AS qtd_marcas_distintas
        FROM trusted.pedido p
        LEFT JOIN trusted.pedido_item i ON i.id_pedido = p.id AND i.flg_cancelado = 'N'
        LEFT JOIN trusted.produto pr ON pr.id = i.id_produto
        WHERE p.sgl_uf_entrega IS NOT NULL
        GROUP BY DATE_TRUNC('month', p.data), p.sgl_uf_entrega
        ORDER BY mes_ano DESC, receita_total DESC;
    """))

    with engine.begin() as conn:
        conn.execute(query)
    log("✅ Tabela refined.analise_regional criada com sucesso.")

# ==========================================================
# 🚀 Execução principal do pipeline refined
# ==========================================================
if __name__ == "__main__":
    log("🚀 Iniciando transformações na camada refined...")
    inicializar_schemas()

    transformacoes = [
        carregar_best_sellers,
        carregar_performance_mensal,
        carregar_kpis_vendas,
        carregar_analise_cancelamentos,
        carregar_vendas_categoria,
        carregar_analise_regional,
    ]

    for func in transformacoes:
        try:
            func()
        except SQLAlchemyError as e:
            erro(f"Erro ao executar {func.__name__}: {e}")

    log("🏁 Pipeline refined finalizado com sucesso!")
