import os
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Tuple

# =====================================================
# 1️⃣ Carregar variáveis de ambiente
# =====================================================
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# =====================================================
# 2️⃣ Criar engine de conexão
# =====================================================
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# =====================================================
# 3️⃣ Variáveis globais para controle
# =====================================================
validation_results = []
total_errors = 0

# =====================================================
# 🛠️ Funções auxiliares
# =====================================================

def log_success(message: str):
    """Log de sucesso"""
    print(f"✅ {message}")
    validation_results.append({"status": "SUCCESS", "message": message, "timestamp": datetime.now()})

def log_warning(message: str):
    """Log de aviso"""
    print(f"⚠️  {message}")
    validation_results.append({"status": "WARNING", "message": message, "timestamp": datetime.now()})

def log_error(message: str):
    """Log de erro"""
    global total_errors
    total_errors += 1
    print(f"❌ {message}")
    validation_results.append({"status": "ERROR", "message": message, "timestamp": datetime.now()})

def execute_query(query: str) -> List[Tuple]:
    """Executa query e retorna resultado"""
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.fetchall()

# =====================================================
# 🧪 VALIDAÇÕES DA CAMADA REFINED
# =====================================================

def validate_table_existence():
    """1️⃣ Valida se as tabelas refined existem"""
    print("\n" + "="*60)
    print("📊 VALIDAÇÃO 1: Existência das Tabelas Refined")
    print("="*60)
    
    tables = [
        'mais_vendidos_mensal_estado',
        'performance_mensal_marca'
    ]
    
    for table in tables:
        query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'refined' 
                AND table_name = '{table}'
            )
        """
        result = execute_query(query)
        
        if result[0][0]:
            log_success(f"Tabela refined.{table} existe")
        else:
            log_error(f"Tabela refined.{table} NÃO EXISTE!")

def validate_refined_counts():
    """2️⃣ Valida se as tabelas refined possuem dados"""
    print("\n" + "="*60)
    print("📈 VALIDAÇÃO 2: Contagem de Registros Refined")
    print("="*60)
    
    tables = ['mais_vendidos_mensal_estado', 'performance_mensal_marca']
    
    for table in tables:
        try:
            query = f"SELECT COUNT(*) FROM refined.{table}"
            result = execute_query(query)
            count = result[0][0]
            
            if count == 0:
                log_warning(f"Tabela refined.{table} está VAZIA!")
            else:
                log_success(f"Tabela refined.{table}: {count:,} registros")
        except Exception as e:
            log_error(f"Erro ao consultar refined.{table}: {str(e)}")

def validate_mais_vendidos_ranking():
    """3️⃣ Valida rankings dos mais vendidos"""
    print("\n" + "="*60)
    print("🏆 VALIDAÇÃO 3: Rankings dos Mais Vendidos")
    print("="*60)
    
    try:
        # Verificar se os rankings começam em 1
        query = """
            SELECT mes_ano, sgl_uf_entrega, MIN(posicao) as min_pos
            FROM refined.mais_vendidos_mensal_estado
            GROUP BY mes_ano, sgl_uf_entrega
            HAVING MIN(posicao) != 1
        """
        result = execute_query(query)
        
        if len(result) > 0:
            log_error(f"Encontrados {len(result)} períodos/estados com ranking não iniciando em 1")
        else:
            log_success("Todos os rankings começam corretamente em 1")
        
        # Verificar se não há gaps nos rankings
        query = """
            WITH ranking_gaps AS (
                SELECT 
                    mes_ano, 
                    sgl_uf_entrega,
                    posicao,
                    LAG(posicao) OVER (PARTITION BY mes_ano, sgl_uf_entrega ORDER BY posicao) as prev_pos
                FROM refined.mais_vendidos_mensal_estado
            )
            SELECT COUNT(*) FROM ranking_gaps
            WHERE prev_pos IS NOT NULL AND posicao != prev_pos + 1
        """
        result = execute_query(query)
        
        if result[0][0] > 0:
            log_warning(f"Encontrados {result[0][0]} gaps nos rankings")
        else:
            log_success("Não há gaps nos rankings")
            
    except Exception as e:
        log_error(f"Erro ao validar rankings: {str(e)}")

def validate_performance_calculations():
    """4️⃣ Valida cálculos de performance"""
    print("\n" + "="*60)
    print("💯 VALIDAÇÃO 4: Cálculos de Performance")
    print("="*60)
    
    try:
        # Verificar se percentual de atingimento está calculado corretamente
        query = """
            SELECT COUNT(*) FROM refined.performance_mensal_marca
            WHERE vlr_meta > 0 
            AND ABS(perc_atingimento_meta - (vlr_total_vendido / vlr_meta * 100)) > 0.01
        """
        result = execute_query(query)
        
        if result[0][0] > 0:
            log_error(f"Encontrados {result[0][0]} registros com percentual incorreto!")
        else:
            log_success("Todos os percentuais de atingimento estão corretos")
        
        # Verificar valores negativos
        query = """
            SELECT COUNT(*) FROM refined.performance_mensal_marca
            WHERE vlr_total_vendido < 0 OR vlr_meta < 0
        """
        result = execute_query(query)
        
        if result[0][0] > 0:
            log_error(f"Encontrados {result[0][0]} registros com valores negativos!")
        else:
            log_success("Não há valores negativos em performance")
            
    except Exception as e:
        log_error(f"Erro ao validar cálculos: {str(e)}")

def validate_aggregation_consistency():
    """5️⃣ Valida consistência das agregações"""
    print("\n" + "="*60)
    print("🔢 VALIDAÇÃO 5: Consistência das Agregações")
    print("="*60)
    
    try:
        # Verificar se total de quantidades em mais_vendidos bate com trusted
        query = """
            SELECT 
                SUM(mv.total_qtd) as refined_total,
                (SELECT SUM(qtd_produto) 
                 FROM trusted.pedido_item i
                 JOIN trusted.pedido p ON i.id_pedido = p.id
                 WHERE i.flg_cancelado = 'N') as trusted_total
            FROM refined.mais_vendidos_mensal_estado mv
        """
        result = execute_query(query)
        
        if result[0][0] and result[0][1]:
            refined_total = float(result[0][0])
            trusted_total = float(result[0][1])
            diff_pct = abs((refined_total - trusted_total) / trusted_total * 100) if trusted_total > 0 else 0
            
            if diff_pct > 1:  # Tolerância de 1%
                log_warning(f"Divergência de {diff_pct:.2f}% entre refined e trusted nas quantidades")
            else:
                log_success(f"Agregações de quantidade consistentes (diff: {diff_pct:.2f}%)")
        
        # Verificar se valores em performance batem com trusted
        query = """
            SELECT 
                SUM(pm.vlr_total_vendido) as refined_total,
                (SELECT SUM(vlr_total) FROM trusted.pedido) as trusted_total
            FROM refined.performance_mensal_marca pm
        """
        result = execute_query(query)
        
        if result[0][0] and result[0][1]:
            refined_total = float(result[0][0])
            trusted_total = float(result[0][1])
            diff_pct = abs((refined_total - trusted_total) / trusted_total * 100) if trusted_total > 0 else 0
            
            if diff_pct > 1:  # Tolerância de 1%
                log_warning(f"Divergência de {diff_pct:.2f}% entre refined e trusted nos valores")
            else:
                log_success(f"Agregações de valores consistentes (diff: {diff_pct:.2f}%)")
                
    except Exception as e:
        log_error(f"Erro ao validar agregações: {str(e)}")

def validate_date_ranges():
    """6️⃣ Valida ranges de datas"""
    print("\n" + "="*60)
    print("📅 VALIDAÇÃO 6: Ranges de Datas Refined")
    print("="*60)
    
    try:
        # Verificar se há dados muito antigos ou futuros
        query = """
            SELECT 
                MIN(mes_ano) as data_mais_antiga,
                MAX(mes_ano) as data_mais_recente
            FROM refined.mais_vendidos_mensal_estado
        """
        result = execute_query(query)
        
        if result and result[0][0]:
            min_date = result[0][0]
            max_date = result[0][1]
            log_success(f"Range de datas: {min_date} até {max_date}")
            
            # Verificar se está no futuro
            if max_date > datetime.now().date():
                log_warning(f"Existem dados com data futura: {max_date}")
        else:
            log_warning("Não foi possível determinar range de datas")
            
    except Exception as e:
        log_error(f"Erro ao validar datas: {str(e)}")

def validate_data_quality_metrics():
    """7️⃣ Valida métricas de qualidade dos dados"""
    print("\n" + "="*60)
    print("🎯 VALIDAÇÃO 7: Métricas de Qualidade")
    print("="*60)
    
    try:
        # Verificar completude dos dados (% de nulos)
        query = """
            SELECT 
                COUNT(*) as total,
                COUNT(sgl_uf_entrega) as uf_count,
                COUNT(nome_produto) as nome_count
            FROM refined.mais_vendidos_mensal_estado
        """
        result = execute_query(query)
        
        if result:
            total = result[0][0]
            uf_count = result[0][1]
            nome_count = result[0][2]
            
            uf_completeness = (uf_count / total * 100) if total > 0 else 0
            nome_completeness = (nome_count / total * 100) if total > 0 else 0
            
            log_success(f"Completude UF: {uf_completeness:.1f}%")
            log_success(f"Completude Nome Produto: {nome_completeness:.1f}%")
            
            if uf_completeness < 95 or nome_completeness < 95:
                log_warning("Completude abaixo de 95% detectada")
        
        # Verificar marcas sem vendas
        query = """
            SELECT COUNT(*) FROM trusted.marca m
            WHERE NOT EXISTS (
                SELECT 1 FROM refined.performance_mensal_marca pm
                WHERE pm.id = m.id
            )
        """
        result = execute_query(query)
        
        if result[0][0] > 0:
            log_warning(f"{result[0][0]} marcas sem dados de performance")
        else:
            log_success("Todas as marcas possuem dados de performance")
            
    except Exception as e:
        log_error(f"Erro ao validar métricas de qualidade: {str(e)}")

# =====================================================
# 🏁 Execução principal
# =====================================================

def main():
    print("\n" + "="*60)
    print("🔬 VALIDAÇÕES DA CAMADA REFINED")
    print("="*60)
    print(f"Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Executar todas as validações
    validate_table_existence()
    validate_refined_counts()
    validate_mais_vendidos_ranking()
    validate_performance_calculations()
    validate_aggregation_consistency()
    validate_date_ranges()
    validate_data_quality_metrics()
    
    # Resumo final
    print("\n" + "="*60)
    print("📋 RESUMO DAS VALIDAÇÕES")
    print("="*60)
    
    success_count = sum(1 for r in validation_results if r["status"] == "SUCCESS")
    warning_count = sum(1 for r in validation_results if r["status"] == "WARNING")
    error_count = sum(1 for r in validation_results if r["status"] == "ERROR")
    
    print(f"✅ Sucessos: {success_count}")
    print(f"⚠️  Avisos:   {warning_count}")
    print(f"❌ Erros:    {error_count}")
    print("="*60)
    
    if error_count == 0:
        print("\n🎉 TODAS AS VALIDAÇÕES CRÍTICAS PASSARAM!")
        print("✅ Camada REFINED está íntegra e pronta para consumo analítico.")
        return 0
    else:
        print(f"\n⚠️  ATENÇÃO: {error_count} erro(s) crítico(s) encontrado(s)!")
        print("❌ Revise a camada REFINED antes de disponibilizar para consumo.")
        return 1

if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)

