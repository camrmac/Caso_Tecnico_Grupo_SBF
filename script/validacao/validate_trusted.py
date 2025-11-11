import os
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, List, Tuple

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
# 🧪 VALIDAÇÕES DA CAMADA TRUSTED
# =====================================================

def validate_table_counts():
    """1️⃣ Valida se as tabelas possuem dados"""
    print("\n" + "="*60)
    print("📊 VALIDAÇÃO 1: Contagem de Registros")
    print("="*60)
    
    tables = ['marca', 'produto', 'data', 'pedido', 'pedido_item', 'meta']
    
    for table in tables:
        query = f"SELECT COUNT(*) FROM trusted.{table}"
        result = execute_query(query)
        count = result[0][0]
        
        if count == 0:
            log_error(f"Tabela trusted.{table} está VAZIA!")
        elif count < 10:
            log_warning(f"Tabela trusted.{table} tem apenas {count} registros")
        else:
            log_success(f"Tabela trusted.{table}: {count:,} registros")

def validate_foreign_keys():
    """2️⃣ Valida integridade referencial (FKs)"""
    print("\n" + "="*60)
    print("🔗 VALIDAÇÃO 2: Integridade Referencial")
    print("="*60)
    
    fk_checks = {
        "produto.id_marca → marca.id": """
            SELECT COUNT(*) FROM trusted.produto p
            LEFT JOIN trusted.marca m ON p.id_marca = m.id
            WHERE m.id IS NULL
        """,
        "pedido.data → data.data": """
            SELECT COUNT(*) FROM trusted.pedido p
            LEFT JOIN trusted.data d ON p.data = d.data
            WHERE d.data IS NULL
        """,
        "pedido_item.id_pedido → pedido.id": """
            SELECT COUNT(*) FROM trusted.pedido_item i
            LEFT JOIN trusted.pedido p ON i.id_pedido = p.id
            WHERE p.id IS NULL
        """,
        "pedido_item.id_produto → produto.id": """
            SELECT COUNT(*) FROM trusted.pedido_item i
            LEFT JOIN trusted.produto pr ON i.id_produto = pr.id
            WHERE pr.id IS NULL
        """,
        "meta.id_marca → marca.id": """
            SELECT COUNT(*) FROM trusted.meta m
            LEFT JOIN trusted.marca ma ON m.id_marca = ma.id
            WHERE ma.id IS NULL
        """
    }
    
    for desc, query in fk_checks.items():
        result = execute_query(query)
        orphans = result[0][0]
        
        if orphans > 0:
            log_error(f"FK violada: {desc} - {orphans} registros órfãos!")
        else:
            log_success(f"FK válida: {desc}")

def validate_null_constraints():
    """3️⃣ Valida campos obrigatórios (NOT NULL)"""
    print("\n" + "="*60)
    print("🚫 VALIDAÇÃO 3: Campos Nulos em Colunas Obrigatórias")
    print("="*60)
    
    null_checks = {
        "marca.nome": "SELECT COUNT(*) FROM trusted.marca WHERE nome IS NULL",
        "produto.nome": "SELECT COUNT(*) FROM trusted.produto WHERE nome IS NULL",
        "produto.id_marca": "SELECT COUNT(*) FROM trusted.produto WHERE id_marca IS NULL",
        "pedido.data": "SELECT COUNT(*) FROM trusted.pedido WHERE data IS NULL",
        "pedido.vlr_total": "SELECT COUNT(*) FROM trusted.pedido WHERE vlr_total IS NULL",
        "pedido_item.id_pedido": "SELECT COUNT(*) FROM trusted.pedido_item WHERE id_pedido IS NULL",
        "pedido_item.id_produto": "SELECT COUNT(*) FROM trusted.pedido_item WHERE id_produto IS NULL",
        "pedido_item.qtd_produto": "SELECT COUNT(*) FROM trusted.pedido_item WHERE qtd_produto IS NULL",
        "data.ano": "SELECT COUNT(*) FROM trusted.data WHERE ano IS NULL",
        "data.mes": "SELECT COUNT(*) FROM trusted.data WHERE mes IS NULL",
    }
    
    for field, query in null_checks.items():
        result = execute_query(query)
        null_count = result[0][0]
        
        if null_count > 0:
            log_error(f"Campo obrigatório {field} tem {null_count} valores NULL!")
        else:
            log_success(f"Campo {field} não possui valores NULL")

def validate_data_ranges():
    """4️⃣ Valida ranges de valores"""
    print("\n" + "="*60)
    print("📏 VALIDAÇÃO 4: Ranges de Valores")
    print("="*60)
    
    # Validar valores negativos
    query = "SELECT COUNT(*) FROM trusted.pedido WHERE vlr_total < 0"
    result = execute_query(query)
    if result[0][0] > 0:
        log_error(f"Existem {result[0][0]} pedidos com valor negativo!")
    else:
        log_success("Todos os pedidos têm valores positivos")
    
    # Validar quantidades
    query = "SELECT COUNT(*) FROM trusted.pedido_item WHERE qtd_produto <= 0"
    result = execute_query(query)
    if result[0][0] > 0:
        log_error(f"Existem {result[0][0]} itens com quantidade inválida!")
    else:
        log_success("Todas as quantidades são válidas")
    
    # Validar UFs
    query = """
        SELECT COUNT(*) FROM trusted.pedido 
        WHERE sgl_uf_entrega IS NOT NULL 
        AND sgl_uf_entrega !~ '^[A-Z]{2}$'
    """
    result = execute_query(query)
    if result[0][0] > 0:
        log_error(f"Existem {result[0][0]} UFs com formato inválido!")
    else:
        log_success("Todas as UFs estão no formato correto")
    
    # Validar mês
    query = "SELECT COUNT(*) FROM trusted.data WHERE mes < 1 OR mes > 12"
    result = execute_query(query)
    if result[0][0] > 0:
        log_error(f"Existem {result[0][0]} datas com mês inválido!")
    else:
        log_success("Todos os meses são válidos (1-12)")

def validate_duplicates():
    """5️⃣ Valida duplicatas em PKs"""
    print("\n" + "="*60)
    print("🔍 VALIDAÇÃO 5: Duplicatas em Chaves Primárias")
    print("="*60)
    
    duplicate_checks = {
        "marca.id": """
            SELECT id, COUNT(*) as cnt FROM trusted.marca 
            GROUP BY id HAVING COUNT(*) > 1
        """,
        "produto.id": """
            SELECT id, COUNT(*) as cnt FROM trusted.produto 
            GROUP BY id HAVING COUNT(*) > 1
        """,
        "pedido.id": """
            SELECT id, COUNT(*) as cnt FROM trusted.pedido 
            GROUP BY id HAVING COUNT(*) > 1
        """,
        "pedido_item.id": """
            SELECT id, COUNT(*) as cnt FROM trusted.pedido_item 
            GROUP BY id HAVING COUNT(*) > 1
        """,
        "data.data": """
            SELECT data, COUNT(*) as cnt FROM trusted.data 
            GROUP BY data HAVING COUNT(*) > 1
        """
    }
    
    for field, query in duplicate_checks.items():
        result = execute_query(query)
        if len(result) > 0:
            log_error(f"PK {field} tem {len(result)} valores duplicados!")
        else:
            log_success(f"PK {field} não possui duplicatas")

def validate_business_rules():
    """6️⃣ Valida regras de negócio"""
    print("\n" + "="*60)
    print("💼 VALIDAÇÃO 6: Regras de Negócio")
    print("="*60)
    
    # Validar se itens cancelados não contam no total do pedido
    query = """
        SELECT p.id, p.vlr_total,
               SUM(CASE WHEN i.flg_cancelado = 'N' 
                   THEN i.qtd_produto * i.vlr_unitario 
                   ELSE 0 END) as calc_total
        FROM trusted.pedido p
        JOIN trusted.pedido_item i ON p.id = i.id_pedido
        GROUP BY p.id, p.vlr_total
        HAVING ABS(p.vlr_total - SUM(CASE WHEN i.flg_cancelado = 'N' 
                                      THEN i.qtd_produto * i.vlr_unitario 
                                      ELSE 0 END)) > 0.01
        LIMIT 10
    """
    result = execute_query(query)
    if len(result) > 0:
        log_warning(f"Encontrados {len(result)} pedidos com divergência de valores")
    else:
        log_success("Valores dos pedidos estão consistentes com seus itens")
    
    # Validar se existem pedidos sem itens
    query = """
        SELECT COUNT(*) FROM trusted.pedido p
        LEFT JOIN trusted.pedido_item i ON p.id = i.id_pedido
        WHERE i.id IS NULL
    """
    result = execute_query(query)
    if result[0][0] > 0:
        log_warning(f"Existem {result[0][0]} pedidos sem itens")
    else:
        log_success("Todos os pedidos possuem itens")

def validate_date_consistency():
    """7️⃣ Valida consistência de datas"""
    print("\n" + "="*60)
    print("📅 VALIDAÇÃO 7: Consistência de Datas")
    print("="*60)
    
    # Validar se ano, mes, dia batem com a data
    query = """
        SELECT COUNT(*) FROM trusted.data
        WHERE EXTRACT(YEAR FROM data) != ano
           OR EXTRACT(MONTH FROM data) != mes
           OR EXTRACT(DAY FROM data) != dia
    """
    result = execute_query(query)
    if result[0][0] > 0:
        log_error(f"Existem {result[0][0]} datas inconsistentes!")
    else:
        log_success("Todas as datas estão consistentes (ano, mês, dia)")

# =====================================================
# 🏁 Execução principal
# =====================================================

def main():
    print("\n" + "="*60)
    print("🔬 VALIDAÇÕES DA CAMADA TRUSTED")
    print("="*60)
    print(f"Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Executar todas as validações
    validate_table_counts()
    validate_foreign_keys()
    validate_null_constraints()
    validate_data_ranges()
    validate_duplicates()
    validate_business_rules()
    validate_date_consistency()
    
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
        print("✅ Camada TRUSTED está íntegra e consistente.")
        return 0
    else:
        print(f"\n⚠️  ATENÇÃO: {error_count} erro(s) crítico(s) encontrado(s)!")
        print("❌ Corrija os problemas antes de prosseguir para a camada REFINED.")
        return 1

if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)

