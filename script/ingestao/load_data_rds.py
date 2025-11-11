import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

# =====================================================
# 1️⃣ Carregar variáveis de ambiente (.env)
# =====================================================
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# 🌐 Idioma padrão para formatação de datas (pode ser 'pt_BR' ou 'en_US')
DATE_LANG = os.getenv("DATE_LANG", "pt_BR")

# =====================================================
# 2️⃣ Criar engine de conexão com o PostgreSQL (RDS)
# =====================================================
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# =====================================================
# 3️⃣ Função de carga CSV → PostgreSQL
# =====================================================
def load_csv_to_postgres(csv_path, table_name, schema='trusted', chunksize=5000):
    print(f"\nIniciando carga: {table_name}")

    df_iter = pd.read_csv(csv_path, chunksize=chunksize)
    total_rows = 0

    for chunk in df_iter:
        # =====================================================
        # 🧩 Normalização automática de colunas por tabela
        # =====================================================
        if table_name == 'pedido':
            if 'sgl_uf_entrega' in chunk.columns and 'uf_entrega' not in chunk.columns:
                chunk.rename(columns={'sgl_uf_entrega': 'sgl_uf_entrega'}, inplace=True)

        elif table_name == 'meta':
            if 'vlr_meta' in chunk.columns:
                chunk.rename(columns={'vlr_meta': 'valor'}, inplace=True)

        elif table_name == 'produto':
            if 'idMarca' in chunk.columns:
                chunk.rename(columns={'idMarca': 'id_marca'}, inplace=True)

        # =====================================================
        # 🚀 Envio do chunk para o banco
        # =====================================================
        chunk.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False)
        total_rows += len(chunk)

    # =====================================================
    # 🧾 Registro de log da ingestão
    # =====================================================
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO trusted.log_ingestao (tabela, data_ingestao, usuario, qtd_registros)
            VALUES (:tabela, :data_ingestao, :usuario, :qtd)
        """), {
            'tabela': f'{schema}.{table_name}',
            'data_ingestao': datetime.now(),
            'usuario': DB_USER,
            'qtd': total_rows
        })

    print(f"✅ {table_name} carregada com sucesso: {total_rows} linhas.")

    # =====================================================
    # 🧠 Enriquecimento automático da tabela de datas
    # =====================================================
    if table_name == 'data':
        print(f"🪄 Enriquecendo tabela de datas (campo descricao) com idioma '{DATE_LANG}'...")
        with engine.begin() as conn:
            conn.execute(text(f"""
                SET lc_time = '{DATE_LANG}.UTF-8';
                UPDATE trusted.data
                SET descricao = TO_CHAR(data, 'TMDay, DD TMMonth YYYY')
                WHERE descricao IS NULL;
            """))
        print("✅ Campo descricao preenchido automaticamente com sucesso.")

# =====================================================
# 4️⃣ Validação pós-carga
# =====================================================
def validate_data():
    print("\n🔍 Iniciando validações pós-carga...")
    queries = {
        'Total pedidos': 'SELECT COUNT(*) FROM trusted.pedido',
        'Total itens': 'SELECT COUNT(*) FROM trusted.pedido_item',
        'Pedidos com cliente nulo': 'SELECT COUNT(*) FROM trusted.pedido WHERE cliente_id_hash IS NULL',
        'Itens sem pedido correspondente': '''
            SELECT COUNT(*) FROM trusted.pedido_item i
            LEFT JOIN trusted.pedido p ON i.id_pedido = p.id
            WHERE p.id IS NULL
        ''',
        'Produtos sem marca correspondente': '''
            SELECT COUNT(*) FROM trusted.produto pr
            LEFT JOIN trusted.marca m ON pr.id_marca = m.id
            WHERE m.id IS NULL
        '''
    }

    with engine.connect() as conn:
        for desc, sql in queries.items():
            result = conn.execute(text(sql)).fetchone()
            print(f"{desc}: {result[0]}")

    print("\n✅ Validação concluída.")

# =====================================================
# 5️⃣ Execução principal
# =====================================================
if __name__ == '__main__':
    BASE_PATH = './data/trusted'

    arquivos = {
        'marca': f'{BASE_PATH}/marca.csv',
        'produto': f'{BASE_PATH}/produto.csv',
        'data': f'{BASE_PATH}/data.csv',
        'cliente_pii': f'{BASE_PATH}/cliente_pii.csv',
        'cliente_pseudo': f'{BASE_PATH}/cliente_pseudo.csv',
        'pedido': f'{BASE_PATH}/pedido.csv',
        'pedido_item': f'{BASE_PATH}/pedido_item.csv',
        'meta': f'{BASE_PATH}/meta.csv'
    }

    for tabela, caminho in arquivos.items():
        if os.path.exists(caminho):
            load_csv_to_postgres(caminho, tabela)
        else:
            print(f"⚠️  Arquivo não encontrado: {caminho}")

    validate_data()

    print("\n🚀 Ingestão finalizada com sucesso!")
