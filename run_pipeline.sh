#!/bin/bash
# ==========================================================
# 🚀 PIPELINE DE DADOS - GRUPO SBF CASE (Camila Macedo)
# ==========================================================

# Caminho base do projeto
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPT_DIR="$BASE_DIR/script/Ingestão"

# Ativar ambiente virtual
echo "🔧 Ativando ambiente virtual..."
source "$BASE_DIR/.venv_case/Scripts/activate"

# Carregar variáveis de ambiente
if [ -f "$BASE_DIR/.env" ]; then
  echo "🌱 Carregando variáveis do .env..."
  export $(grep -v '^#' "$BASE_DIR/.env" | xargs)
else
  echo "⚠️  Arquivo .env não encontrado! Configure as variáveis de conexão."
  exit 1
fi

# ==========================================================
# 1️⃣ INGESTÃO - camada TRUSTED
# ==========================================================
echo -e "\n🚚 Iniciando ingestão (trusted)..."
python "$SCRIPT_DIR/load_data_rds.py"
if [ $? -ne 0 ]; then
  echo "❌ Erro na ingestão (trusted). Abortando pipeline."
  exit 1
fi
echo "✅ Ingestão concluída com sucesso!"

# ==========================================================
# 2️⃣ TRANSFORMAÇÃO - camada REFINED
# ==========================================================
echo -e "\n🔄 Executando transformações (refined)..."
python "$SCRIPT_DIR/transform_refined.py"
if [ $? -ne 0 ]; then
  echo "❌ Erro na transformação (refined). Abortando pipeline."
  exit 1
fi
echo "✅ Transformações concluídas com sucesso!"

# ==========================================================
# 3️⃣ (OPCIONAL) ORQUESTRAÇÃO VIA AIRFLOW
# ==========================================================
if [ "$1" == "--airflow" ]; then
  echo -e "\n🌀 Iniciando orquestração Airflow local..."
  airflow db init
  airflow webserver -p 8080 &
  airflow scheduler &
  echo "🌐 Acesse o Airflow em: http://localhost:8080"
fi

# ==========================================================
# 4️⃣ FINALIZAÇÃO
# ==========================================================
echo -e "\n🏁 Pipeline executado com sucesso!"
echo "----------------------------------------------------------"
echo "🔹 Trusted e refined atualizados no banco RDS PostgreSQL."
echo "🔹 Logs registrados em trusted.log_ingestao."
echo "----------------------------------------------------------"
