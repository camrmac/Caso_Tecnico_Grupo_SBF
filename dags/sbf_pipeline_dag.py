from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

# =====================================================
# Configurações básicas da DAG
# =====================================================
default_args = {
    'owner': 'camila.macedo',
    'depends_on_past': False,
    'email': ['camila.macedo@sbf.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# =====================================================
# Definição da DAG
# =====================================================
with DAG(
    dag_id='sbf_pipeline_dag',
    description='Pipeline ETL do case técnico Grupo SBF (trusted → refined)',
    schedule_interval='@daily',  # executa 1x por dia
    start_date=datetime(2024, 11, 1),
    catchup=False,
    default_args=default_args,
    tags=['SBF', 'analytics-engineer', 'case'],
) as dag:

    # Caminho base do projeto
    BASE_DIR = os.path.expanduser('~/empresa/SBF/sbf_case_ae_db/script/Ingestão')

    # =====================================================
    # 1️⃣ Task - Ingestão da camada TRUSTED
    # =====================================================
    trusted_ingest = BashOperator(
        task_id='trusted_ingest',
        bash_command=f'python {BASE_DIR}/load_data_rds.py',
        dag=dag
    )

    # =====================================================
    # 2️⃣ Task - Transformação da camada REFINED
    # =====================================================
    refined_transform = BashOperator(
        task_id='refined_transform',
        bash_command=f'python {BASE_DIR}/transform_refined.py',
        dag=dag
    )

    # =====================================================
    # Dependência: trusted → refined
    # =====================================================
    trusted_ingest >> refined_transform
