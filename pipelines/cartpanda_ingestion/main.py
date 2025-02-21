from library import save_bigquery, login_bigquery
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from datetime import datetime, timedelta
from google.cloud import bigquery
from airflow import DAG
from time import sleep
import pandas as pd
import requests

__author__ = 'Jhonathan Wesley Faria Dias Alves'

# Configurações da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "execution_timeout": timedelta(hours=10)
}

dag = DAG(
    'processamento_incremental_dados',
    default_args=default_args,
    description='Processamento incremental de dados de API e envio para o BigQuery',
    schedule='0 */2 * * *',
    catchup=False
)

def load_staging_data():
    """Carrega dados da API e atualiza tabela de staging no BigQuery"""
    url = "https://accounts.cartpanda.com/api/store-slug/orders"
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer api-token"
    }

    params = {'page': 1}
    data_frame = []

    # Recebe resposta da API em .json
    response = requests.get(url=url, headers=headers)
    data = response.json()

    # Cria lista de páginas da API
    last_page = data['result']['last_page'] + 1
    pages_lenght = list(range(last_page))
    pages_lenght.remove(0)

    # Faz a paginação para coletar dados
    for i in pages_lenght:
        params = {'page': i}
        response = requests.get(url=url, headers=headers, params=params)
        data = response.json()
        data_frame.extend(data['result']['data'])

    # Monta o DataFrame com as colunas necessárias
    df = pd.DataFrame(data_frame)
    df = df.loc[:, ['id', 'user_id', 'amount', 'discount', 'total', 'status', 'created_at', 'updated_at', 'payment_method']]

    # Adiciona coluna auxiliar para processamento futuro
    df['process_status'] = 'not_processed'
    
    # Salva DataFrame no BigQuery, atualizando tabela staging
    table_id = 'project.dataset.staging_table'
    save_bigquery(table_id, df)
    print(f'Tabela de staging {table_id} foi atualizada')

def insert_new_data():
    """Insere dados novos de forma incremental na tabela final"""
    insert_query = """
    INSERT INTO `project.dataset.final_table` (id, user_id, amount, discount, total, status, created_at, updated_at, payment_method, process_status)
    SELECT *
    FROM `project.dataset.staging_table` stg
    WHERE stg.id NOT IN (SELECT final.id FROM `project.dataset.final_table` final)"""
    
    try:
        login_bigquery(query=insert_query)
    except:
        print('Inserção de dados concluída')

def process_incremental_data():
    """Processa um lote de dados incrementalmente"""
    query = """SELECT id FROM `project.dataset.final_table` WHERE process_status = 'not_processed'"""
    df = login_bigquery(query)

    pending_ids = df['id'].to_list()
    print(f"Faltam processar: {len(pending_ids)} registros")

    processed_data = []
    
    for record_id in pending_ids:
        try_count = 0
        max_try_limit = 3
        response_status = 200
        
        if response_status == 200:
            while try_count < max_try_limit:
                url = f"https://accounts.cartpanda.com/api/store-slug/orders/{record_id}"
                headers = {
                    "Accept": "application/json",
                    "Authorization": "Bearer api-token"
                }
                response = requests.get(url, headers=headers)

                if response.status_code != 200:
                    response_status = response.status_code
                    break
                elif response.status_code == 200:
                    record_data = response.json().get('details')

                    if record_data:
                        processed_data.append(record_data)
                        print(f"Registro {record_id} processado com sucesso!")

                    else:
                        print(f"Nenhum dado encontrado para o registro {record_id}.")
                    sleep(1)
                    break

                else:
                    try_count += 1
                    print(f"Tentativa {try_count} para o registro {record_id} falhou. Status: {response.status_code}")
                    sleep(1)
            
                if try_count == max_try_limit:
                    print(f"Falha ao processar o registro {record_id} após {max_try_limit} tentativas.")
        
        else:
            break

    df_processed = pd.DataFrame(processed_data)
    df_processed = df_processed.rename(columns={'id': 'processed_id', 'total': 'processed_total'})

    table_id = 'project.dataset.processed_table'
    save_bigquery(table_id, df_processed)
    print(f"Dados processados e salvos na tabela {table_id}")

def update_final_table():
    """Atualiza dados processados na tabela final"""
    update_query = """
        UPDATE `project.dataset.final_table` AS final
        SET
        final.processed_total = proc.processed_total,
        final.process_status = 'processed'
        FROM `project.dataset.processed_table` AS proc
        WHERE final.id = proc.processed_id
        AND final.process_status = 'not_processed'
    """

    try:
        login_bigquery(update_query)
        print(f"Update realizado com sucesso na tabela final")
    except Exception as e:
        print(f"Erro na atualização: {e}")

# Define as tasks
load_staging_task = PythonOperator(
    task_id='carregar_dados_staging',
    python_callable=load_staging_data,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='inserir_novos_dados',
    python_callable=insert_new_data,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='processar_dados_incrementais',
    python_callable=process_incremental_data,
    dag=dag,
)

update_final_task = PythonOperator(
    task_id='atualizar_tabela_final',
    python_callable=update_final_table,
    dag=dag,
)

# Define dependências
load_staging_task >> insert_data_task >> process_data_task >> update_final_task
