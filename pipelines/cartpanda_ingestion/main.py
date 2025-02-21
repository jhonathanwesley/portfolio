from library import save_bigquery, login_bigquery
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from datetime import datetime, timedelta
from google.cloud import bigquery
from airflow import DAG
from time import sleep
import pandas as pd
import pandas_gbq
import requests
import time

__author__ = 'Jhonathan Wesley'

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
    'cartpanda_orders_shipping',
    default_args=default_args,
    description='Processamento incremental de pedidos e fretes do Cartpanda',
    # schedule_interval = '0 3 * * *'
    # 0 */2 * * *
    schedule='0 */2 * * *',
    catchup=False
)

def load_staging_cartpanda():
    """Mantém a tabela  table_name  atualizada no bigquery com os novos pedidos, para ser usada como staging area dos dados do Cartpanda, ETL dos dados obtidos via API do cartpanda, faz REPLACE por default"""
    url = "https://accounts.cartpanda.com/api/store-slug/orders"

    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer store-token"
    }

    params = {'page': 1}
    data_frame = []

    # Recebe resposta da api em .json
    response = requests.get(url=url, headers=headers)
    orders = response.json()

    # Cria lista das páginas presentes na API removendo o 0
    last_page = orders['orders']['last_page']
    last_page = last_page + 1
    pages_lenght = list(range(last_page))
    pages_lenght.remove(0)

    # Efetua a paginação
    for i in pages_lenght:
        params = {'page': i}
        response = requests.get(url=url, headers=headers, params=params)
        orders = response.json()
        data_frame.extend(orders['orders']['data'])

    # Fetch DataFrame filtering important columns
    df = pd.DataFrame(data_frame)
    df = df.loc[:, ['id', 'customer_id', 'subtotal_price', 'subtotal_price_set', 'total_discounts', 'total_discounts_set', 'total_line_items_price', 'total_line_items_price_set', 'total_price', 'total_price_set', 'total_tax', 'total_tax_set', 'total_tip_received', 'email', 'discount_codes', 'note', 'phone', 'processing_method', 'source_name', 'card_token', 'processed_at', 'cancel_reason', 'cancelled_at', 'closed_at', 'boleto_email_sent', 'created_at', 'updated_at', 'payment_gateway', 'payment_type', 'fulfillment_status', 'converted_total_price', 'payment_currency']]

    # Extrai código SP do shopify
    df['cod_pedido_shopify'] = df['note'].str.extract(r'Shopify Order Name:\s*(\S+)')
    df['shipping_processed_status'] = 'not_processed'
    
    # Salva DataFrame no BigQuery; atualizando tabela staging table_name
    table_id = 'google_cloud_project.schema_name.table_name'
    save_bigquery(table_id, df)
    print(f'A tabela {table_id} de staging area foi atualizada')

def insert_new_data_final_table():
    """Insere novos dados presentes na  table_name  que não estão na  cartpanda_orders  de forma incremental, rodando um INSERT no Bigquery"""

    insert_new = """
    INSERT INTO `google_cloud_project.schema_name.table_name` (id, customer_id, subtotal_price, subtotal_price_set, total_discounts, total_discounts_set, total_line_items_price, total_line_items_price_set, total_price, total_price_set, total_tax, total_tax_set, total_tip_received, email, discount_codes, note, phone, processing_method, source_name, card_token, processed_at, cancel_reason, cancelled_at, closed_at, boleto_email_sent, created_at, updated_at, payment_gateway, payment_type, fulfillment_status, converted_total_price, payment_currency, cod_pedido_shopify, shipping_processed_status)
    SELECT *
    FROM `google_cloud_project.schema_name.table_name` shp
    WHERE shp.id NOT IN (SELECT ord.id FROM `google_cloud_project.schema_name.orders_table` ord)"""
    
    try:
        login_bigquery(query=insert_new)
    except:
        print('Insert concluído')

def process_shipping_batch():
    """Processa um lote de fretes de forma incremental"""

    # Consulta os dados de linhas na qual o frete não foi processado
    query = """SELECT co.id FROM `google_cloud_project.schema_name.orders_table` co WHERE co.shipping_processed_status = 'not_processed'"""
    df = login_bigquery(query)

    # Lista para receber os dados de fretes processados com sucesso e ser convertida em dataframe
    shipping_df = []

    # Listar id pendentes de processamento obtidos com o SELECT
    order_to_process_list = df['id'].to_list()
    print(f"Falta processar: {len(order_to_process_list)} ID's de pedidos")
    
    # Consulta cada  id  de pedido na API do cartpanda para recuperar o valor de frete até o limite da API
    for order_id in order_to_process_list:
        try_count = 0
        max_try_limit = 3
        returned_status = 200
        
        if returned_status == 200:

            while try_count < max_try_limit:
                url = f"https://accounts.cartpanda.com/api/store-slug/orders/{order_id}"
                headers = {
                "Accept": "application/json",
                "Authorization": "Bearer store-token"
                }
                response = requests.get(url, headers=headers)
                
                if response.status_code != 200:
                    returned_status = response.status_code
                    break
                elif response.status_code == 200:
                    orders = response.json()
                    shipping_data = orders['order'].get('orders_shippings')

                    if shipping_data:
                        shipping_df.append(shipping_data)
                        print(f'O frete do pedido {order_id} foi processado com sucesso!')

                    else:
                        print(f'Nenhum dado de frete encontrado para o pedido {order_id}.')
                    sleep(1)
                    break

                else:
                    try_count += 1
                    print(f"Tentativa {try_count} para o pedido {order_id} falhou. Status: {response.status_code}")
                    sleep(1)
            
                if try_count == max_try_limit:
                    print(f"Não foi possível obter os dados do pedido {order_id} após {max_try_limit} tentativas.")
        
        else:
            break

    # Converte os dados obtidos enquanto a API permitiu em um dataframe
    df_frete = pd.DataFrame(shipping_df)

    df_frete = df_frete[['id', 'order_id', 'price', 'title', 'created_at', 'updated_at', 'shipping_gateway']]
    df_frete = df_frete.rename(columns={'price': 'frete_valor'})
    df_frete = df_frete.rename(columns={'id': 'frete_id'})
    df_frete = df_frete.rename(columns={'order_id': 'frete_order_id'})
    df_frete = df_frete.rename(columns={'title': 'frete_title'})
    df_frete = df_frete.rename(columns={'created_at': 'frete_created_at'})
    df_frete = df_frete.rename(columns={'updated_at': 'frete_updated_at'})
    df_frete = df_frete.rename(columns={'shipping_gateway': 'frete_gateway'})

    # Salva lote de fretes processados no bigquery na tabela  processed_shipping_table
    table_id = 'google_cloud_project.schema_name.processed_shipping_table'
    save_bigquery(table_id, df_frete)
    print(f'A tabela {table_id} foi carregada com sucesso no BigQuery')

def update_shipping_into_cartpanda_orders():
    """Roda UPDATE dos fretes processados na tabela final completa"""
    # Consulta fretes processados com sucesso
    query = """UPDATE `google_cloud_project.schema_name.orders_table` AS co
        SET
        co.frete_valor = cps.frete_valor,
        co.frete_id = cps.frete_id,
        co.frete_order_id = cps.frete_order_id,
        co.frete_title = cps.frete_title,
        co.frete_created_at = cps.frete_created_at,
        co.frete_updated_at = cps.frete_updated_at,
        co.frete_gateway = cps.frete_gateway,
        co.shipping_gateway = cps.shipping_gateway,
        co.shipping_processed_status = 'processed'
        FROM `google_cloud_project.schema_name.processed_shipping_table` AS cps
        WHERE co.id = cps.frete_order_id"""

    try:
        login_bigquery(query=query)
    except:
        print(f'O UPDATE foi realizado com sucesso no Big Query')

# Define as tasks
atualizing_stage_table_task = PythonOperator(
    task_id='load_staging_area_table',
    python_callable=load_staging_cartpanda,
    dag=dag,
)

insert_new_data_task = PythonOperator(
    task_id='insert_new_data_cartpanda_orders_table',
    python_callable=insert_new_data_final_table,
    dag=dag,
)

process_incremental_shipping_task = PythonOperator(
    task_id='process_and_update_shipping_info',
    python_callable=process_shipping_batch,
    dag=dag,
)

final_update_processed_shipping_task = PythonOperator(
    task_id='atualiza_cartpanda_orders_com_fretes_processados',
    python_callable=update_shipping_into_cartpanda_orders,
    dag=dag,
)

# Define dependências
atualizing_stage_table_task >> insert_new_data_task >> process_incremental_shipping_task >> final_update_processed_shipping_task
