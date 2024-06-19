from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json

default_args = {
'owner': 'airflow',
'depends_on_past': False,
'start_date': datetime(2024, 6, 16),
'email': ['seu-email@example.com'],
'email_on_failure': True,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=5),
}

dag = DAG(
'data_pipeline',
default_args=default_args,
description='Pipeline de Dados: API para Camadas de Dados',
schedule_interval=timedelta(hours=6),
)

url = 'https://api.openbrewerydb.org/breweries'
output_dir_bronze = 'dags/bronze/'
output_dir_silver = 'dags/silver/'
output_dir_gold = 'dags/gold/'

def process_layer_bronze():


    # Diretório onde o arquivo JSON será salvo
    if not os.path.exists(output_dir_bronze):
        os.makedirs(output_dir_bronze)

    # Nome do arquivo JSON com data/hora
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir_bronze, f'raw_data_{current_time}.json')

    try:
        # Fazer a requisição GET à API
        response = requests.get(url)
        response.raise_for_status()  # Lança um erro para status >= 400

        # Parse do JSON
        raw_data = response.json()
        print(raw_data)

        # Salvar os dados brutos em um arquivo JSON
        with open(output_file, 'w') as f:
            json.dump(raw_data, f, indent=4)
        
        print(f'Dados brutos salvos em {output_file}')
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except requests.exceptions.RequestException as req_err:
        print(f'Error occurred: {req_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    
#Função para encontrar o arquivo mais recente no diretório
def get_latest_file(directory, file_extension='.json'):
    files = [f for f in os.listdir(directory) if f.endswith(file_extension)]
    if not files:
        raise FileNotFoundError(f"No files with extension {file_extension} found in {directory}")
    files = [os.path.join(directory, f) for f in files]
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def process_layer_silver():


    latest_json_file = get_latest_file(output_dir_bronze)
    if not os.path.exists(output_dir_silver):
        os.makedirs(output_dir_silver)
    # Nome do arquivo Parquet
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir_silver, f'brewery_data_{current_time}.parquet')
    try:
        # Carregar os dados em um DataFrame do pandas
        df = pd.read_json(latest_json_file)

        
        # Exemplo de transformação: Particionar por localização
        df['country'] = df['country'].str.title()  # Normalização de string
        df['state_province'] = df['state'].str.title()  # Normalização de string
        df['city'] = df['city'].str.title()

        # Salvar os dados no formato Parquet, particionados por localização da cervejaria
        df.to_parquet(output_file, partition_cols=['country','state_province','city'], engine='pyarrow', index=False)

        print(f'Dados transformados e salvos em {output_file}')
    except Exception as err:
        print(f'Erro ao transformar e salvar os dados: {err}')

def process_layer_gold():


    if not os.path.exists(output_dir_gold):
        os.makedirs(output_dir_gold)
    try:
        # Carregar os dados do Parquet em um DataFrame do pandas
        df = pd.read_parquet(get_latest_file(output_dir_silver,'.parquet'))
        # Agregar a quantidade de cervejarias por tipo e localização
        aggregated_df = df.groupby(['brewery_type', 'country', 'state_province', 'city']).size().reset_index(name='brewery_count')

        # Filtrar para incluir apenas onde a contagem é maior que 1
        filtered_df = aggregated_df[aggregated_df['brewery_count'] > 0]
        current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(output_dir_gold, f'aggregated_data.parquet')
        filtered_df.to_parquet(output_file, engine='pyarrow', index=False)
        print(f'Dados agregados e salvos em {output_dir_gold}')
    except Exception as err:
        print(f'Erro ao agregar e salvar os dados: {err}')
    
t1 = PythonOperator(
task_id='get_data_to_bronze',
python_callable=process_layer_bronze,
dag=dag,
)

t2 = PythonOperator(
task_id='transform_2_silver',
python_callable=process_layer_silver,
dag=dag,
)

t3 = PythonOperator(
task_id='gold_view',
python_callable=process_layer_gold,
dag=dag,
)

t1 >> t2 >> t3