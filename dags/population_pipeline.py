# dags/custom_population_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

# URLs y paths de los datos a descargar
DATA_URL_1 = "https://raw.githubusercontent.com/datasets/population/master/data/population.csv"
DATA_URL_2 = "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv"
RAW_DATA_PATH_1 = "/tmp/raw_data_1.csv"
RAW_DATA_PATH_2 = "/tmp/raw_data_2.csv"
MERGED_DATA_PATH = "/tmp/merged_data.csv"
REPORT_PATH = "/output/combined_report.txt" 

# Funciones de procesamiento
def download_data_1():
    response = requests.get(DATA_URL_1)
    with open(RAW_DATA_PATH_1, "w") as f:
        f.write(response.text)

def download_data_2():
    response = requests.get(DATA_URL_2)
    with open(RAW_DATA_PATH_2, "w") as f:
        f.write(response.text)

def merge_data():
    # Leer ambos archivos
    df1 = pd.read_csv(RAW_DATA_PATH_1)
    df2 = pd.read_csv(RAW_DATA_PATH_2)

    # Hacer un merge de ambos archivos en función de los campos comunes (por ejemplo, 'Country Name' y 'Year')
    merged_df = pd.merge(df1, df2, on=['Country Name', 'Year'], how='inner')
    merged_df.to_csv(MERGED_DATA_PATH, index=False)

def generate_combined_report():
    df = pd.read_csv(MERGED_DATA_PATH)
    # Filtrar o transformar datos según sea necesario; aquí se genera un informe simple
    top_countries = df.groupby('Country Name')['Value_x'].max().sort_values(ascending=False).head(5)

    with open(REPORT_PATH, "w") as f:
        f.write("Top 5 Países por Población Combinada\n")
        f.write("==================================\n\n")
        for country, population in top_countries.items():
            f.write(f"{country}: {population:,.0f}\n")

# Definición del DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'custom_population_analysis',
    default_args=default_args,
    description='Pipeline de análisis de población con descargas paralelas y merge',
    schedule_interval=timedelta(days=1),
)

# Tareas del DAG
t1 = PythonOperator(
    task_id='download_data_1',
    python_callable=download_data_1,
    dag=dag,
)

t2 = PythonOperator(
    task_id='download_data_2',
    python_callable=download_data_2,
    dag=dag,
)

t3 = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    dag=dag,
)

t4 = PythonOperator(
    task_id='generate_combined_report',
    python_callable=generate_combined_report,
    dag=dag,
)

# Definir dependencias
[t1, t2] >> t3 >> t4
