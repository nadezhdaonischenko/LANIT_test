from airflow import DAG
from airflow.operators.python import PythonOperator
import os 
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine

# ПАРАМЕТРЫ POSTGRESQL

DB_USER = "postgres"
DB_PASSWORD = os.getenv("parole_postgresql", "")
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

URL = "https://run.mob-edu.ru/webhook/da-test-sample"

# ETL 

def etl():

    # ИЗВЛЕЧЕНИЕ ДАННЫХ ИЗ API

    response = requests.get(URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    # ПРЕОБРАЗОВАНИЕ ДАННЫХ В ТАБЛИЦЫ
    # 1. КЛИЕНТЫ (clients)

    clients = []
    for item in data:
        clients.append({
            "client_id": int(item["ID"]),
            "title": item.get("TITLE"),
            "first_name": item.get("NAME"),
            "last_name": item.get("LAST_NAME"),
            "status_id": item.get("STATUS_ID"),
            "source_id": item.get("SOURCE_ID"),
            "client_type": item.get("UF_CLIENT_TYPE"),
            "preferred_contact_method": item.get("UF_CONTACT_METHOD")
        })
    df_clients = pd.DataFrame(clients)

    #  2. ТЕЛЕФОНЫ КЛИЕНТОВ (phones)

    phones = []
    for item in data:
        client_id = int(item["ID"])
        for phone in item.get("PHONE", []):
            phones.append({
                "phone_id": int(phone["ID"]),
                "client_id": client_id,
                "phone_type": phone.get("TYPE_ID"),
                "phone_number": phone.get("VALUE"),
                "phone_category": phone.get("VALUE_TYPE")
            })
    df_phones = pd.DataFrame(phones)

    # 3.  ЭЛЕКТРОННАЯ ПОЧТА КЛИЕНТОВ (emails)

    emails = []
    for item in data:
        client_id = int(item["ID"])
        for email in item.get("EMAIL", []):
            emails.append({
                "email_id": int(email["ID"]),
                "client_id": client_id,
                "email_type": email.get("TYPE_ID"),
                "email_address": email.get("VALUE"),
                "email_category": email.get("VALUE_TYPE")
            })
    df_emails = pd.DataFrame(emails)

    # ЗАГРУЗКА ДАННЫХ

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    
    df_clients.to_sql("clients", engine, if_exists="append", index=False) 

    df_phones.to_sql("client_phones", engine, if_exists="append", index=False)

    df_emails.to_sql("client_emails", engine, if_exists="append", index=False) 

# DAG

DAG_ID = "Lanit_test"

default_args = {
    "owner": "NadezdaO",
    "start_date": datetime(2023, 1, 1)
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "postgresql", "api"]
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=etl
    )

    run_etl