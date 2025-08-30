from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from write_logs import start_write_logs, end_write_logs

import logging

logging.basicConfig(filename="app.log",
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s"
                    )

PATH = Variable.get("export_to_csv")
conf.set("core", "template_searchpath", PATH)

default_args = {
    "owner": "maiorovkv",
    "start_date": datetime(2025,8,6),
    "retries": 2
}

# Перенос данных из DM_F101_ROUND_F витрины в csv
def export_to_csv(table_name):
    start_write_logs(table_name, "Выгрузка формы 101 в csv файл")
    try:
        file_path = PATH + 'dm_f101_round_f.csv'
        query = "select * from DM.DM_F101_ROUND_F"

        postgres_hook = PostgresHook("postgres-db")

        with postgres_hook.get_conn() as conn:
            df = pd.read_sql(query, conn)
            df.to_csv(file_path, index=False, encoding='utf-8')

    except Exception as e:
        logging.error(f'Ошибка при создании csv файла {file_path}: {str(e)}')
        raise
    finally:
        if conn:
            conn.close()

    end_write_logs(table_name)


with DAG(
    'export_to_csv',
    default_args=default_args,
    description="Экспорт формы 101 в csv файл",
    schedule=None
) as dag:

    export_to_csv = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_to_csv,
        op_kwargs= {"table_name": "export_to_csv"}
    )

    (
    export_to_csv
    )


