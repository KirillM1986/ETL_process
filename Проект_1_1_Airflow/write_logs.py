from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

import time


def start_write_logs(**kwargs):
    dag_name = kwargs['dag'].dag_id

    query = """
        INSERT INTO logs.etl_logs (dag_name, start_loading_time)
        VALUES (%s, CURRENT_TIMESTAMP)
        RETURNING log_id
        """


    postgres_hook = PostgresHook("postgres-db")

    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (dag_name,))
            process_id = cursor.fetchone()[0]

    kwargs['ti'].xcom_push(key='process_id', value=process_id)

    time.sleep(5)




def end_write_logs(**kwargs):
    process_id = kwargs['ti'].xcom_pull(
        task_ids='start_write_logs',
        key='process_id'
    )

    query = """
            UPDATE logs.etl_logs
            SET end_loading_time = CURRENT_TIMESTAMP,
                log_file = 'Данные успешно загружены'
            WHERE log_id = %s
            """
    postgres_hook = PostgresHook("postgres-db")

    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (process_id,))