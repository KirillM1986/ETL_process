from airflow.providers.postgres.hooks.postgres import PostgresHook


import logging

logging.basicConfig(filename='app.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )


def start_write_logs(table_name, operation):
    query = """
        INSERT INTO logs.etl_logs (name_table, operation, start_time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        """
    try:
        postgres_hook = PostgresHook("postgres-db")

        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (f'stage.{table_name}', operation))

    except postgres_hook.Error as e:
        logging.error(f"Ошибка при записи в таблицу LOGS.ETL_LOGS: {e}")
        raise






def end_write_logs(table_name):

    query = """
            UPDATE logs.etl_logs
            SET end_time = CURRENT_TIMESTAMP,
                log_file = 'Данные успешно загружены'
            """
    try:
        postgres_hook = PostgresHook("postgres-db")

        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (table_name,))

    except postgres_hook.Error as e:
        logging.error(f"Ошибка при записи в таблицу LOGS.ETL_LOGS: {e}")
        raise