from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable
from write_logs import start_write_logs,end_write_logs

import pandas as pd
import chardet
from datetime import datetime


PATH = Variable.get("path_data")

conf.set("core", "template_searchpath", PATH)

default_args = {
    "owner": "maiorovkv",
    "start_date": datetime(2025,8,6),
    "retries": 2
}


def loading_data_into_database(table_name):

    with open(PATH+f"{table_name}.csv", 'rb') as f:
        result_encoding = chardet.detect(f.read())['encoding']
    df = pd.read_csv(PATH+f"{table_name}.csv", delimiter=";", encoding=result_encoding)

    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name,
              engine,
              schema="stage",
              if_exists="append",
              index=False,
              method='multi',
              chunksize=10000,
    )


with DAG(
    "loading_data_into_database",
    default_args=default_args,
    description="Загрузка данных в БД",
    template_searchpath=["/home/airflow/files/sql"],
    schedule=None
) as dag:

    start_write_logs = PythonOperator(
        task_id="start_write_logs",
        python_callable=start_write_logs
    )

    clearing_tables_stage = SQLExecuteQueryOperator(
        task_id="clearing_tables_stage",
        conn_id="postgres-db",
        sql="clearing_tables_stage.sql"
    )

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=loading_data_into_database,
        op_kwargs={"table_name": "ft_balance_f"}
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=loading_data_into_database,
        op_kwargs={"table_name": "ft_posting_f"}
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=loading_data_into_database,
        op_kwargs={"table_name": "md_account_d"}
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=loading_data_into_database,
        op_kwargs={"table_name": "md_currency_d"}
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=loading_data_into_database,
        op_kwargs={"table_name": "md_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=loading_data_into_database,
        op_kwargs={"table_name": "md_ledger_account_s"}
    )

    split_1 = EmptyOperator(
        task_id="split_1"
    )

    sql_ft_balance_f = SQLExecuteQueryOperator(
        task_id="sql_ft_balance_f",
        conn_id="postgres-db",
        sql="ft_balance_f.sql"
    )

    sql_ft_posting_f = SQLExecuteQueryOperator(
        task_id="sql_ft_posting_f",
        conn_id="postgres-db",
        sql="ft_posting_f.sql"
    )

    sql_md_account_d = SQLExecuteQueryOperator(
        task_id="sql_md_account_d",
        conn_id="postgres-db",
        sql="md_account_d.sql"
    )

    sql_md_currency_d = SQLExecuteQueryOperator(
        task_id="sql_md_currency_d",
        conn_id="postgres-db",
        sql="md_currency_d.sql"
    )

    sql_md_exchange_rate_d = SQLExecuteQueryOperator(
        task_id="sql_md_exchange_rate_d",
        conn_id="postgres-db",
        sql="md_exchange_rate_d.sql"
    )

    sql_md_ledger_account_s = SQLExecuteQueryOperator(
        task_id="sql_md_ledger_account_s",
        conn_id="postgres-db",
        sql="md_ledger_account_s.sql"
    )

    end_write_logs = PythonOperator(
        task_id="end_write_logs",
        python_callable=end_write_logs
    )

    (
    start_write_logs
    >> clearing_tables_stage
    >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s]
    >> split_1
    >> [sql_ft_balance_f, sql_ft_posting_f, sql_md_account_d, sql_md_currency_d, sql_md_exchange_rate_d, sql_md_ledger_account_s]
    >> end_write_logs
    )