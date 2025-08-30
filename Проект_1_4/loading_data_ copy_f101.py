import pandas as pd
import sqlalchemy as sa
import psycopg2
from dns.e164 import query
from sqlalchemy import create_engine
from sqlalchemy import types
from sqlalchemy import text
import re
import os
import chardet
import datetime
import logging

logging.basicConfig(filename='app.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )


def loading_data(directory_path, engine):
    file_processors = {
        'dm_f101_round_f.csv' : lambda df: df
    }
    dtype_mappings = {
            'dm_f101_round_f.csv':{
            'from_date': types.DATE,
            'to_date': types.DATE,
            'chapter': types.CHAR(length=1),
            'ledger_account': types.CHAR(length=5),
            'characteristic': types.CHAR(length=1),
            'balance_in_rub': types.NUMERIC(23,8),
            'balance_in_val': types.NUMERIC(23,8),
            'balance_in_total': types.NUMERIC(23,8),
            'turn_deb_rub': types.NUMERIC(23,8),
            'turn_deb_val': types.NUMERIC(23,8),
            'turn_deb_total': types.NUMERIC(23,8),
            'turn_cre_rub': types.NUMERIC(23,8),
            'turn_cre_val': types.NUMERIC(23,8),
            'turn_cre_total': types.NUMERIC(23,8),
            'balance_out_rub': types.NUMERIC(23,8),
            'balance_out_val': types.NUMERIC(23,8),
            'balance_out_total': types.NUMERIC(23,8)
        }
    }

    # Извлечение данных из файла
    for filename in os.listdir(directory_path):
        if filename not in file_processors:
            logging.error(f'Нет файлов для загрузки')
            continue

        file_path = os.path.join(directory_path, filename)

        try:
            with open(file_path, 'rb') as f:
                result_encoding = chardet.detect(f.read())['encoding']
                df = pd.read_csv(file_path,
                                sep=',',
                                encoding=result_encoding,
                                dtype='str')

            if filename in ('dm_f101_round_f.csv'):
                with engine.connect() as conn:
                    query_create_temp_table = text("""
                                            CREATE TEMPORARY TABLE temp_table(
	                                                FROM_DATE DATE
	                                                , TO_DATE DATE
	                                                , CHAPTER CHAR(1)
	                                                , LEDGER_ACCOUNT CHAR(5)
	                                                , CHARACTERISTIC CHAR(1)
	                                                , BALANCE_IN_RUB NUMERIC(23,8)
	                                                , BALANCE_IN_VAL NUMERIC(23,8)
	                                                , BALANCE_IN_TOTAL NUMERIC(23,8)
	                                                , TURN_DEB_RUB NUMERIC(23,8)
	                                                , TURN_DEB_VAL NUMERIC(23,8)
	                                                , TURN_DEB_TOTAL NUMERIC(23,8)
	                                                , TURN_CRE_RUB NUMERIC(23,8)
	                                                , TURN_CRE_VAL NUMERIC(23,8)
	                                                , TURN_CRE_TOTAL NUMERIC(23,8)
	                                                , BALANCE_OUT_RUB NUMERIC(23,8)
	                                                , BALANCE_OUT_VAL NUMERIC(23,8)
	                                                , BALANCE_OUT_TOTAL NUMERIC(23,8))
                                                """)
                    query_insert = text(
                        """
                        INSERT INTO dm.dm_f101_round_f_v2 (
                            from_date
                            , to_date
                            , chapter
                            , ledger_account
                            , characteristic
                            , balance_in_rub
                            , balance_in_val
                            , balance_in_total
                            , turn_deb_rub
                            , turn_deb_val
                            , turn_deb_total
                            , turn_cre_rub
                            , turn_cre_val
                            , turn_cre_total
                            , balance_out_rub
                            , balance_out_val
                            , balance_out_total
                        ) 
                        SELECT from_date
                            , to_date
                            , chapter
                            , ledger_account
                            , characteristic
                            , balance_in_rub
                            , balance_in_val
                            , balance_in_total
                            , turn_deb_rub
                            , turn_deb_val
                            , turn_deb_total
                            , turn_cre_rub
                            , turn_cre_val
                            , turn_cre_total
                            , balance_out_rub
                            , balance_out_val
                            , balance_out_total 
                        FROM temp_table
                        ON CONFLICT (ledger_account, from_date) DO
                            UPDATE SET 
                                to_date = EXCLUDED.to_date
                                , chapter = EXCLUDED.chapter
                                , characteristic = EXCLUDED.characteristic
                                , balance_in_rub = EXCLUDED.balance_in_rub
                                , balance_in_val = EXCLUDED.balance_in_val
                                , balance_in_total = EXCLUDED.balance_in_total
                                , turn_deb_rub = EXCLUDED.turn_deb_rub
                                , turn_deb_val = EXCLUDED.turn_deb_val
                                , turn_deb_total = EXCLUDED.turn_deb_total
                                , turn_cre_rub = EXCLUDED.turn_cre_rub
                                , turn_cre_val = EXCLUDED.turn_cre_val
                                , turn_cre_total = EXCLUDED.turn_cre_total
                                , balance_out_rub = EXCLUDED.balance_out_rub
                                , balance_out_val = EXCLUDED.balance_out_val
                                , balance_out_total = EXCLUDED.balance_out_total
                        """
                        )

                    with conn.begin():
                        conn.execute(query_create_temp_table)

                        df.to_sql(
                            'temp_table',
                            conn,
                            schema=None,
                            if_exists='append',
                            index=False,
                            method='multi',
                            chunksize=10000,
                            dtype=dtype_mappings[filename]
                        )

                        conn.execute(query_insert)

            logging.info(f'Данные файла {filename} успешно загружены в БД')

        except Exception as e:
            logging.error(f'Ошибка при обработке файла {filename}: {str(e)}')
            raise


# Подключение к БД
def database_connection():
    try:
        engine = create_engine(
            f'postgresql://{os.getenv("DB_USER")}:'
            f'{os.getenv("DB_PASSWORD")}@'
            f'{os.getenv("DB_HOST")}:'
            f'{os.getenv("DB_PORT")}/'
            f'{os.getenv("DB_NAME")}'
        )

        return engine
    except Exception as e:
        logging.error(f"Ошибка подключения к БД: {str(e)}")
        raise


# Функция запуска ETL
def main():
    try:
        engine = database_connection()
        loading_data(os.getenv("DIRECTORY_PATH"), engine)

    except Exception as e:
        logging.error(f"Критическая ошибка: {str(e)}")
        raise

if __name__ == "__main__":
    main()

