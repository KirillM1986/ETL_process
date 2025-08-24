import time

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import types
import re
import os
import chardet
import datetime
import logging

logging.basicConfig(filename='app.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )


# Логирование в БД процесса ETL
def insert_log_data(start_loading_time,
                    file_name,
                    log_file):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        cur = conn.cursor()

        cur.execute("""
                INSERT INTO LOGS.ETL_LOGS (
                    start_loading_time,
                    end_loading_time,
                    dag_name,
                    log_file
                ) VALUES (%s, %s, %s, %s)""",
                    (
                    start_loading_time,
                    datetime.datetime.now(),
                    file_name,
                    log_file))

        conn.commit()

    except psycopg2.Error as e:
        logging.error(f"Ошибка при записи в таблицу LOGS.ETL_LOGS: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


# Преобразует дату в формат ГГГГ-ММ-ДД
def format_date(date_string):
    patterns = [
        (r'\d{2}\.\d{2}\.\d{4}', lambda x: re.sub(r'(\d{2})\.(\d{2})\.(\d{4})', r'\3-\2-\1', x)),
        (r'\d{4}\.\d{2}\.\d{2}', lambda x: x.replace('.', '-')),
        (r'\d{4}-\d{2}-\d{2}', lambda x: x),
        (r'\d{2}-\d{2}-\d{4}', lambda x: re.sub(r'(\d{2})-(\d{2})-(\d{4})', r'\3-\2-\1', x))
    ]

    for pattern, transform in patterns:
        if re.match(pattern, date_string):
            return transform(date_string)
    logging.error(f"Некорректный формат даты: {date_string}")
    raise ValueError (f"Некорректный формат даты: {date_string}")


# Форматирование дат в DataFrame
def process_dataframe(reader, columns_to_format):
    try:
        for column_idx in columns_to_format:
            if column_idx < len(reader.columns):
                reader.iloc[:, column_idx] = reader.iloc[:, column_idx].apply(format_date)
        return reader
    except Exception as e:
        logging.error(f"Ошибка при обработке DataFrame: {str(e)}")
        raise


# ETL данных файлов
def process_files(directory_path, engine):
    start_loading_time = None
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

    # Извлечение данных из файлов
    for filename in os.listdir(directory_path):
        if filename not in file_processors:
            logging.error(f'Нет файлов для загрузки')
            continue

        file_path = os.path.join(directory_path, filename)

        try:
            with open(file_path, 'rb') as f:
                result_encoding = chardet.detect(f.read())['encoding']
            reader = pd.read_csv(file_path,
                                sep=';',
                                encoding=result_encoding,
                                dtype='str')

            processor = file_processors[filename]
            # трасформация данных
            processed_df = processor(reader)

            processed_df.columns = [col.lower() for col in processed_df.columns]

            # Загрузка данных в БД
            start_loading_time = datetime.datetime.now()

            if filename in ('dm_f101_round_f.csv'):
                processed_df.to_sql(
                    'dm_f101_round_f_v2',
                    engine,
                    schema='dm',
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=10000,
                    dtype=dtype_mappings[filename]
                )

            insert_log_data(
                            start_loading_time,
                            filename,
                            f'Данные успешно загружены')

            logging.info(f'Данные файла {filename} успешно загружены в БД')

        except Exception as e:
            insert_log_data(start_loading_time,
                                filename,
                                f'Ошибка при обработке файла {filename}: {str(e)}')
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
        process_files(os.getenv("DIRECTORY_PATH"), engine)
    except Exception as e:
        logging.error(f"Критическая ошибка: {str(e)}")
        raise

if __name__ == "__main__":
    main()

