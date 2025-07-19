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
def insert_log_data(start_reader,
                    start_loading_time,
                    end_loading_time,
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
                    start_reader,
                    start_loading_time,
                    end_loading_time,
                    file_name,
                    log_file
                ) VALUES (%s, %s, %s, %s, %s)""",
                    (start_reader,
                    start_loading_time,
                    end_loading_time,
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
    start_reader = None
    start_loading_time = None
    end_loading_time = None
    file_processors = {
        'ft_balance_f.csv': lambda df: process_dataframe(df, [0]),
        'ft_posting_f.csv': lambda df: process_dataframe(df, [0]),
        'md_account_d.csv': lambda df: process_dataframe(df, [0, 1]),
        'md_currency_d.csv': lambda df: process_dataframe(df, [1, 2]),
        'md_exchange_rate_d.csv': lambda df: process_dataframe(df, [0, 1]),
        'md_ledger_account_s.csv': lambda df: process_dataframe(df, [10, 11]),
        'dm_f101_round_f.csv' : lambda df: df
    }
    dtype_mappings = {
        'ft_balance_f.csv': {
            'on_date': types.DATE,
            'account_rk': types.BIGINT,
            'currency_rk': types.BIGINT,
            'balance_out': types.DOUBLE_PRECISION
        },
        'ft_posting_f.csv': {
            'oper_date': types.DATE,
            'credit_account_rk': types.BIGINT,
            'debet_account_rk': types.BIGINT,
            'credit_amount': types.DOUBLE_PRECISION,
            'debet_amount': types.DOUBLE_PRECISION
        },
        'md_account_d.csv': {
            'data_actual_date': types.DATE,
            'data_actual_end_date': types.DATE,
            'account_rk': types.BIGINT,
            'account_number': types.VARCHAR(length=20),
            'char_type': types.VARCHAR(length=1),
            'currency_rk': types.BIGINT,
            'currency_code': types.VARCHAR(length=200)
        },
        'md_currency_d.csv': {
            'currency_rk': types.BIGINT,
            'data_actual_date': types.DATE,
            'data_actual_end_date': types.DATE,
            'currency_code': types.VARCHAR(length=3),
            'code_iso_char': types.VARCHAR(length=3)
        },
        'md_exchange_rate_d.csv': {
            'data_actual_date': types.DATE,
            'data_actual_end_date': types.DATE,
            'currency_rk': types.BIGINT,
            'reduced_cource': types.DOUBLE_PRECISION,
            'code_iso_num': types.VARCHAR(length=3)
        },
        'md_ledger_account_s.csv': {
            'chapter': types.CHAR(length=1),
            'chapter_name': types.VARCHAR(length=16),
            'section_number': types.INTEGER,
            'section_name': types.VARCHAR(length=22),
            'subsection_name': types.VARCHAR(length=21),
            'ledger1_account': types.INTEGER,
            'ledger1_account_name': types.VARCHAR(length=47),
            'ledger_account': types.INTEGER,
            'ledger_account_name': types.VARCHAR(length=153),
            'characteristic': types.CHAR(length=1),
            'is_resident': types.INTEGER,
            'is_reserve': types.INTEGER,
            'is_reserved': types.INTEGER,
            'is_loan': types.INTEGER,
            'is_reserved_assets': types.INTEGER,
            'is_overdue': types.INTEGER,
            'is_interest': types.INTEGER,
            'pair_account': types.VARCHAR(length=5),
            'start_date': types.DATE,
            'end_date': types.DATE,
            'is_rub_only': types.INTEGER,
            'min_term': types.VARCHAR(length=1),
            'min_term_measure': types.VARCHAR(length=1),
            'max_term': types.VARCHAR(length=1),
            'max_term_measure': types.VARCHAR(length=1),
            'ledger_acc_full_name_translit': types.VARCHAR(length=1),
            'is_revaluation': types.VARCHAR(length=1),
            'is_correct': types.VARCHAR(length=1)
        },
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

        start_reader = datetime.datetime.now()
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
            time.sleep(5)

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

            if filename in ('ft_balance_f.csv',
                            'ft_posting_f.csv',
                            'md_account_d.csv',
                            'md_currency_d.csv',
                            'md_exchange_rate_d.csv',
                            'md_ledger_account_s.csv'):
                processed_df.to_sql(
                    filename.replace('.csv', ''),
                    engine,
                    schema='ds',
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=10000,
                    dtype= dtype_mappings[filename]
                )
            end_loading_time = datetime.datetime.now()

            insert_log_data(start_reader,
                            start_loading_time,
                            end_loading_time,
                            filename,
                            f'Данные успешно загружены')

            logging.info(f'Данные файла {filename} успешно загружены в БД')

        except Exception as e:
            insert_log_data(start_reader,
                                start_loading_time,
                                end_loading_time,
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


