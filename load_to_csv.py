import psycopg2
import csv
import os
import datetime
import logging

logging.basicConfig(filename='app.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )


# Перенос данных из DM_F101_ROUND_F витрины в csv
def db_to_csv(directory_path, conn):
    try:
        file_path = os.path.join(directory_path, 'dm_f101_round_f.csv')
        start_loading_time = datetime.datetime.now()

        cursor = conn.cursor()
        cursor.execute('select * from DM.DM_F101_ROUND_F')

        column_names = []
        for row in cursor.description:
            column_names.append(row[0])

        with open(file_path, 'w', newline='', encoding='UTF-8') as filename:
            write_filename = csv.writer(filename, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            write_filename.writerow(column_names)
            for row in cursor:
                write_filename.writerow(row)

        end_loading_time = datetime.datetime.now()
        # Логирование в БД формирования файла формата csv
        cursor.execute("""
                INSERT INTO LOGS.ETL_LOGS (
                        start_loading_time,
                        end_loading_time,
                        file_name,
                        log_file
                )
                VALUES (%s, %s, %s, %s)""",
                (start_loading_time,
                end_loading_time,
                'dm_f101_round_f',
                'CSV файл успешно сформирован'))

        conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        cursor.execute("""
                        INSERT INTO LOGS.ETL_LOGS (
                                start_loading_time,
                                end_loading_time,
                                file_name,
                                log_file
                        )
                        VALUES (%s, %s, %s, %s)""",
                            (start_loading_time,
                            end_loading_time,
                            'dm_f101_round_f',
                            f'Ошибка при создании csv файла {filename}: {str(e)}'))
        logging.error(f'Ошибка при создании csv файла {filename}: {str(e)}')
        raise
    finally:
        if conn:
            conn.close()


# Подключение к БД
def database_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except Exception as e:
        logging.error(f"Ошибка подключения к БД: {str(e)}")
        raise


# Функция запуска формирования файла csv
def main():
    try:
        conn = database_connection()
        db_to_csv(os.getenv("DIRECTORY_PATH"), conn)
    except Exception as e:
        logging.error(f"Критическая ошибка: {str(e)}")
        raise


if __name__ == "__main__":
    main()


