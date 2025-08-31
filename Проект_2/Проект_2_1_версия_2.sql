-- Создать временнную таблицу с уникальными значениями
SELECT DISTINCT *
INTO temptable_client
FROM dm.client;

-- Очистить таблицу dm.client
TRUNCATE dm.client;

-- Наполнить уникальными значениями из временной таблицы таблицу dm.client
INSERT INTO dm.client
SELECT * FROM temptable_client;

-- Удалить временную таблицу
DROP TABLE temptable_client;


