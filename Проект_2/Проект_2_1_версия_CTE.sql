-- Создаю идентификатор для записей в таблице
ALTER TABLE DM.CLIENT ADD COLUMN IF NOT EXISTS row_id SERIAL;


-- Выборка и удаление дублей
WITH unique_rows AS (
  SELECT ROW_NUMBER() OVER(PARTITION BY client_rk, effective_from_date ORDER BY row_id) AS row_num
  , client_rk 
  , effective_from_date
  , row_id
  FROM DM.CLIENT
)
DELETE FROM DM.CLIENT c
WHERE EXISTS (SELECT 1 
        FROM unique_rows ur
        WHERE ur.row_id = c.row_id 
        AND row_num > 1);

-- Удуление идентификатора для записей в таблице
ALTER TABLE DM.CLIENT DROP COLUMN IF EXISTS row_id;

-- Добавление огрангичение уникальности по client_rk, effective_from_date
ALTER TABLE DM.CLIENT ADD CONSTRAINT pk_client_effective_from_date PRIMARY KEY (client_rk, effective_from_date);

