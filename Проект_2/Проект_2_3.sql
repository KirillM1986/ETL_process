-- account_rk для проверки изменений
SELECT * FROM rd.account_balance
WHERE account_rk = '2007640'
SELECT * FROM dm.account_balance_turnover
WHERE account_rk = '2007640'


/*
Подготовить запрос, который определит корректное значение поля account_in_sum. 
Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются, 
то корректным выбирается значение account_out_sum предыдущего дня.
*/
WITH prev_day_balance AS (
	SELECT 
        account_rk,
        effective_date,
		account_in_sum,
		account_out_sum,
        LAG(account_out_sum, 1, 0) OVER (PARTITION BY account_rk ORDER BY effective_date) AS prev_account_out_sum
    FROM rd.account_balance
)

SELECT pdb.account_rk,
		pdb.effective_date,
		CASE 
			WHEN pdb.account_in_sum != pdb.prev_account_out_sum THEN pdb.prev_account_out_sum 
			ELSE pdb.account_in_sum
		END AS account_in_sum,
		pdb.account_out_sum
FROM prev_day_balance pdb
-- WHERE pdb.account_rk = '2007640'


/* 
Подготовить такой же запрос, только проблема теперь в том, что account_in_sum одного дня правильная, 
а account_out_sum предыдущего дня некорректна. Это означает, что если эти значения отличаются, 
то корректным значением для account_out_sum предыдущего дня выбирается значение account_in_sum текущего дня. 
*/
WITH next_day_balance AS (
	SELECT 
        account_rk,
        effective_date,
		account_in_sum,
		account_out_sum,
        LEAD(account_in_sum, 1, account_out_sum) OVER (PARTITION BY account_rk ORDER BY effective_date) AS next_account_in_sum
    FROM rd.account_balance

)

SELECT pdb.account_rk,
		pdb.effective_date,
		pdb.account_in_sum,
		CASE 
			WHEN pdb.account_out_sum != pdb.next_account_in_sum THEN pdb.next_account_in_sum
			ELSE pdb.account_out_sum 
		END AS account_out_sum	
FROM next_day_balance pdb
--WHERE pdb.account_rk = '2007640'   


/* 
Подготовить запрос, который поправит данные в таблице rd.account_balance,
используя уже имеющийся запрос из п.1
*/
WITH prev_day_balance AS (
	SELECT 
        account_rk,
        effective_date,
		account_in_sum,
		account_out_sum,
        COALESCE(LAG(account_out_sum) OVER (PARTITION BY account_rk ORDER BY effective_date), 0) AS prev_account_out_sum
    FROM rd.account_balance
)

UPDATE rd.account_balance ab
SET account_in_sum = corrected_sum.correct_account_in_sum
	FROM (
		SELECT 
			account_rk,
        	effective_date,
			CASE 
				WHEN account_out_sum != prev_account_out_sum THEN prev_account_out_sum
				ELSE account_in_sum 
			END AS correct_account_in_sum	
		FROM prev_day_balance ) AS corrected_sum
		WHERE ab.account_rk = corrected_sum.account_rk 
			AND ab.effective_date = corrected_sum.effective_date


/* 
Написать процедуру по аналогии с задание 2.2 для перезагрузки данных в витрину
*/
CREATE OR REPLACE PROCEDURE reload_account_balance_turnover() AS $$
DECLARE
	v_error_text text;
BEGIN
	DELETE FROM dm.account_balance_turnover;

	INSERT INTO dm.account_balance_turnover(account_rk, currency_name, department_rk, effective_date, account_in_sum, account_out_sum)
	SELECT a.account_rk AS account_rk,
	   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
	   a.department_rk AS department_rk,
	   ab.effective_date AS effective_date,
	   ab.account_in_sum AS account_in_sum,
	   ab.account_out_sum AS account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;

EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS
			v_error_text := MESSAGE_TEXT;
			RAISE EXCEPTION 'Произошла ошибка загрузки данных: %', v_error_text;
END;
$$ LANGUAGE plpgsql;


-- вызов процедуры перезагрузки dm.account_balance_turnover
CALL reload_account_balance_turnover();


-- проверка результата
SELECT * FROM dm.account_balance_turnover

ORDER BY account_rk, effective_date
