CREATE SCHEMA IF NOT EXISTS dm;

-- Создается таблица loading_logs в схеме logs
DROP TABLE IF EXISTS logs.loading_logs;
CREATE TABLE IF NOT EXISTS logs.loading_logs(
	log_id SERIAL PRIMARY KEY,
	settlement_date DATE,
	name_data_marts VARCHAR(70),
	count_rec_row BIGINT,
    start_calculation TIMESTAMP,
	end_calculation TIMESTAMP,
	log_file TEXT
);

-- Создается таблица dm_account_turnover_f в схеме DM
DROP TABLE IF EXISTS dm.dm_account_turnover_f;
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f(
	on_date DATE,
	account_rk BIGINT,
	credit_amount NUMERIC(23,8),
	credit_amount_rub NUMERIC(23,8),
	debet_amount NUMERIC(23,8),
	debet_amount_rub NUMERIC(23,8)
);

-- Создается таблица dm_account_balance_f в схеме DM
DROP TABLE IF EXISTS dm.dm_account_balance_f;
CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f(
	on_date DATE,
    account_rk BIGINT,
	balance_out NUMERIC(23,8),
	balance_out_rub NUMERIC(23,8)
);

-- Загрузка данных в витрину dm_account_turnover_f
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE) AS $$
DECLARE
	v_log_id INTEGER;
	v_count_rec_row BIGINT := 0;
	v_currency_rk INTEGER;
	v_reduced_cource DOUBLE PRECISION;
	account_rk_record RECORD;
	credit_amount_record RECORD;
	debet_amount_record RECORD;
	v_error_text text;
	account_rk_cursor CURSOR FOR 
		SELECT DISTINCT credit_account_rk AS account_rk
		FROM ds.ft_posting_f
		WHERE oper_date = i_OnDate
		UNION
		SELECT DISTINCT debet_account_rk AS account_rk
		FROM ds.ft_posting_f
		WHERE oper_date = i_OnDate;
BEGIN
	-- Логирование начала загрузки данных
	INSERT INTO logs.loading_logs(settlement_date, name_data_marts, start_calculation)
	VALUES(i_OnDate, 'dm_account_turnover_f', CLOCK_TIMESTAMP())
	RETURNING log_id INTO v_log_id;

	DELETE FROM dm.dm_account_turnover_f
	WHERE on_date = i_OnDate;

	-- Трансформация данных для каждого счета с транзакциями за дату
	OPEN account_rk_cursor;
	LOOP
		FETCH account_rk_cursor INTO account_rk_record;
		EXIT WHEN NOT FOUND;

		-- Получение курса валюты
		SELECT er.reduced_cource  
		INTO v_reduced_cource
		FROM ds.md_account_d a
		JOIN ds.md_exchange_rate_d er ON a.currency_rk = er.currency_rk
		WHERE a.account_rk = account_rk_record.account_rk
		AND er.data_actual_date <= i_OnDate
		AND er.data_actual_end_date >= i_OnDate
		LIMIT 1;

		-- Получение значений по кредиту счета
		SELECT SUM(credit_amount)::NUMERIC(23,8) credit_amount,
			(SUM(credit_amount) * COALESCE(v_reduced_cource, 1))::NUMERIC(23,8) credit_amount_rub
		INTO credit_amount_record
		FROM ds.ft_posting_f p
		WHERE p.oper_date = i_OnDate 
		AND p.credit_account_rk = account_rk_record.account_rk;
		-- Получение значений по дебету счета
		SELECT SUM(debet_amount)::NUMERIC(23,8) debet_amount,
			(SUM(debet_amount) * COALESCE(v_reduced_cource, 1))::NUMERIC(23,8) debet_amount_rub
		INTO debet_amount_record
		FROM ds.ft_posting_f p
		WHERE p.oper_date = i_OnDate 
		AND p.debet_account_rk = account_rk_record.account_rk;

		-- Загрузка данных в витрину dm_account_turnover_f
		INSERT INTO dm.dm_account_turnover_f(on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
		VALUES(i_OnDate, 
		account_rk_record.account_rk, 
		credit_amount_record.credit_amount, 
		credit_amount_record.credit_amount_rub,
		debet_amount_record.debet_amount,
		debet_amount_record.debet_amount_rub
		);

		v_count_rec_row = v_count_rec_row + 1;
	
	END LOOP;
	CLOSE account_rk_cursor;

	-- Логирование завершения загрузки данных
	UPDATE logs.loading_logs
		SET count_rec_row = v_count_rec_row,
			end_calculation = CLOCK_TIMESTAMP(),
			log_file = 'Данные успешно загружены'
		WHERE log_id = v_log_id;
EXCEPTION 
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS
			v_error_text := MESSAGE_TEXT;
			RAISE EXCEPTION 'Произошла ошибка загрузки данных в витрину: %', v_error_text;
END;
$$ LANGUAGE plpgsql;



-- Расчет ежедневной витрины за месяц 
DO $$
DECLARE
	OnDate DATE := '2018-01-01'::DATE;
BEGIN
    FOR i IN 0..30 LOOP
        CALL ds.fill_account_turnover_f(OnDate+i);
    END LOOP;
END $$;



-- Наполнение данными витрины dm_account_balance_f за 31.12.2017
INSERT INTO dm.dm_account_balance_f(on_date, account_rk, balance_out, balance_out_rub)
SELECT on_date,
		account_rk::NUMERIC,
		balance_out::NUMERIC(23,8),
		(balance_out * COALESCE(reduced_cource, 1))::NUMERIC(23,8) AS balance_out_rub
FROM ds.ft_balance_f b
LEFT JOIN (SELECT currency_rk, MAX(reduced_cource) AS reduced_cource
		FROM ds.md_exchange_rate_d
		WHERE data_actual_date <= TO_DATE('2017-12-31', 'YYYY-MM-DD')
		AND data_actual_end_date >= TO_DATE('2017-12-31', 'YYYY-MM-DD')
		GROUP BY currency_rk, reduced_cource) AS er ON b.currency_rk = er.currency_rk




-- Загрузка данных в витрину dm_account_balance_f
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE) AS $$
DECLARE
	v_count_rec_row BIGINT := 0;
	v_credit_amount dm.dm_account_turnover_f.credit_amount%TYPE;
	v_debet_amount dm.dm_account_turnover_f.debet_amount%TYPE;
	v_reduced_cource ds.md_exchange_rate_d.reduced_cource%TYPE;
	account_rk_record RECORD;
	v_log_id BIGINT;
	v_error_text TEXT;

BEGIN
	-- Логирование начала загрузки данных
	INSERT INTO logs.loading_logs(settlement_date, name_data_marts, start_calculation)
	VALUES(i_OnDate, 'dm_account_balance_f', CLOCK_TIMESTAMP())
	RETURNING log_id INTO v_log_id;

	DELETE FROM dm.dm_account_balance_f
	WHERE on_date = i_OnDate;
	-- Баланс счета на предыдущую дату
	FOR account_rk_record IN 
		SELECT i_OnDate,
			acc.char_type,
			acc.account_rk,
			acc.currency_rk,
			CASE
				WHEN acc.char_type = 'А' THEN (SELECT accb.balance_out 
														FROM dm.dm_account_balance_f accb
														WHERE accb.account_rk = acc.account_rk AND on_date = i_OnDate - 1)
				WHEN acc.char_type = 'П' THEN (SELECT accb.balance_out 
														FROM dm.dm_account_balance_f accb
														WHERE accb.account_rk = acc.account_rk AND on_date = i_OnDate - 1)
			END balance_prev
		FROM ds.md_account_d acc
		WHERE i_OnDate BETWEEN data_actual_date AND data_actual_end_date
	LOOP 
		-- Получаем изменение баланса по кредиту и дебету счета
		SELECT COALESCE(SUM(credit_amount), 0),
				COALESCE(SUM(debet_amount), 0)
		INTO v_credit_amount, v_debet_amount
		FROM dm.dm_account_turnover_f acct
		WHERE acct.account_rk = account_rk_record.account_rk and on_date = i_OnDate;
		
		-- Получаем курс валюты на текущую дату
		SELECT reduced_cource
		INTO v_reduced_cource
		FROM ds.md_exchange_rate_d er
		WHERE er.currency_rk = account_rk_record.currency_rk
		AND er.data_actual_date <= i_OnDate
		AND er.data_actual_end_date >= i_OnDate
		LIMIT 1; 

		-- Загрузка данных активных счетов в витрину dm_account_balance_f
		IF account_rk_record.char_type = 'А' THEN
		INSERT INTO dm.dm_account_balance_f(on_date, account_rk, balance_out, balance_out_rub)
		VALUES(i_OnDate,
				account_rk_record.account_rk,
				COALESCE(account_rk_record.balance_prev, 0) + v_debet_amount - v_credit_amount, 
				(COALESCE(account_rk_record.balance_prev, 0) + v_debet_amount - v_credit_amount) * COALESCE(v_reduced_cource, 1)
				);
		END IF;
		-- Загрузка данных пассивных счетов в витрину dm_account_balance_f
		IF account_rk_record.char_type = 'П' THEN
		INSERT INTO dm.dm_account_balance_f(on_date, account_rk, balance_out, balance_out_rub)
		VALUES(i_OnDate,
				account_rk_record.account_rk,
				COALESCE(account_rk_record.balance_prev, 0) - v_debet_amount + v_credit_amount, 
				(COALESCE(account_rk_record.balance_prev, 0) - v_debet_amount + v_credit_amount) * COALESCE(v_reduced_cource, 1)
				);
		END IF;
		v_count_rec_row = v_count_rec_row + 1;
	END LOOP;

	-- Логирование завершения загрузки данных
	UPDATE logs.loading_logs
		SET count_rec_row = v_count_rec_row,
			end_calculation = CLOCK_TIMESTAMP(),
			log_file = 'Данные успешно загружены'
		WHERE log_id = v_log_id;
EXCEPTION 
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS
			v_error_text := MESSAGE_TEXT;
			RAISE EXCEPTION 'Произошла ошибка загрузки данных в витрину: %', v_error_text;
END;
$$ LANGUAGE plpgsql


-- Расчет витрины остатков за месяц 
DO $$
DECLARE
	OnDate DATE := '2018-01-01'::DATE;
BEGIN
    FOR i IN 0..30 LOOP
        CALL ds.fill_account_balance_f(OnDate + i);
    END LOOP;
END $$;
