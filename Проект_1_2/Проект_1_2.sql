CREATE SCHEMA IF NOT EXISTS dm;

-- Создается таблица loading_logs в схеме logs
DROP TABLE IF EXISTS logs.loading_logs;
CREATE TABLE logs.loading_logs(
	settlement_date DATE,
	procedure_name VARCHAR(70),
	operation VARCHAR(100),
    start_calculation TIMESTAMPTZ,
	end_calculation TIMESTAMPTZ,
	log_file TEXT,
	CONSTRAINT pk_logs PRIMARY KEY (start_calculation, procedure_name)
);

-- Создается таблица dm_account_turnover_f в схеме DM
DROP TABLE IF EXISTS dm.dm_account_turnover_f;
CREATE TABLE dm.dm_account_turnover_f(
	on_date DATE,
	account_rk BIGINT,
	credit_amount NUMERIC(23,8),
	credit_amount_rub NUMERIC(23,8),
	debet_amount NUMERIC(23,8),
	debet_amount_rub NUMERIC(23,8)
);

-- Создается таблица dm_account_balance_f в схеме DM
DROP TABLE IF EXISTS dm.dm_account_balance_f;
CREATE TABLE dm.dm_account_balance_f(
	on_date DATE,
    account_rk BIGINT,
	balance_out NUMERIC(23,8),
	balance_out_rub NUMERIC(23,8)
);


-- Логирование начала загрузки данных
CREATE OR REPLACE PROCEDURE logs.start_logs(p_settlement_date DATE, p_procedure_name VARCHAR(70), p_operation VARCHAR(100), p_start_calculation TIMESTAMPTZ) AS $$
BEGIN
	INSERT INTO logs.loading_logs(settlement_date, procedure_name, operation, start_calculation)
	VALUES(p_settlement_date, p_procedure_name, p_operation, p_start_calculation);
END;
$$ LANGUAGE plpgsql;



-- Логирование завершения загрузки данных
CREATE OR REPLACE PROCEDURE logs.end_logs(p_procedure_name VARCHAR(70), p_start_calculation TIMESTAMPTZ, p_end_calculation TIMESTAMPTZ, p_log_file TEXT) AS $$
BEGIN
	INSERT INTO logs.loading_logs(procedure_name, start_calculation, end_calculation, log_file)
	VALUES(p_procedure_name, p_start_calculation, p_end_calculation, p_log_file)
	ON CONFLICT (start_calculation, procedure_name)
	DO UPDATE SET 
	end_calculation = EXCLUDED.end_calculation,
	log_file = EXCLUDED.log_file;
END;
$$ LANGUAGE plpgsql;


--Индекс по дате ds.ft_posting_f
CREATE INDEX IF NOT EXISTS idx_ft_posting_f_oper_date ON ds.ft_posting_f(oper_date);


-- Загрузка данных в витрину dm_account_turnover_f
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE) AS $$
DECLARE
	v_currency_rk INTEGER;
	v_account_rk INTEGER;
    v_reduced_cource NUMERIC(12,8);
	credit_amount_record RECORD;
	debet_amount_record RECORD;
	v_error_text text;
	account_rk_cursor CURSOR FOR 
		WITH account_rk_record AS (SELECT DISTINCT credit_account_rk AS account_rk
		FROM ds.ft_posting_f
		WHERE oper_date = i_OnDate
		UNION
		SELECT DISTINCT debet_account_rk AS account_rk
		FROM ds.ft_posting_f
		WHERE oper_date = i_OnDate
)
SELECT DISTINCT account_rk_record.account_rk AS account_rk,
	er.reduced_cource AS reduced_cource
		FROM account_rk_record
		LEFT JOIN ds.md_account_d acc ON account_rk_record.account_rk = acc.account_rk
		LEFT JOIN ds.md_exchange_rate_d er ON acc.currency_rk = er.currency_rk
			AND er.data_actual_date <= i_OnDate
			AND er.data_actual_end_date >= i_OnDate;
BEGIN

	DELETE FROM dm.dm_account_turnover_f
	WHERE on_date = i_OnDate;

	-- Трансформация данных для каждого счета с транзакциями за дату
	OPEN account_rk_cursor;
	LOOP
		FETCH account_rk_cursor INTO v_account_rk, v_reduced_cource;
		EXIT WHEN NOT FOUND;


		-- Получение значений по кредиту счета
		SELECT SUM(credit_amount)::NUMERIC(23,8) credit_amount,
			(SUM(credit_amount) * COALESCE(v_reduced_cource, 1))::NUMERIC(23,8) credit_amount_rub
		INTO credit_amount_record
		FROM ds.ft_posting_f p
		WHERE p.oper_date = i_OnDate 
		AND p.credit_account_rk = v_account_rk;
		-- Получение значений по дебету счета
		SELECT SUM(debet_amount)::NUMERIC(23,8) debet_amount,
			(SUM(debet_amount) * COALESCE(v_reduced_cource, 1))::NUMERIC(23,8) debet_amount_rub
		INTO debet_amount_record
		FROM ds.ft_posting_f p
		WHERE p.oper_date = i_OnDate 
		AND p.debet_account_rk = v_account_rk;

		-- Загрузка данных в витрину dm_account_turnover_f
		INSERT INTO dm.dm_account_turnover_f(on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
		VALUES(i_OnDate, 
		v_account_rk, 
		credit_amount_record.credit_amount, 
		credit_amount_record.credit_amount_rub,
		debet_amount_record.debet_amount,
		debet_amount_record.debet_amount_rub
		);
	
	END LOOP;
	CLOSE account_rk_cursor;

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
	start_calculation TIMESTAMPTZ;
BEGIN
    FOR i IN 0..30 LOOP
		start_calculation = CLOCK_TIMESTAMP();
		CALL logs.start_logs(OnDate+i, 'ds.fill_account_turnover_f', 'Загрузка витрины оборотов', start_calculation);
        CALL ds.fill_account_turnover_f(OnDate+i);
		CALL logs.end_logs('ds.fill_account_turnover_f', start_calculation,  CLOCK_TIMESTAMP(), 'Данные успешно загружены');
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
	start_calculation TIMESTAMPTZ;
BEGIN
    FOR i IN 0..30 LOOP
		start_calculation = CLOCK_TIMESTAMP();
		CALL logs.start_logs(OnDate+i, 'ds.fill_account_balance_f', 'Загрузка витрины остатков', start_calculation);
        CALL ds.fill_account_balance_f(OnDate + i);
		CALL logs.end_logs('ds.fill_account_balance_f', start_calculation,  CLOCK_TIMESTAMP(), 'Данные успешно загружены');
    END LOOP;
END $$;
