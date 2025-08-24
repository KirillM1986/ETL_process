-- Создается таблица DM_F101_ROUND_F в схеме DM
DROP TABLE IF EXISTS DM.DM_F101_ROUND_F;
CREATE TABLE IF NOT EXISTS DM.DM_F101_ROUND_F(
	FROM_DATE DATE,
	TO_DATE DATE,
	CHAPTER CHAR(1),
	LEDGER_ACCOUNT CHAR(5),
	CHARACTERISTIC CHAR(1),
	BALANCE_IN_RUB NUMERIC(23,8),
	BALANCE_IN_VAL NUMERIC(23,8),
	BALANCE_IN_TOTAL NUMERIC(23,8),
	TURN_DEB_RUB NUMERIC(23,8),
	TURN_DEB_VAL NUMERIC(23,8),
	TURN_DEB_TOTAL NUMERIC(23,8),
	TURN_CRE_RUB NUMERIC(23,8),
	TURN_CRE_VAL NUMERIC(23,8),
	TURN_CRE_TOTAL NUMERIC(23,8),
	BALANCE_OUT_RUB NUMERIC(23,8),
	BALANCE_OUT_VAL NUMERIC(23,8),
	BALANCE_OUT_TOTAL NUMERIC(23,8)
);


-- Загрузка данных в витрину DM_F101_ROUND_F
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE) AS $$
DECLARE
	v_log_id INTEGER;
	v_count_rec_row BIGINT := 0;
	v_from_date DATE :=  ((DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 MONTH'))::DATE;--DATE_TRUNC('month', i_OnDate)::DATE;
	v_to_date DATE := i_OnDate - 1; --((DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 MONTH'))::DATE;
	v_error_text TEXT;
BEGIN
	-- Логирование начала загрузки данных
	INSERT INTO logs.loading_logs(settlement_date, name_data_marts, start_calculation)
	VALUES(i_OnDate, 'DM_F101_ROUND_F', CLOCK_TIMESTAMP())
	RETURNING log_id INTO v_log_id;

	-- Очистка таблицы по дате(отчетному периоду)
	DELETE FROM DM.DM_F101_ROUND_F
	WHERE EXTRACT(YEAR FROM FROM_DATE) = EXTRACT(YEAR FROM v_from_date)
	AND EXTRACT(MONTH FROM TO_DATE) = EXTRACT(MONTH FROM v_to_date);

	-- Загрузка данных в витрину DM_F101_ROUND_F
	INSERT INTO DM.DM_F101_ROUND_F(
		FROM_DATE,
		TO_DATE,
		CHAPTER,
		LEDGER_ACCOUNT,
		CHARACTERISTIC,
		BALANCE_IN_RUB,
		BALANCE_IN_VAL,
		BALANCE_IN_TOTAL,
		TURN_DEB_RUB,
		TURN_DEB_VAL,
		TURN_DEB_TOTAL,
		TURN_CRE_RUB,
		TURN_CRE_VAL,
		TURN_CRE_TOTAL,
		BALANCE_OUT_RUB,
		BALANCE_OUT_VAL,
		BALANCE_OUT_TOTAL
	)
	SELECT v_from_date FROM_DATE,
			v_to_date TO_DATE,
			lacc.chapter CHAPTER, 
			lacc.ledger_account LEDGER_ACCOUNT,
			acc.char_type  CHARACTERISTIC,
			BALANCE_IN_RUB,
			BALANCE_IN_VAL,
			BALANCE_IN_TOTAL,
			COALESCE(SUM(CASE 
				WHEN acc.currency_code IN ('810', '643') THEN acct.debet_amount_rub
				ELSE 0
			END), 0) TURN_DEB_RUB,
			COALESCE(SUM(CASE 
				WHEN acc.currency_code NOT IN ('810', '643') THEN acct.debet_amount_rub
				ELSE 0
			END), 0) TURN_DEB_VAL,
			COALESCE(SUM(acct.debet_amount_rub), 0) TURN_DEB_TOTAL, 
			COALESCE(SUM(CASE 
				WHEN acc.currency_code IN ('810', '643') THEN acct.credit_amount_rub
				ELSE 0
			END), 0) TURN_CRE_RUB,
			COALESCE(SUM(CASE 
				WHEN acc.currency_code NOT IN ('810', '643') THEN acct.credit_amount_rub
				ELSE 0
			END), 0) TURN_CRE_VAL,
			COALESCE(SUM(acct.credit_amount_rub), 0) TURN_CRE_TOTAL,
			BALANCE_OUT_RUB,
			BALANCE_OUT_VAL,
			BALANCE_OUT_TOTAL
	FROM ds.md_ledger_account_s lacc
	LEFT JOIN ds.md_account_d acc ON CAST(lacc.ledger_account AS VARCHAR(5)) = LEFT(acc.account_number, 5)
	LEFT JOIN dm.dm_account_turnover_f acct ON acc.account_rk = acct.account_rk
	LEFT JOIN (SELECT
		lacc.ledger_account,
		COALESCE(SUM(CASE 
			WHEN acc.currency_code IN ('810', '643') AND on_date = v_from_date - 1 THEN balance_out_rub
			ELSE 0
		END), 0) BALANCE_IN_RUB,
		COALESCE(SUM(CASE 
			WHEN acc.currency_code NOT IN ('810', '643') AND on_date = v_from_date - 1 THEN balance_out_rub
			ELSE 0
		END), 0) BALANCE_IN_VAL,
		COALESCE(SUM(CASE 
			WHEN on_date = v_from_date - 1 THEN balance_out_rub
			ELSE 0
		END), 0) BALANCE_IN_TOTAL,
		COALESCE(SUM(CASE 
			WHEN acc.currency_code IN ('810', '643') AND on_date = v_to_date THEN balance_out_rub
			ELSE 0
		END), 0) BALANCE_OUT_RUB,
		COALESCE(SUM(CASE 
			WHEN acc.currency_code NOT IN ('810', '643') AND on_date = v_to_date THEN balance_out_rub
			ELSE 0
		END), 0) BALANCE_OUT_VAL,
		COALESCE(SUM(CASE 
			WHEN on_date = v_to_date THEN balance_out_rub
			ELSE 0
		END), 0) BALANCE_OUT_TOTAL
		FROM ds.md_ledger_account_s lacc
		LEFT JOIN ds.md_account_d acc ON CAST(lacc.ledger_account AS VARCHAR(5)) = LEFT(acc.account_number, 5)
		LEFT JOIN dm.dm_account_balance_f accb ON acc.account_rk = accb.account_rk
		GROUP BY ledger_account, lacc.chapter, char_type) a ON a.ledger_account = lacc.ledger_account
	GROUP BY lacc.ledger_account, 
			lacc.chapter, char_type, 
			BALANCE_IN_RUB, 
			BALANCE_IN_VAL, 
			BALANCE_IN_TOTAL,
			BALANCE_OUT_RUB,
			BALANCE_OUT_VAL,
			BALANCE_OUT_TOTAL;
	
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


-- Вызов процедуры dm.fill_f101_round_f
DO $$
BEGIN
    CALL dm.fill_f101_round_f('2018-02-01'::DATE);
END $$;

