DO $$
DECLARE
	start_time TIMESTAMPTZ = CLOCK_TIMESTAMP();
BEGIN
	CALL logs.start_logs(NULL, 'ds.ft_balance_f', 'Загрузка данных в слой ds', start_time);

	INSERT INTO ds.ft_balance_f(
      	account_rk
    	, currency_rk
    	, balance_out
    	, on_date
		)
	SELECT fbf."ACCOUNT_RK" 
     	, fbf."CURRENCY_RK" 
     	, fbf."BALANCE_OUT" 
     	, TO_DATE(fbf."ON_DATE" , 'DD.MM.YYYY') AS on_date
  	FROM stage.ft_balance_f fbf
 	WHERE fbf."ACCOUNT_RK"  IS NOT NULL
   	AND fbf."CURRENCY_RK"  IS NOT NULL
	ON CONFLICT (on_date, account_rk)
	DO UPDATE SET 
    		currency_rk = EXCLUDED.currency_rk, 
    		balance_out = EXCLUDED.balance_out; 

	CALL logs.end_logs('ds.ft_balance_f', start_time,  CLOCK_TIMESTAMP(), 'Данные успешно загружены');
END $$;