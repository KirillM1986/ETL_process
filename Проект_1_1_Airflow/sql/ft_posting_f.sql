-- ds.ft_posting_f
DO $$
DECLARE
	start_time TIMESTAMPTZ = CLOCK_TIMESTAMP();
BEGIN
	CALL logs.start_logs(NULL, 'ds.ft_posting_f', 'Загрузка данных в слой ds', start_time);
	
 	INSERT INTO ds.ft_posting_f(
      		credit_account_rk
    		, debet_account_rk
    		, credit_amount
    		, debet_amount
    		, oper_date
	)
	SELECT fpf."CREDIT_ACCOUNT_RK"
    		, fpf."DEBET_ACCOUNT_RK" 
    		, fpf."CREDIT_AMOUNT" 
    		, fpf."DEBET_AMOUNT" 
    		, TO_DATE(fpf."OPER_DATE", 'DD-MM-YYYY') oper_date
  	FROM stage.ft_posting_f fpf
	WHERE fpf."CREDIT_ACCOUNT_RK" IS NOT NULL
   	AND fpf."DEBET_ACCOUNT_RK" IS NOT NULL; 

	CALL logs.end_logs('ds.ft_posting_f', start_time,  CLOCK_TIMESTAMP(), 'Данные успешно загружены');
END $$;