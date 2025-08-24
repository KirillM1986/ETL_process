INSERT INTO ds.md_account_d(
      data_actual_date
    , data_actual_end_date
    , account_rk
    , account_number
	, char_type
	, currency_rk
	, currency_code
)
SELECT TO_DATE(mad."DATA_ACTUAL_DATE", 'YYYY-MM-DD')
	 , TO_DATE(mad."DATA_ACTUAL_END_DATE", 'YYYY-MM-DD')
	 , mad."ACCOUNT_RK"
	 , mad."ACCOUNT_NUMBER"
     , mad."CHAR_TYPE" 
     , mad."CURRENCY_RK" 
     , mad."CURRENCY_CODE"
  FROM stage.md_account_d mad
 WHERE mad."DATA_ACTUAL_DATE"  IS NOT NULL
   AND mad."ACCOUNT_RK"  IS NOT NULL
ON CONFLICT (data_actual_date, account_rk)
DO UPDATE SET 
	data_actual_end_date = EXCLUDED.data_actual_end_date,
    account_number = EXCLUDED.account_number,
	char_type = EXCLUDED.char_type,
	currency_rk = EXCLUDED.currency_rk,
	currency_code = EXCLUDED.currency_code; 