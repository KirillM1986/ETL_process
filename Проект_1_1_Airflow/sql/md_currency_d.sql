INSERT INTO ds.md_currency_d(
	  currency_rk
    , data_actual_date
    , data_actual_end_date
	, currency_code
	, code_iso_char
)
SELECT mcd."CURRENCY_RK" 
	 , TO_DATE(mcd."DATA_ACTUAL_DATE", 'YYYY-MM-DD')
	 , TO_DATE(mcd."DATA_ACTUAL_END_DATE", 'YYYY-MM-DD') 
     , mcd."CURRENCY_CODE"
	 , mcd."CODE_ISO_CHAR"
  FROM stage.md_currency_d mcd
 WHERE mcd."DATA_ACTUAL_DATE"  IS NOT NULL
   AND mcd."CURRENCY_RK"  IS NOT NULL
ON CONFLICT (currency_rk, data_actual_date)
DO UPDATE SET 
	data_actual_end_date = EXCLUDED.data_actual_end_date,
	currency_code = EXCLUDED.currency_code,
	code_iso_char = EXCLUDED.code_iso_char; 