INSERT INTO ds.md_ledger_account_s(
	  chapter
	, chapter_name
	, section_number
	, section_name
	, subsection_name
	, ledger1_account
	, ledger1_account_name
	, ledger_account
	, ledger_account_name
	, characteristic
	--, is_resident
	--, is_reserve
	--, is_reserved
	--, is_loan
	--, is_reserved_assets
	--, is_overdue
	--, is_interest
	--, pair_account
	, start_date
	, end_date
	--, is_rub_only
	--, min_term
	--, min_term_measure
	--, max_term
	--, max_term_measure
	--, ledger_acc_full_name_translit
	--, is_revaluation
	--, is_correct

)
SELECT  COALESCE(mlas."CHAPTER", NULL)
	 , mlas."CHAPTER_NAME"
	 , mlas."SECTION_NUMBER"
	 , mlas."SECTION_NAME"
	 , mlas."SUBSECTION_NAME"
	 , mlas."LEDGER1_ACCOUNT"
	 , mlas."LEDGER1_ACCOUNT_NAME"
	 , mlas."LEDGER_ACCOUNT"
	 , mlas."LEDGER_ACCOUNT_NAME"
	 , mlas."CHARACTERISTIC"
	 --, mlas."IS_RESIDENT"
	 --, mlas."IS_RESERVE"
	 --, mlas."IS_RESERVED"
	 --, mlas."IS_LOAN"
	 --, mlas."IS_RESERVED_ASSETS"
	 --, mlas."IS_OVERDUE"
	 --, mlas."IS_INTEREST"
	 --, mlas."IS_PAIR_ACCOUNT"
	 , TO_DATE(mlas."START_DATE", 'YYYY-MM-DD')
	 , TO_DATE(mlas."END_DATE", 'YYYY-MM-DD') 
	 --, mlas."IS_RUB_ONLY" 
     --, mlas."MIN_TERM"
	 --, mlas."MIN_TERM_MEASURE"
	 --, mlas."MAX_TERM"
	 --, mlas."MAX_TERM_MEASURE"
	 --, mlas."LEDGER_ACC_FULL_NAME_TRANSLIT"
	 --, mlas."IS_REVALUATION"
	 --, mlas."IS_CORRECT"
  FROM stage.md_ledger_account_s mlas
 WHERE mlas."LEDGER_ACCOUNT"  IS NOT NULL
   AND mlas."START_DATE"  IS NOT NULL
ON CONFLICT (ledger_account, start_date)
DO UPDATE SET 
	chapter = EXCLUDED.chapter
	, chapter_name =EXCLUDED.chapter_name
	, section_number = EXCLUDED.section_number
	, section_name = EXCLUDED.section_name
	, subsection_name = EXCLUDED.subsection_name
	, ledger1_account = EXCLUDED.ledger1_account
	, ledger1_account_name = EXCLUDED.ledger1_account_name
	, ledger_account = EXCLUDED.ledger_account
	, ledger_account_name = EXCLUDED.ledger_account_name
	, characteristic = EXCLUDED.characteristic
	, is_resident = EXCLUDED.is_resident
	, is_reserve = EXCLUDED.is_reserve
	, is_reserved = EXCLUDED.is_reserved
	, is_loan = EXCLUDED.is_loan
	, is_reserved_assets = EXCLUDED.is_reserved_assets
	, is_overdue = EXCLUDED.is_overdue
	, is_interest = EXCLUDED.is_interest
	, pair_account = EXCLUDED.pair_account
	, start_date = EXCLUDED.start_date
	, end_date = EXCLUDED.end_date
	, is_rub_only = EXCLUDED.is_rub_only
	, min_term = EXCLUDED.min_term
	, min_term_measure = EXCLUDED.min_term_measure
	, max_term = EXCLUDED.max_term
	, max_term_measure = EXCLUDED.max_term_measure
	, ledger_acc_full_name_translit = EXCLUDED.ledger_acc_full_name_translit
	, is_revaluation = EXCLUDED.is_revaluation
	, is_correct = EXCLUDED.is_correct; 