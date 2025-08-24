
-- Создание схем
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS ds;
CREATE SCHEMA IF NOT EXISTS logs;



-- Создание таблиц
DROP TABLE IF EXISTS LOGS.ETL_LOGS;
CREATE TABLE IF NOT EXISTS LOGS.ETL_LOGS(
	log_id SERIAL PRIMARY KEY,
	dag_name VARCHAR(50),
	start_loading_time TIMESTAMP,
	end_loading_time TIMESTAMP,
	log_file TEXT
);


DROP TABLE IF EXISTS DS.FT_BALANCE_F;
CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F(
    on_date DATE NOT NULL,
    account_rk BIGINT NOT NULL,
    currency_rk BIGINT,
    balance_out NUMERIC(10,2),
    CONSTRAINT pk_ft_balance_f PRIMARY KEY (on_date, account_rk)
);


DROP TABLE IF EXISTS DS.FT_POSTING_F;
CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
	oper_date DATE NOT NULL,
	credit_account_rk BIGINT NOT NULL,
	debet_account_rk BIGINT NOT NULL,
	credit_amount NUMERIC(10,2),
	debet_amount NUMERIC(10,2)
);


DROP TABLE IF EXISTS DS.MD_ACCOUNT_D;
CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE NOT NULL,
	account_rk BIGINT NOT NULL,
	account_number VARCHAR(20) NOT NULL,
	char_type VARCHAR(1) NOT NULL,
	currency_rk BIGINT NOT NULL,
	currency_code VARCHAR(3) NOT NULL,
	CONSTRAINT pk_md_account_d PRIMARY KEY (data_actual_date, account_rk)
);


DROP TABLE IF EXISTS DS.MD_CURRENCY_D;
CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
	currency_rk BIGINT NOT NULL,
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_code VARCHAR(3),
	code_iso_char VARCHAR(3),
	CONSTRAINT pk_md_currency_d PRIMARY KEY (currency_rk, data_actual_date)
);


DROP TABLE IF EXISTS DS.MD_EXCHANGE_RATE_D;
CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_rk BIGINT NOT NULL,
	reduced_cource NUMERIC(10,8),
	code_iso_num VARCHAR(3),
	CONSTRAINT pk_md_exchange_rate_d PRIMARY KEY (data_actual_date, currency_rk)
);


DROP TABLE IF EXISTS DS.MD_LEDGER_ACCOUNT_S;
CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
	chapter CHAR(1),
	chapter_name VARCHAR(16),
	section_number INTEGER,
	section_name VARCHAR(22),
	subsection_name VARCHAR(21),
	ledger1_account INTEGER,
	ledger1_account_name VARCHAR(47),
	ledger_account INTEGER NOT NULL,
	ledger_account_name VARCHAR(153),
	characteristic CHAR(1),
	is_resident INTEGER,
	is_reserve INTEGER,
	is_reserved INTEGER,
	is_loan INTEGER,
	is_reserved_assets INTEGER,
	is_overdue INTEGER,
	is_interest INTEGER,
	pair_account VARCHAR(5),
	start_date DATE NOT NULL,
	end_date DATE,
	is_rub_only INTEGER,
	min_term VARCHAR(1),
	min_term_measure VARCHAR(1),
	max_term VARCHAR(1),
	max_term_measure VARCHAR(1),
	ledger_acc_full_name_translit VARCHAR(1),
	is_revaluation VARCHAR(1),
	is_correct VARCHAR(1),
	CONSTRAINT pk_md_ledger_account_s PRIMARY KEY (ledger_account, start_date)
);



-- Удалние (очистка) таблиц stage перед загрузкой данных
DROP TABLE IF EXISTS stage.ft_balance_f;
DROP TABLE IF EXISTS stage.ft_posting_f;
DROP TABLE IF EXISTS stage.md_account_d;
DROP TABLE IF EXISTS stage.md_currency_d;
DROP TABLE IF EXISTS stage.md_exchange_rate_d;
DROP TABLE IF EXISTS stage.md_ledger_account_s;




-- Скрипты формирования слоя ds
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


INSERT INTO ds.md_exchange_rate_d(
	  data_actual_date
    , data_actual_end_date
	, currency_rk
	, reduced_cource
	, code_iso_num
)
SELECT  DISTINCT TO_DATE(merd."DATA_ACTUAL_DATE", 'YYYY-MM-DD')
	 , TO_DATE(merd."DATA_ACTUAL_END_DATE", 'YYYY-MM-DD') 
	 , merd."CURRENCY_RK" 
     , merd."REDUCED_COURCE"
	 , merd."CODE_ISO_NUM"
  FROM stage.md_exchange_rate_d merd
 WHERE merd."DATA_ACTUAL_DATE"  IS NOT NULL
   AND merd."CURRENCY_RK"  IS NOT NULL
ON CONFLICT (data_actual_date, currency_rk)
DO UPDATE SET 
	data_actual_end_date = EXCLUDED.data_actual_end_date,
	reduced_cource = EXCLUDED.reduced_cource,
	code_iso_num = EXCLUDED.code_iso_num; 


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

