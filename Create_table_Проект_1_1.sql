CREATE SCHEMA IF NOT EXISTS ds;
CREATE SCHEMA IF NOT EXISTS logs;


CREATE TABLE IF NOT EXISTS LOGS.ETL_LOGS(
	log_id SERIAL PRIMARY KEY,
	start_reader TIMESTAMP,
	start_loading_time TIMESTAMP,
	end_loading_time TIMESTAMP,
	file_name VARCHAR(30),
	log_file TEXT
);


CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F(
    on_date DATE NOT NULL,
    account_rk BIGINT NOT NULL,
    currency_rk BIGINT,
    balance_out DOUBLE PRECISION,
    CONSTRAINT pk_ft_balance_f PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
	oper_date DATE NOT NULL,
	credit_account_rk BIGINT NOT NULL,
	debet_account_rk BIGINT NOT NULL,
	credit_amount DOUBLE PRECISION,
	debet_amount DOUBLE PRECISION
);


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


CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
	currency_rk BIGINT NOT NULL,
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_code VARCHAR(3),
	code_iso_char VARCHAR(3),
	CONSTRAINT pk_md_currency_d PRIMARY KEY (currency_rk, data_actual_date)
);


CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_rk BIGINT NOT NULL,
	reduced_cource DOUBLE PRECISION,
	code_iso_num VARCHAR(3),
	CONSTRAINT pk_md_exchange_rate_d PRIMARY KEY (data_actual_date, currency_rk)
);


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
