--CREATE DATABASE finance;
CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS transform;

set search_path = 'stage';
DROP TABLE IF EXISTS stage.account_processing;
CREATE TABLE IF NOT EXISTS stage.account_processing (
    prefix varchar(50) PRIMARY KEY,
    bank varchar(50) NOT NULL,
    account_name varchar(50) NOT NULL,
    account_processor varchar(50) NOT NULL,
    columns varchar(500)
);

insert into stage.account_processing values ('Chase7226','Chase','Chase Sapphire','process_chase_cc.py', 'line_no, transaction_date, posting_date, src_description, category, src_type, amount, memo');
insert into stage.account_processing values ('Chase5074','Chase','Chase Marriott','process_chase_cc.py', 'line_no, transaction_date, posting_date, src_description, category, src_type, amount, memo');
insert into stage.account_processing values ('Chase2085','Chase','Chase Hyatt','process_chase_cc.py', 'line_no, transaction_date, posting_date, src_description, category, src_type, amount, memo');
insert into stage.account_processing values ('Chase3770','Chase','Chase Checking','process_chase_banking.py', 'line_no, details, posting_date, src_description, amount, src_type, balance, check_no_slip_no, filler');
insert into stage.account_processing values ('Chase3894','Chase','Chase Emergency','process_chase_banking.py', 'line_no, details, posting_date, src_description, amount, src_type, balance, check_no_slip_no, filler');

set search_path = 'ingest';
DROP TABLE IF EXISTS ingest.chase7226;
CREATE TABLE IF NOT EXISTS ingest.chase7226 (
    line_no integer, 
    transaction_date varchar(100),
    posting_date varchar(100),
    src_description varchar(100),
    category varchar(100),
    src_type varchar(100),
    amount varchar(100),
    memo varchar(100)
);

DROP TABLE IF EXISTS ingest.chase5074;
CREATE TABLE IF NOT EXISTS ingest.chase5074 (
    line_no integer, 
    transaction_date varchar(100),
    posting_date varchar(100),
    src_description varchar(100),
    category varchar(100),
    src_type varchar(100),
    amount varchar(100),
    memo varchar(100)
);

DROP TABLE IF EXISTS ingest.chase2085;
CREATE TABLE IF NOT EXISTS ingest.chase2085 (
    line_no integer, 
    transaction_date varchar(100),
    posting_date varchar(100),
    src_description varchar(100),
    category varchar(100),
    src_type varchar(100),
    amount varchar(100),
    memo varchar(100)
);

DROP TABLE IF EXISTS ingest.chase3894;
CREATE TABLE IF NOT EXISTS ingest.chase3894 (
    line_no integer, 
    details varchar(100),
    posting_date varchar(100),
    src_description varchar(100),
    amount varchar(100),
    src_type varchar(100),
    balance varchar(100),
    check_no_slip_no varchar(100),
    filler varchar(20)
);

DROP TABLE IF EXISTS ingest.chase3770;
CREATE TABLE IF NOT EXISTS ingest.chase3770 (
    line_no integer, 
    details varchar(100),
    posting_date varchar(100),
    src_description varchar(100),
    amount varchar(100),
    src_type varchar(100),
    balance varchar(100),
    check_no_slip_no varchar(100),
    filler varchar(20)
);