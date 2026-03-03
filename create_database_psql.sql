--CREATE DATABASE finance;
CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS transform;

set search_path = 'stage';
DROP TABLE IF EXISTS stage.account_processing;
CREATE TABLE IF NOT EXISTS stage.account_processing (
    suffix varchar(50) PRIMARY KEY,
    bank varchar(50) NOT NULL,
    account_name varchar(50) NOT NULL,
    account_processor varchar(50) NOT NULL
);

insert into stage.account_processing values ('Chase7226','Chase','Chase Sapphire','process_chase_cc.py');
insert into stage.account_processing values ('Chase5074','Chase','Chase Marriott','process_chase_cc.py');
insert into stage.account_processing values ('Chase2085','Chase','Chase Hyatt','process_chase_cc.py');
insert into stage.account_processing values ('Chase3770','Chase','Chase Checking','process_chase_banking.py');
insert into stage.account_processing values ('Chase3894','Chase','Chase Emergency','process_chase_banking.py');

set search_path = 'ingest';
DROP TABLE IF EXISTS ingest.chase7226;
CREATE TABLE IF NOT EXISTS ingest.chase7226 (
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
    details varchar(100),
    posting_date varchar(100),
    src_description varchar(100),
    amount varchar(100),
    src_type varchar(100),
    balance varchar(100),
    check_no_slip_no varchar(100)
);

DROP TABLE IF EXISTS ingest.chase3770;
CREATE TABLE IF NOT EXISTS ingest.chase3770 (
    details varchar(100),
    posting_date varchar(100),
    src_description varchar(100),
    amount varchar(100),
    src_type varchar(100),
    balance varchar(100),
    check_no_slip_no varchar(100)
);