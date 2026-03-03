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
    account_processer varchar(50) NOT NULL
);

insert into stage.account_processing values ('Chase7226','Chase','Chase Sapphire','Chase_CC');
insert into stage.account_processing values ('Chase5074','Chase','Chase Marriott','Chase_CC');
insert into stage.account_processing values ('Chase2085','Chase','Chase Hyatt','Chase_CC');
insert into stage.account_processing values ('Chase3770','Chase','Chase Checking','Chase_Banking');
insert into stage.account_processing values ('Chase3894','Chase','Chase Emergency','Chase_Banking');