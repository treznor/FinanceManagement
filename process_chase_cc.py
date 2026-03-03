import os
from os.path import isfile, join
import psycopg
import argparse

file_name = 'process_chase_cc.py'

parser = argparse.ArgumentParser(
    prog = 'process_chase_cc',
    description = 'Process Chase credit card transactions'
)

parser.add_argument('filename')

args = parser.parse_args()

file_split = args.filename.split("/")
dir_path = file_split[0]
file_suffix = file_split[1][:9]

query = f"select account_processor from stage.account_processing where suffix = '{file_suffix}'"

with psycopg.connect("dbname=finance user=treznor") as conn:
    with conn.cursor() as cur:
        cur.execute(query)

        for result in cur.fetchone():
            account_processor = result

if account_processor != file_name:
    raise Exception("Wrong processor for file suffix")

file_records = 0

with open(args.filename, 'r', encoding = 'utf-8') as f:
    next(f)
    content = f.read()
    content_lines = content.split("\n")
    
    data_to_insert = []
    for line in content_lines:
        if line == '':
            continue
        data_to_insert.append(tuple(line.split(',')))
        file_records += 1

copy_query = f"COPY ingest.{file_suffix} (transaction_date, posting_date, src_description, category, src_type, amount, memo) from STDIN"
delete_query = f"DELETE from ingest.{file_suffix}"

with psycopg.connect("dbname=finance user=treznor") as conn:
    with conn.cursor() as cur:
        cur.execute(delete_query)
    with conn.cursor() as cur:
        with cur.copy(copy_query) as copy:
            for record in data_to_insert:
                copy.write_row(record)
    conn.commit()

query = f"select count(*) from ingest.{file_suffix}"

with psycopg.connect("dbname=finance user=treznor") as conn:
    with conn.cursor() as cur:
        cur.execute(query)

        for result in cur.fetchone():
            loaded_records = result

if not loaded_records == file_records:
    raise Exception(f"Number of records are different between loaded file {args.filename} ({file_records} records) and ingest table ingest.{file_suffix} ({loaded_records} records)")