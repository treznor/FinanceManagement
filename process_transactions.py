import os
from os.path import isfile, join
import psycopg
import argparse
import csv
import time

start_time = time.time()

file_name = 'process_chase_cc.py'

parser = argparse.ArgumentParser(
    prog = 'process_chase_cc',
    description = 'Process transactions'
)

parser.add_argument('filename')

args = parser.parse_args()

file_split = args.filename.split("/")
dir_path = file_split[0]
file_prefix = file_split[1][:9]
processed_dir_path = dir_path + '/processed/' + file_split[1]

query = f"select columns from stage.account_processing where prefix = '{file_prefix}'"
with psycopg.connect("dbname=finance user=treznor") as conn:
    with conn.cursor() as cur:
        cur.execute(query)

        for result in cur.fetchone():
            columns = result

if columns == '':
    raise Exception("File type not found or columns not defined for file type")

file_records = 0

# moving away from split as there are commas in the data
with open(args.filename, 'r', encoding = 'utf-8') as f:
    next(f)
    reader = csv.reader(f, delimiter = ',')
    
    data_to_insert = []
    for line in reader:
        if line == '':
            continue
        line.insert(0, file_records + 1)
        data_to_insert.append(tuple(line))
        file_records += 1


copy_query = f"COPY ingest.{file_prefix} ({columns}) from STDIN"
delete_query = f"DELETE from ingest.{file_prefix}"

with psycopg.connect("dbname=finance user=treznor") as conn:
    with conn.cursor() as cur:
        cur.execute(delete_query)
    with conn.cursor() as cur:
        with cur.copy(copy_query) as copy:
            for record in data_to_insert:
                copy.write_row(record)
    conn.commit()

query = f"select count(*) from ingest.{file_prefix}"

with psycopg.connect("dbname=finance user=treznor") as conn:
    with conn.cursor() as cur:
        cur.execute(query)

        for result in cur.fetchone():
            loaded_records = result

if not loaded_records == file_records:
    raise Exception(f"Number of records are different between loaded file {args.filename} ({file_records} records) and ingest table ingest.{file_prefix} ({loaded_records} records)")

os.rename(args.filename, processed_dir_path)
end_time = time.time()
elapsed_time = round(end_time - start_time, 3)
records_per_second = round(file_records / elapsed_time)

print(f"Processed {args.filename}. {file_records} records ingested in {elapsed_time} seconds, {records_per_second} records per second")