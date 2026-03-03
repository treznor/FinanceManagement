import os
import sys
from os.path import isfile, join
import argparse
import csv
import time
from query import *

def filename_creation():
    # create argument parser to allow file names to be passed in
    parser = argparse.ArgumentParser(
    prog = 'process_transactions',
    description = 'Process transactions'
    )
    parser.add_argument('filename')
    args = parser.parse_args()
    main(args.filename)


def main(ingest_file):
    # starting timer
    start_time = time.time()

    # create file paths based on file name passed in
    file_split = ingest_file.split("/")
    dir_path = file_split[0]
    file_prefix = file_split[1][:9]
    processed_dir_path = dir_path + '/processed/' + file_split[1]

    # get the ingestion columns for the file prefix
    query = f"select columns from stage.account_processing where prefix = '{file_prefix}'"
    columns = querySelectOne(query)
    if columns == '':
        raise Exception("File type not found or columns not defined for file type")

    # read data in from the file to be loaded
    file_records = 0

    with open(ingest_file, 'r', encoding = 'utf-8') as f:
        next(f)
        reader = csv.reader(f, delimiter = ',')
        data_to_insert = []
        for line in reader:
            if line == '':
                continue
            line.insert(0, file_records + 1)
            data_to_insert.append(tuple(line))
            file_records += 1

    # load data from file into the ingest table
    copy_query = f"COPY ingest.{file_prefix} ({columns}) from STDIN"
    delete_query = f"DELETE from ingest.{file_prefix}"

    queryWrite(copy_query, data_to_insert, delete_query)

    # count the number of records loaded
    query = f"select count(*) from ingest.{file_prefix}"
    loaded_records = querySelectOne(query)

    # if the number of records in the file is different than the number of records loaded, there were missed records somewhere
    if not loaded_records == file_records:
        raise Exception(f"Number of records are different between loaded file {ingest_file} ({file_records} records) and ingest table ingest.{file_prefix} ({loaded_records} records)")

    # move the file to the processed area
    os.rename(ingest_file, processed_dir_path)

    # determine the elapsed time and records per second
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    records_per_second = round(file_records / elapsed_time)

    # output a summary
    print(f"Processed {ingest_file}. {file_records} records ingested in {elapsed_time} seconds, {records_per_second} records per second")

if __name__ == "__main__":
    filename_creation()