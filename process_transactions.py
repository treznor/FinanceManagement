import time
from query import *

def main():
    # starting timer
    start_time = time.time()

    query = "SELECT prefix from stage.account_processing"

    tables_to_process = querySelectMany(query)

    for table in tables_to_process:
        print(table)

    # determine the elapsed time and records per second
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
#    records_per_second = round(file_records / elapsed_time)

    # output a summary
#    print(f"Processed {ingest_file}. {file_records} records ingested in {elapsed_time} seconds, {records_per_second} records per second")

if __name__ == "__main__":
    main()