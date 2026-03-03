import os
from os.path import isfile, join
import ingest_transactions
import process_transactions
from query import *

directory_path = './transactions'

files_to_process = [f for f in os.listdir(directory_path) if isfile(join(directory_path, f))]

for file in files_to_process:
    ingest_file_path = 'transactions/' + file
    ingest_transactions.main(ingest_file_path)

process_transactions.main()