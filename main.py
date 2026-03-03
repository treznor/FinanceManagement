import os
from os.path import isfile, join
import psycopg

directory_path = './transactions'

files_to_process = [f for f in os.listdir(directory_path) if isfile(join(directory_path, f))]

for file in files_to_process:
    print(file)

with psycopg.connect("dbname=finance user=treznor") as conn:
    with conn.cursor() as cur:
        cur.execute("select * from stage.account_processing")

        for db in cur.fetchall():
            print(db)