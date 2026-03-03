import psycopg

#--------------------------------------------------------------------------------------------------
# input
# write_query (string): query that insert data into the database
# data_to_write (list): data to be written
# delete_query (string, optional): query that deletes data from the database prior to insertion
#
# return
# No return
# TO DO: Return True if there are no errors, return False for an error
#--------------------------------------------------------------------------------------------------

def queryWrite(write_query, data_to_write, delete_query = ''):

    with psycopg.connect("dbname=finance user=treznor") as conn:
        if not delete_query == '':
            with conn.cursor() as cur:
                cur.execute(delete_query)
        with conn.cursor() as cur:
            with cur.copy(write_query) as copy:
                for record in data_to_write:
                    copy.write_row(record)
        conn.commit()

#--------------------------------------------------------------------------------------------------
# input
# query (string): query to be executed in the database. Should produce a single value (select count(*), sum, etc)
#
# return
# output - A single record and value
#--------------------------------------------------------------------------------------------------

def querySelectOne(query):
    with psycopg.connect("dbname=finance user=treznor") as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            for result in cur.fetchone():
                output = result
    return output

#--------------------------------------------------------------------------------------------------
# input
# query (string): query to be executed in the database. Should produce multiple records. If a single record is produced, use querySelectOne
#
# return
# output - A list of records to return
#--------------------------------------------------------------------------------------------------

def querySelectMany(query):
    output = []
    with psycopg.connect("dbname=finance user=treznor") as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            for result in cur.fetchall():
                output.append(result)
    return output