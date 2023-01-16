import sqlite3
import pandas 

def connect_to_database(db_name):  #'mlops_db.db'
    connection_db = sqlite3.connect(db_name)
    cur = con.cursor()
    return cur 

def run_query(query,db_name):
    connection = connect_to_database(db_name)
    for row in connection.execute(query):
        print(row)


if __name__ == '__main__':
    db_name = 'mlops_db.db'
    query = 'SELECT * FROM MLOPS_TEST_TABLE ORDER BY id;'
    run_query(query,db_name)


