import sqlite3
con = sqlite3.connect('mlops_db.db')
cur = con.cursor()



for row in cur.execute('SELECT * FROM MLOPS_TEST_TABLE ORDER BY id;'):
    print(row)

