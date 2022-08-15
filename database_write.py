import sqlite3
con = sqlite3.connect('mlops_db.db')
cur = con.cursor()

create_query = '''
                    DROP TABLE IF EXISTS MLOPS_TEST_TABLE;
                    CREATE TABLE MLOPS_TEST_TABLE (
                        ID  INT NOT NULL,
                        NAME VARCHAR(50)
                    );
               '''

for statement in create_query.split(';'):
    print("Executing query!")
    cur.execute(f'{statement};')



insert_query = '''
                    INSERT INTO MLOPS_TEST_TABLE 
                        VALUES (1, 'ABHIJIT'),
                               (2,'JONDOE'),
                               (3,'JANEDOE');
                    COMMIT;
               '''

print("Inserting Data.")
for statement in insert_query.split(';'):
    print("Executing query!")
    cur.execute(f'{statement};')

for row in cur.execute('SELECT * FROM MLOPS_TEST_TABLE ORDER BY id;'):
    print(row)