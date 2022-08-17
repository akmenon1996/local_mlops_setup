import sqlite3
import pandas as pd
import database


con = sqlite3.connect('mlops_db.db')
cur = con.cursor()
df = pd.read_sql_query("SELECT * FROM MLOPS_TEST_TABLE ORDER BY id;", con)
df['new_col'] = 'Test_Val'

# Write the new DataFrame to a new SQLite table
df.to_sql("new_table", con, if_exists="replace")

table_name = 'new_mlops_table'
database.create_table(con, df, table_name)

database.write_to_table(con,df,table_name,'append')


df = database.read_dataframe(con, table_name)
print(df)

for row in cur.execute(f'SELECT * FROM {table_name};'):
    print(row)

con.close()