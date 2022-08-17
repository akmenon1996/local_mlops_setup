import pandas as pd 
import sqlite3
import re 


def define_dtype(input):
    if input == 'int':
        return 'int'
    elif input == 'datetime64[ns]':
        return 'date'
    elif input == 'float64':
        return 'numeric[5,2]'
    else:
        return 'varchar(500)'

def read_dataframe(connection, table_name, limit = None):
    """
    :params
        connection --> Connection to SQLite3 database
        table_name --> Name of the table you want to read from. 
        limit -->  Number of records to be returned. 
    """
    con = connection 
    if limit == None:
        df = pd.read_sql_query(f"SELECT * FROM {table_name};", con)
        return df
    else:
        df = pd.read_sql_query(f"SELECT * FROM {table_name} limit {limit};", con)
        return df 

def write_to_table(connection, df,table_name, if_exists = 'Fail'):
    """
    :params 
            connection --> Connection to SQLite3 database 
            df --> DataFrame that needs to be written 
            table_name --> Name of table to be written to. 
            if_exists --> 
                            Optional values --> 
                                    1) 'fail' --> If the table already exists, this will fail. (Default)
                                    2) 'replace' --> If the table already exists, this will replace it with new data. 
                                    3) 'append' --> If the table already exists, this will add new data to it.  
    """
    con = connection
    cur = con.cursor()
    df.to_sql(table_name, con, if_exists=if_exists,index = False)
    con.commit()    
    print(f"Data written to the table --> {table_name}")

def create_table(connection, df, table_name):
    con = connection
    cur = connection.cursor() 

    create_table_query = f'''
                            DROP TABLE IF EXISTS {table_name} <----->
                            CREATE TABLE {table_name} (
                                %f
                            )<----->
                         '''

    datatype_string = ''
    date_cols = df.columns[df.columns.str.contains(pat='Date|date')]
    for date_col in date_cols:
        try:
            df[date_col] = pd.to_datetime(df[date_col])
        except:
            pass
    dtypes_dict = df.dtypes.to_dict()
    for keys, values in dtypes_dict.items():
        lower_key = keys.lower().replace(' ','_').replace('-','_').replace(' - ','_')
        dtype_str = define_dtype(values)
        out_str = f'{lower_key}             {dtype_str}, \n'
        datatype_string+=out_str
    datatype_string = re.sub(r',', '', datatype_string[::-1], 1)[::-1]
    print(datatype_string)
    final_query =  create_table_query.replace('%f',datatype_string)
    print(final_query)
    for statement in final_query.split('<----->'):
        print("Executing query!")
        print(statement)
        cur.execute(f'{statement}')


if __name__ == '__main__':
    print("Use as a library by importing inside a file.")