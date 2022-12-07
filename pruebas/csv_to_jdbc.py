#Israel Valencia

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import numpy as np

from airflow.models import DagModel

from typing import Iterable

import jaydebeapi,os


#
dsn_database = "public"
dsn_hostname = "localhost"
dsn_port = "3306"
dsn_uid = "root"
dsn_pwd = "I$ra2022@"
jdbc_driver_name = "com.mysql.jdbc.Driver"
jdbc_driver_loc = os.path.join(r'/home/isra/Documentos/JDBC Drivers/mysql-connector-j-8.0.31.jar')
#


sql_str = "select version()"

connection_string='jdbc:mysql://'+ dsn_hostname+':'+ dsn_port +'/'+ dsn_database+'?defaultAutoCommit="false"'

url = f'{connection_string}:user={dsn_uid};password={dsn_pwd}'

print("Connection String: " + url)

schema_name= 'public'
table = 'tabla_jdbc'

##### Inicio

connection = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': dsn_uid, 'password': dsn_pwd},
jars=jdbc_driver_loc)

df = pd.read_csv('/home/isra/Descargas/tabla_jdbc.csv').replace(np.nan,'')


cursor = connection.cursor()
sql_exceptions = []
row_nbr = 0
df_length = df.shape[0]

schema_table = f"{schema_name}.{table}"


cols_names_list = df.columns.values.tolist()
cols_names = f'({",".join(cols_names_list)})'

chunksize = 1000

while row_nbr < df_length:       
    # Determine insert statement boundaries (size)
    beginrow = row_nbr
    endrow = df_length 
    endrow = df_length if (row_nbr+chunksize) > df_length else row_nbr + chunksize 

        # Extract the chunk
    tuples = [tuple(x) for x in df.values[beginrow : endrow]]            
    values_params = '(?,?,?)'       
    sql = f"INSERT INTO {schema_table} {cols_names} VALUES {values_params}"

    print(tuples)

    try:
        cursor.executemany(sql,tuples)
        connection.commit()
    except Exception as e: 
        sql_exceptions.append((beginrow,endrow, e))

    row_nbr = endrow

    cursor.close()
    connection.close()

print(sql_exceptions)
