#Israel Valencia

from __future__ import annotations

import datetime 
from datetime import timedelta
import pendulum
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from typing import Iterable

import jaydebeapi
from   jaydebeapi import Error

import json

@task(task_id="csv_to_jdbc")
def csv_to_jdbc(parametros_conexion,query_destino,path_root,schema,table):
    try: 
    #Intenta realizar la conexión
    
    #Se establece la conexión
        connection = jaydebeapi.connect(**parametros_conexion)

        connection.jconn.setAutoCommit(False)

        #Lee archivo con los datos
        df = pd.read_csv(path_root).replace(np.nan,'')

        cursor = connection.cursor()

        #Se ejecuta la consulta
        cursor.execute(query_destino)

        sql_exceptions = []
        row_nbr = 0
        df_length = df.shape[0]

        schema_table = f"{schema}.{table}"

        cols_names_list = df.columns.values.tolist()
        cols_names = f'({",".join(cols_names_list)})'

        chunksize = 1000

        print(cols_names_list)

        while row_nbr < df_length:       
            # Determine insert statement boundaries (size)
            beginrow = row_nbr
            endrow = df_length 
            endrow = df_length if (row_nbr+chunksize) > df_length else row_nbr + chunksize 

                # Extract the chunk
            tuples = [tuple(x) for x in df.values[beginrow : endrow]]     

            values_params = '('+",".join('?' for i in cols_names_list)+')' 


            #Las columnas del dataframe deben tener el mismo nombre que las columnas de las tablas
            sql = f"INSERT INTO {schema_table} {cols_names} VALUES {values_params}"

            try:
                cursor.executemany(sql,tuples)
                connection.commit()
            except Exception as e: 
                sql_exceptions.append((beginrow,endrow, e))

            row_nbr = endrow

        return sql_exceptions

    except (Exception,Error) as error:
        #Si el intento no funciona entonces arroja el mensaje de error
        print("Error de conexión: ",error)
        return []
    finally:
        #Para finalizar cierra la conexión si está abierta.
        if (connection):
            cursor.close()
            connection.close()
            print("Conexión cerrada.")

@task(task_id="jdbc_to_csv")
def jdbc_to_csv(parametros_conexion,query,path_root,schema,table):
    try: 
    #Intenta realizar la conexión
    
    #Se establece la conexión
        connection = jaydebeapi.connect(**parametros_conexion)

        cursor = connection.cursor()

        #Se ejecuta la consulta
        cursor.execute(query)

        #Nombre de las columnas de la tabla consultada
        columnas = []
        for col in cursor.description:
            columnas.append(col[0])

    
        #Se inicializa la lista con los regostros
        rows =[]

        #Se inicializa con el primer registro
        row=cursor.fetchone()

        while row:
            #Se guarda cada registro en la lista
            rows.append(row)
            #Se obtiene el siguiente registro
            row=cursor.fetchone()

        #Crear el dataframe de salida
        
        data =  pd.DataFrame.from_records(data=rows,columns=columnas)
        data.to_csv(f'{path_root}/{schema}_{table}.csv',index=False)

        return 0

    except (Exception,Error) as error:
        #Si el intento no funciona entonces arroja el mensaje de error
        print("Error de conexión: ",error)
        data =  pd.DataFrame()
        data.to_csv(f'{path_root}/{schema}_{table}.csv',index=False)
        return 0
    finally:
        #Para finalizar cierra la conexión si está abierta.
        if (connection):
            cursor.close()
            connection.close()
            print("Conexión cerrada.")


# Info del excel
# Lee directamente desde el excel
# Por el momento el google sheets consultado tiene acceso público, se tendrá 
# que cambiar por una acceso por service account

FILE_ID = '1NsPnYiHwqVLBHR8QwOXaXEj1WakJ_kko3MDg2_4U2ds'
SHEET_ID = '1592617781'

URL = f'https://docs.google.com/spreadsheets/d/{FILE_ID}/export?format=xlsx&gid={SHEET_ID}'

#Lee el excel desde google sheets
#Cambia NaN a Null, ya que los NaN no son válidos en el typo json de la tabla serialized_dags
ctl_dags = pd.read_excel(URL).replace(np.nan,'')

#Se guarda la información con el nombre de DAG como índice
params: Iterable[dict] = ctl_dags.set_index('DAG', drop=False).to_dict('records')

##Documentación del dag##
doc_md = """
# Dag generado automáticamente 

Este dag es generado automáticamente a partir de la información documentada en el siguiente archivo de control:
- [Catálogo Dags (JDBC TO JDBC)](https://docs.google.com/spreadsheets/d/1NsPnYiHwqVLBHR8QwOXaXEj1WakJ_kko3MDg2_4U2ds/edit#gid=1592617781)

## Casos del proceso de ETL

### Caso 1

Si asignamos una tabla origen entonces se realiza el siguiente proceso:

1. Se copian los datos de la tabla origen mediante una conexión JDBC hacia un archivo csv.
2. Se copian los datos desde el csv generado hacia una tabla destino  mediante una conexión JDBC.

<img src="https://github.com/LeArsiVU/Airflow/blob/main/imagenes/example_dag_jdbc_db_csv_db_caso_1.png?raw=true" width=500>

### Caso 2
Si no asignamos una tabla origen entonces se realiza el siguiente proceso:

- Se copian los datos desde el csv que se indica en la columna "Ubicación Temporal" hacia una tabla destino  mediante una conexión JDBC.


<img src="https://github.com/LeArsiVU/Airflow/blob/main/imagenes/example_dag_jdbc_db_csv_db_caso_2.png?raw=true" width=300>

### Caso 3

- Se copian los datos de la tabla origen mediante una conexión JDBC hacia un archivo csv con el nomnre esquema_tabla, dada la ubicación en "Ubicación Temporal".


<img src="https://github.com/LeArsiVU/Airflow/blob/main/imagenes/example_dag_jdbc_db_csv_db_caso_3.png?raw=true" width=300>

"""
#####

#Es posible definir más dags desde un solo archivo
for param in params:
    if param["Activo"] == True:
        
        #Si no se tiene una expresion CRON entonces se establece un valor de None
        scheduling = param["Schedule"] if param["Schedule"] != '' else None

        #Genera Etiquetas
        tags_json= '{'+f'"tags":[{param["Tags"]}]'+'}'
        tag_dict = json.loads(tags_json)
        tag_list = tag_dict['tags']
        tag_list.append(param["Grupo"])
        tag_list.append(param["Tipo Origen"])
        tag_list.append(param["Unidad De Negocio O Transversales"])
        tag_list.append(param["Área De Negocio O Transversales"])

        #Genera DAG de acuerdo a los parámetros
        with DAG(
            param["DAG"],
            schedule=scheduling,
            doc_md=doc_md,
            start_date=pendulum.from_format(f'{param["Fecha Inicio"]}','YYYY-MM-DD', tz="America/Mazatlan"),
            catchup=False,
            dagrun_timeout=datetime.timedelta(minutes=4),
            default_args={'owner':param['Owner'], 
                          'retries':0,
                          'retry_delay':timedelta(minutes=0)},
            description= f"Actualiza  {param['DB Destino']}.{param['Esquema Destino']}.{param['Tabla Destino']}",
            tags=tag_list
        ) as dag:
            
            if param["Tabla Origen"]!='':
                #Conexion tabla origen
                #Parámetros de la conexión JDBC
                conn_param = dict(jclassname=param["JDBC Name"],
                            url=f'jdbc:{param["Tipo Origen"]}://{param["Host Origen"]}:{int(param["Puerto Origen"])}/{param["DB Origen"]}',
                            driver_args={'user':param["Usuario Origen"],'password':param["Password Origen"]},
                            jars=param["JDBC Driver"])

                #Se hace el llamado a la función que se conecta por JDBC
                task_from_jdbc_to_csv=jdbc_to_csv(conn_param,
                                            param["Query Origen"],
                                            param["Ubicación Temporal"],
                                            param["Esquema Origen"],
                                            param["Tabla Origen"])  
            
            if param["Tabla Destino"]!='':        
                #Conexion tabla destino
                #Parámetros de la conexión JDBC
                conn_param_destino = dict(jclassname=param["JDBC Name Destino"],
                            url=f'jdbc:{param["Tipo Destino"]}://{param["Host Destino"]}:{int(param["Puerto Destino"])}/{param["DB Destino"]}',
                            driver_args={'user':param["Usuario Destino"],'password':param["Password Destino"]},
                            jars=param["JDBC Driver Destino"])

                #Se hace el llamado a la función que se conecta por JDBC
                task_csv_to_jdbc=csv_to_jdbc(conn_param_destino,
                                            f'{param["Query Destino"]} {param["Esquema Destino"]}.{param["Tabla Destino"]} {param["Filtro Query Destino"]}',
                                            f'{param["Ubicación Temporal"]}/{param["Esquema Origen"]}_{param["Tabla Origen"]}.csv' if param["Tabla Origen"]!='' else param["Ubicación Temporal"],
                                            param["Esquema Destino"],
                                            param["Tabla Destino"])  

            #Ejecuta un dag externo
            if param["Executa DAG"]!='':
                task_trigger = TriggerDagRunOperator(
                    task_id = 'trigger_'+param["Executa DAG"],
                    trigger_dag_id = param["Executa DAG"],
                    trigger_rule = "all_success",
                    dag = dag
                )    
            
            #Se asigna orden de ejecuión de las tareas y operadores
            #Caso 1
            if param["Tabla Origen"]!='' and param["Tabla Destino"]!='' and param["Executa DAG"]!='':
                task_from_jdbc_to_csv>>task_csv_to_jdbc>>task_trigger
            elif param["Tabla Origen"]!='' and param["Tabla Destino"]!='' and param["Executa DAG"]=='':
                task_from_jdbc_to_csv>>task_csv_to_jdbc
            #Caso 2
            elif param["Tabla Origen"]=='' and param["Tabla Destino"]!='' and param["Executa DAG"]!='':
                task_csv_to_jdbc>>task_trigger
            elif param["Tabla Origen"]=='' and param["Tabla Destino"]!='' and param["Executa DAG"]=='':
                task_csv_to_jdbc
            #Caso 3
            elif param["Tabla Origen"]!='' and param["Tabla Destino"]=='' and param["Executa DAG"]!='':
                task_from_jdbc_to_csv>>task_trigger
            elif param["Tabla Origen"]!='' and param["Tabla Destino"]=='' and param["Executa DAG"]=='':
                task_from_jdbc_to_csv
            else: 
                print("Secuencia sin definir")