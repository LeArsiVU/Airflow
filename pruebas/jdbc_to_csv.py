#!/usr/bin/python3
# -- coding: utf-8 --

#Israel Valencia 2022-11-23

import pandas as pd

import jaydebeapi,os
from   jaydebeapi import Error

#Parámetros de la conexión

jdbc_driver_name = "org.postgresql.Driver"
jdbc_driver_loc = os.path.join(r'/home/isra/Documentos/JDBC Drivers/postgresql-42.5.1.jar')


connection_string = 'jdbc:postgresql://localhost:5432/airflow_db'
                           
query = 'SELECT dag_id, fileloc, dag_hash FROM public.serialized_dag'

try: 
    #Intenta realizar la conexión
    
    #Se establece la conexión
    connection = jaydebeapi.connect(jclassname=jdbc_driver_name,
    url=connection_string, driver_args={'user':'isra','password':'I$ra2022'},
    jars=jdbc_driver_loc)

    cursor = connection.cursor()

    #Se ejecuta la consulta
    cursor.execute(query)

    #Nombre de las columnas de la tabla consultada
    columnas = []
    for col in cursor.description:
       columnas.append(col[0])

   
    #Se inicializa la lista con los regostros
    datos =[]

    #Se inicializa con el primer registro
    row=cursor.fetchone()

    while row:
        #Se guarda cada registro en la lista
        datos.append(row)
        #Se obtiene el siguiente registro
        row=cursor.fetchone()

    #Crear el dataframe de salida
    df_output = pd.DataFrame.from_records(data=datos,columns=columnas)

    #guarda en csv
    df_output.to_csv('/home/isra/Descargas/tabla_jdbc.csv',index=False)
    print("¡Hecho!")

except (Exception,Error) as error:
    #Si el intento no funciona entonces arroja el mensaje de error
    print("Error de conexión: ",error)
finally:
    #Para finalizar cierra la conexión si está abierta.
    if (connection):
        cursor.close()
        connection.close()
        print("Conexión cerrada.")