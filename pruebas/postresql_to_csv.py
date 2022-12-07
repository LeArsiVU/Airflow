#!/usr/bin/python3
# -- coding: utf-8 --

#Israel Valencia 2022-11-23

import pandas as pd
import  psycopg2 as pg
from   psycopg2 import Error

#Parámetros de la conexión
parametros_conexion = dict(host="localhost",
                           port="5432",
                           database="airflow_db",
                           user="isra",
                           password="I$ra2022")
                           
query = 'SELECT dag_id, fileloc, dag_hash FROM public.serialized_dag'

try: 
    #Intenta realizar la conexión
    
    #Se establece la conexión
    connection = pg.connect(**parametros_conexion)
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
    df_output.to_csv('/home/isra/Descargas/tabla.csv',index=False)
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