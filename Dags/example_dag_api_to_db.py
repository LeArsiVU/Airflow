#Israel Valencia

from __future__ import annotations

import json 
import requests
import pendulum
import pandas as pd


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



@task(task_id="api_to_jdbc")
def api_to_jdbc(parametros_conexion,query_destino,api_url,body,headers,schema,table):
    try: 
    #Intenta realizar la conexión
    
    #Se establece la conexión
        connection = jaydebeapi.connect(**parametros_conexion)

        connection.jconn.setAutoCommit(False)

        #Lee archivo con los datos
        df = get_data_api_kraken(api_url,body,headers)

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


def get_data_api_kraken(api_url,body,headers):
    try:
    #Se realiza la petición a la API
        response = requests.post(api_url, json=json.loads(body), headers=json.loads(headers))
    #Se guarda el json con los datos de la petición
        data = response.json()
    #Se guardan los datos en un data frame
    #Esta parte es la que puede cambiar con cada API
        df_data = pd.DataFrame.from_dict(data["data"]["metricas"][:],orient="columns")
    except requests.exceptions.RequestException as e:  # Muestra el mensaje de error ystatus http
        print(f'Error: {e}\nHTTP ERROR: {response.status_code}')
        df_data = pd.DataFrame()

    return df_data

import logging

logger = logging.getLogger(__name__)

def cw_sla_missed_take_action(*args, **kwargs):
    logger.info("************************************* SLA missed! ***************************************")
    logger.info(args)

# Info del excel
# Lee directamente desde el excel
# Por el momento el google sheets consultado tiene acceso público, se tendrá 
# que cambiar por una acceso por service account

FILE_ID = '1NsPnYiHwqVLBHR8QwOXaXEj1WakJ_kko3MDg2_4U2ds'
SHEET_ID = '2131814459'

URL = f'https://docs.google.com/spreadsheets/d/{FILE_ID}/export?format=xlsx&gid={SHEET_ID}'

#Lee el excel desde google sheets
#Cambia NaN a Null, ya que los NaN no son válidos en el typo json de la tabla serialized_dags
ctl_dags = pd.read_excel(URL).replace(np.nan,'')

#Se guarda la información con el nombre de DAG como índice
params: Iterable[dict] = ctl_dags.set_index('DAG', drop=False).to_dict('records')

doc_md ="""Ejemplo de dag que realiza una extracción de datos mediante una Rest API. 

Observaciones:
- EL detalle que existe en cuanto a realizar un request a una API es que el archivo de respuesta es diferente para cada una.
Por lo que muy probablemente se necesite crear un script para cada dag que utiliza las APIS como origen.
- Tambien puede presentarse que se requiera realizar transformaciones de columnas o agregar/quitar columns de archivo de respuesta.

Este archivo se puede usar como base, y en el caso de GCP los datos se pueden guardar en CSV en Google Storage para que BigQuery tome los datos desde ahí.
"""

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
                          'retry_delay':timedelta(minutes=0),
                          'sla': timedelta(seconds=5)},#El Dag debe ejecutarse dentro de este delta de tiempo despúes de que se inicie la ejecución por trigger.
            description= f"Actualiza  {param['DB Destino']}.{param['Esquema Destino']}.{param['Tabla Destino']}",
            tags=tag_list,
            sla_miss_callback=cw_sla_missed_take_action  # callback function
        ) as dag:
            
            #Se cargan las credenciales de acceso a la API
            #Estas no están en el repositorio
            credenciales = json.load(open(f'{param["Archivo Credenciales"]}'))
            
            #Credenciales del usuario con acceso a Kraken
            user = credenciales["data"][0]["user"]
            psw= credenciales["data"][0]["password"]

            #Hearder de autorización
            authorization= credenciales["data"][0]["authorization"]

            #Establece las fechas inicual y final para filtrar los datos
            fecha_inicio = pendulum.today(tz="America/Mazatlan").add(days=-2).format('YYYY-MM-DD')
            fecha_fin = pendulum.today(tz="America/Mazatlan").add(days=-1).format('YYYY-MM-DD')


            #Información de la API
            api_url = param["URL API"]
            headers =  '{'+f'"Accept":"*/*","Content-Type":"application/json","authorization":"{authorization}"'+'}'
            body = '{'+f'"fh_inicio": "{fecha_inicio}","fh_fin": "{fecha_fin}","user": "{user}","password": "{psw}","tipoAuditoria": "Lighthouse","sn_semanaAnterior": "0" '+ '}'

            if param["Tabla Destino"]!='':        
                #Conexion tabla destino
                #Parámetros de la conexión JDBC
                conn_param_destino = dict(jclassname=param["JDBC Name Destino"],
                            url=f'jdbc:{param["Tipo Destino"]}://{param["Host Destino"]}:{int(param["Puerto Destino"])}/{param["DB Destino"]}',
                            driver_args={'user':param["Usuario Destino"],'password':param["Password Destino"]},
                            jars=param["JDBC Driver Destino"])

                task_get_data_api = api_to_jdbc(conn_param_destino,
                                                f'{param["Query Destino"]} {param["Esquema Destino"]}.{param["Tabla Destino"]} {param["Filtro Query Destino"]}',
                                                api_url,body,headers,
                                                param["Esquema Destino"],
                                                param["Tabla Destino"])  
            task_get_data_api
