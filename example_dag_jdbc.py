#Israel Valencia

from __future__ import annotations

import datetime 
from datetime import timedelta
import pendulum
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.models import DagModel

from typing import Iterable

import jaydebeapi
from   jaydebeapi import Error

import json

@task(task_id="from_jdbc")
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
excel_file = '/home/isra/Descargas/Canalización e Integración de datos.xlsx'
excel_sheet = 'Catálogo Dags Ejemplo (JDBC)'

#Lee el excel
#Cambia NaN a Null, ya que los NaN no son válidos en el typo json de la tabla serialized_dags
ctl_dags = pd.read_excel(excel_file,excel_sheet).replace(np.nan,'')

#Se guarda la información con el nombre de DAG como índice
params: Iterable[dict] = ctl_dags.set_index('DAG', drop=False).to_dict('records')

#Es posible definir más dags desde un solo archivo
for param in params:
    if param["Activo"] == True:
        
        #Si no se tiene una expresion CRON entonces se establece un valor de None
        if param["Schedule"] != '':
            scheduling = param["Schedule"]
        else:
            scheduling = None

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
            start_date=pendulum.from_format(f'{param["Fecha Inicio"]}','YYYY-MM-DD', tz="America/Mazatlan"),
            catchup=False,
            dagrun_timeout=datetime.timedelta(minutes=4),
            default_args={'owner':param['Dueño'], 
                          'retries':0,
                          'retry_delay':timedelta(minutes=0)},
            description= f"Actualiza  {param['Proyecto']}.{param['Dataset']}.{param['Tabla']}",
            tags=tag_list
        ) as dag:

            #Parámetros de la conexión JDBC
            conn_param = dict(jclassname=param["JDBC Name"],
                           url=f'jdbc:{param["Tipo Origen"]}://{param["Host"]}:{param["Puerto"]}/{param["DB Origen"]}',
                           driver_args={'user':param["Usuario"],'password':param["Password"]},
                           jars=param["JDBC Driver"])

            #Se hace el llamdo a la función que se conecta por JDBC
            task_from_jdbc_to_csv=jdbc_to_csv(conn_param,
                                          param["Query Origen"],
                                          param["Ubicación Temporal"],
                                          param["Esquema Origen"],
                                          param["Tabla Origen"])  

            #Ejecuta un dag externo
            #Si no se asigna un dag externo entonces se genera un operador vacío
            if param["Executa DAG"]!='':
                task_trigger = TriggerDagRunOperator(
                    task_id = 'trigger_'+param["Executa DAG"],
                    trigger_dag_id = param["Executa DAG"],
                    trigger_rule = "all_success",
                    dag = dag
                )    
            else:
                task_trigger  = EmptyOperator(
                    task_id="empty",
                    trigger_rule="all_success",
                )
                
            #Se asigna orden de ejecuión de las tareas y operadores
            task_from_jdbc_to_csv>>task_trigger