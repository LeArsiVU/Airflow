
from __future__ import annotations

import datetime 
from datetime import timedelta
import pendulum
import pandas as pd
import numpy as np

from airflow.operators.bash import BashOperator

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.datasets import Dataset 

# Es posible lanzar la ejecución de un dag a partir de la  actualización de un archivo
# Pero esta actualización debe ser realizada por un proceso dentro de Airflow
# como en el dag dag_update_dataset indica que el dataset se actualizó medialte el parámetro outlets.

dataset_1=Dataset("home/isra/Documentos/Python/Airflow/archivos/CatalogodeLocalidades.csv")


with DAG( "dag_update_dataset",
            start_date=pendulum.from_format('2022-11-01','YYYY-MM-DD', tz="America/Mazatlan"),
            catchup=False,            
            schedule= None
        ) as dag:

            BashOperator(
                task_id="task_upstream",
                bash_command="firefox",
                outlets=[dataset_1]
            )


with DAG( "dag_triggered_by_dataset",
            start_date=pendulum.from_format('2022-11-01','YYYY-MM-DD', tz="America/Mazatlan"),
            catchup=False,            
            schedule= [dataset_1]
        ) as dag:
            
             task_trigger = TriggerDagRunOperator(
                    task_id = 'trigger_dagcsvpruebaslocalidades',
                    trigger_dag_id = "dag-csv-pruebas-localidades",
                    trigger_rule = "all_success",
                    dag = dag
                )    
            