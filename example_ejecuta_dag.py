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

#Permite saber la posicion de creación de un operador dado su nombre

# Info del excel
excel_file = '/home/isra/Descargas/Canalización e Integración de datos.xlsx'
excel_sheet = 'Exemplo Ejecuta'

#Lee el excel
#Cambia NaN a Null, ya que los NaN no son válidos en el typo json de la tabla serialized_dags
ctl_dags = pd.read_excel(excel_file,excel_sheet).replace(np.nan,'')

#Se guarda la información con el nombre de DAG como índice
params: Iterable[dict] = ctl_dags.set_index('DAG', drop=False).to_dict('records')

with DAG(
        "dag_ejecuta_exemplo",
        schedule=None,
        start_date=pendulum.today('America/Mazatlan').add(days=-2),
        catchup=False
) as dag:

    Inicio = EmptyOperator(
                    task_id="Inicio",
                    trigger_rule="all_success",
                )

    Fin = EmptyOperator(
                    task_id="Fin",
                    trigger_rule="all_success",
                )


    ops_list=[]

    op_info= ("Inicio",0)
    ops_list.append(op_info)

    op_info= ("Fin",1)
    ops_list.append(op_info)

    ops=[]

    ops.append(Inicio)

    ops.append(Fin)

    i=2

    for param in params:
        op_info= (param["DAG"],i)
        ops_list.append(op_info)
        op= TriggerDagRunOperator(
                    task_id = param["DAG"],
                    trigger_dag_id=param["DAG"],
                    trigger_rule="all_success",
                    dag=dag
                )
        ops.append(op)
        i=i+1

    df_dags_posiciones =  pd.DataFrame.from_records(data=ops_list,columns=["DAG","Posicion"]).replace(np.nan,'')

    #Se guarda la información con el nombre de DAG como índice
    dags_posiciones: Iterable[dict] = df_dags_posiciones.set_index('DAG', drop=False).to_dict('index')

    for param in params:
        ops[dags_posiciones[param["DAG Anterior"]]["Posicion"]]>>ops[dags_posiciones[param["DAG"]]["Posicion"]]>>ops[dags_posiciones[param["DAG Siguiente"]]["Posicion"]]
        print(f'{param["DAG"]}: {dags_posiciones[param["DAG"]]["Posicion"]}')