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

i=0
ops_list=[]

for param in params:
    i=i+1
    op_info= (param["DAG"],i)
    ops_list.append(op_info)

df_dags_posiciones =  pd.DataFrame.from_records(data=ops_list,columns=["DAG","Posicion"]).replace(np.nan,'')

#Se guarda la información con el nombre de DAG como índice
dags_posiciones: Iterable[dict] = df_dags_posiciones.set_index('DAG', drop=False).to_dict('index')

for param in params:
    print(f'{param["DAG"]}: {dags_posiciones[param["DAG"]]["Posicion"]}')
