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
excel_sheet = 'Ejecuta Ejemplo'

#Info del ejecuta
info_ejecuta = pd.read_excel(excel_file,excel_sheet,header=None,usecols=[0,1]).replace(np.nan,'').transpose()

header = info_ejecuta.iloc[0]
info_ejecuta.columns= header
info_ejecuta=info_ejecuta[1:]

#Lee el excel
#Cambia NaN a Null, ya que los NaN no son válidos en el typo json de la tabla serialized_dags
ctl_dags = pd.read_excel(excel_file,excel_sheet,usecols=[3,4,5]).replace(np.nan,'')
ctl_dags = ctl_dags[ctl_dags["DAG"]!='']

#Si el dag anterior está vació por default se asigna el operador de inicio
ctl_dags["DAG Anterior"] = ctl_dags["DAG Anterior"].replace('','Inicio')

#Si el dag siguiente está vació por default se asigna el operador de fin
ctl_dags["DAG Siguiente"] = ctl_dags["DAG Siguiente"].replace('','Fin')

#Se guarda la información con el nombre de DAG como índice
params: Iterable[dict] = ctl_dags.set_index('DAG', drop=False).to_dict('records')

if info_ejecuta["Activo"][1]==True:
    with DAG(
            info_ejecuta["Ejecuta"][1],
            schedule=info_ejecuta["Schedule"][1],
            start_date=pendulum.from_format(f'{info_ejecuta["Fecha Inicio"][1]}','YYYY-MM-DD HH:mm:ss', tz="America/Mazatlan"),
            catchup=False,
            dagrun_timeout=datetime.timedelta(minutes=4),
            default_args={'owner':info_ejecuta['Owner'][1], 
                            'retries':1,
                            'retry_delay':timedelta(minutes=0)},
            description= "Ejecuta una serie de dags",
            tags=["Ejecuta",info_ejecuta["Grupo"][1],info_ejecuta["Unidad De Negocio O Transversales"][1],info_ejecuta["Área De Negocio O Transversales"][1]]
                
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
                        task_id=param["DAG"],
                        trigger_dag_id=param["DAG"],
                        trigger_rule="all_success",
                        wait_for_completion=True,
                        poke_interval= 30,
                        dag=dag
                    )
            ops.append(op)
            i=i+1

        df_dags_posiciones =  pd.DataFrame.from_records(data=ops_list,columns=["DAG","Posicion"]).replace(np.nan,'')

        #Se guarda la información con el nombre de DAG como índice
        dags_posiciones: Iterable[dict] = df_dags_posiciones.set_index('DAG', drop=False).to_dict('index')

        for param in params:
            ops[dags_posiciones[param["DAG Anterior"]]["Posicion"]]>>ops[dags_posiciones[param["DAG"]]["Posicion"]]>>ops[dags_posiciones[param["DAG Siguiente"]]["Posicion"]]