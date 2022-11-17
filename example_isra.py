#Israel Valencia

from __future__ import annotations

import datetime
import pendulum
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator

from airflow.models import DagModel

from typing import List, Iterable

# Info del excel
excel_file = '/home/isra/Descargas/Canalización e Integración de datos.xlsx'
excel_sheet = 'General (Ejemplo Propuesta 2)'

#Lee el excel
ctl_dags = pd.read_excel(excel_file,excel_sheet)

#Se guarda la información con el nombre de DAG como índice
params: Iterable[dict] = ctl_dags.set_index('DAG', drop=False).to_dict('records')

#Es posible definir más dags desde un solo archivo

for param in params:
	if param["Activo"] == 1:
		with DAG(
		    param["DAG"],
		    schedule=param["Schedule"],
		    start_date=pendulum.datetime(2022, 11, 15, tz="UTC"),
		    catchup=False,
		    dagrun_timeout=datetime.timedelta(minutes=4),
		    tags=[param["Canalización"]]
		) as dag:
		    #Activar DAG
		    #dag = DagModel.get_dagmodel(param["DAG"])
		    #dag.set_is_paused(is_paused=False)

		    Tarea_1 = DummyOperator(
					task_id="Tarea_1",
					trigger_rule="all_success",
		    )			    
		    Tarea_2 = DummyOperator(
					task_id="Tarea_2",
					trigger_rule="all_success",
		    )
		    Tarea_3 = DummyOperator(
					task_id="Tarea_3",
					trigger_rule="all_success",
		    )
		    Tarea_4 = DummyOperator(
					task_id="Tarea_4",
					trigger_rule="all_success",
		    )
			    
		Tarea_1>>[Tarea_2,Tarea_3]>>Tarea_4