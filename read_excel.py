#!/usr/bin/python3
# -- coding: utf-8 --

#Israel Valencia 2022-11-15

# import pandas lib as pd
import pandas as pd
from typing import List, Iterable
 
# Info del excel
excel_file = 'C:\\Users\\israel.valencia\\Downloads\\Canalización e Integración de datos.xlsx'
excel_sheet = 'General (Ejemplo Propuesta 1)'

#Lee el excel
ctl_dags = pd.read_excel(excel_file,excel_sheet)

#Se guarda la información con el nombre de DAG como índice
params: Iterable[dict] = ctl_dags.set_index('DAG', drop=False).to_dict('records')

for param in params:
    print(f'{param["DAG"]}')