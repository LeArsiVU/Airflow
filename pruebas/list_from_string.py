#!/usr/bin/python3
# -- coding: utf-8 --

#Israel Valencia 2022-11-15

# import pandas lib as pd
import pandas as pd
import numpy as np
from typing import List, Iterable

import json

# Info del excel
excel_file = '/home/isra/Descargas/Canalización e Integración de datos.xlsx'
excel_sheet = 'Catálogo Dags Ejemplo'

#Lee el excel
#Cambia NaN a Null, ya que los NaN no son válidos en el typo json de la tabla serialized_dags
ctl_dags = pd.read_excel(excel_file,excel_sheet).replace(np.nan,'')

#Se guarda la información con el nombre de DAG como índice
params: Iterable[dict] = ctl_dags.set_index('DAG', drop=False).to_dict('records')

for param in params:
    tags_json= '{'+f'"tags":[{param["Tags"]}]'+'}'
    tag_dict = json.loads(tags_json)
    tag_list = tag_dict['tags']
    tag_list.append("AA")
    print(tag_list,type(tag_dict))


#Con esto se puede crear las tablas a partir de la las tablas
# Solo hay que colocar el nombre de las columnas en una columna del google sheet
col_names = '{"col_names":[{"name":"columna1","type":"INT","mode":"NULLABLE"},{"name":"columna2","type":"INT","mode":"NULLABLE"}]}'
col_dict = json.loads(col_names)

print(col_dict["col_names"][1]["name"])

