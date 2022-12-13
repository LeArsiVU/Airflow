import pandas as pd
import numpy as np
from typing import Iterable

FILE_ID = '1NsPnYiHwqVLBHR8QwOXaXEj1WakJ_kko3MDg2_4U2ds'
SHEET_ID = '559945068'
url = f'https://docs.google.com/spreadsheets/d/{FILE_ID}/export?format=xlsx&gid={SHEET_ID}'

#Lee google sheets
#Cambia NaN a Null, ya que los NaN no son válidos en el typo json de la tabla serialized_dags

ctl_dags = pd.read_excel(url,usecols=[3,4,5]).replace(np.nan,'')
ctl_dags = ctl_dags[ctl_dags["DAG"]!='']

#Se guarda la información con el nombre de DAG como índice
params: Iterable[dict] = ctl_dags.set_index('DAG', drop=False).to_dict('records')

print(params)
