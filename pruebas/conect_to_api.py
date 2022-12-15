#Israel Valencia

from __future__ import annotations
import json 
import requests
import pendulum
import pandas as pd

#Carga credenciales
credenciales = json.load(open("/home/isra/Descargas/kraken_user.json"))
user = credenciales["data"][0]["user"]
psw= credenciales["data"][0]["password"]
authorization= credenciales["data"][0]["authorization"]

fecha_inicio = pendulum.today(tz="America/Mazatlan").add(days=-2).format('YYYY-MM-DD')
fecha_fin = pendulum.today(tz="America/Mazatlan").add(days=-1).format('YYYY-MM-DD')
api_url = "https://apikraken.coppel.com/wallet/datos-reporte"
headers =  '{'+f'"Accept":"*/*","Content-Type":"application/json","authorization":"{authorization}"'+'}'
body = '{'+f'"fh_inicio": "{fecha_inicio}","fh_fin": "{fecha_fin}","user": "{user}","password": "{psw}","tipoAuditoria": "Lighthouse","sn_semanaAnterior": "0" '+ '}'

response = requests.post(api_url, json=json.loads(body), headers=json.loads(headers))

data = response.json()

df_data = pd.DataFrame.from_dict(data["data"]["metricas"][:],orient="columns")

print(df_data)