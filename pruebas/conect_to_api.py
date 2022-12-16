#Israel Valencia

from __future__ import annotations
import json 
import requests
import pendulum
import pandas as pd

#Se cargan las credenciales de acceso a la API
#Estas no están en el repositorio
credenciales = json.load(open("/home/isra/Descargas/kraken_user.json"))

#Credenciales del usuario con acceso a Kraken
user = credenciales["data"][0]["user"]
psw= credenciales["data"][0]["password"]

#Hearder de autorización
authorization= credenciales["data"][0]["authorization"]


#Establece las fechas inicual y final para filtrar los datos
fecha_inicio = pendulum.today(tz="America/Mazatlan").add(days=-2).format('YYYY-MM-DD')
fecha_fin = pendulum.today(tz="America/Mazatlan").add(days=-1).format('YYYY-MM-DD')


#Información de la API
api_url = "https://apikraken.coppel.com/wallet/datos-reporte"
headers =  '{'+f'"Accept":"*/*","Content-Type":"application/json","authorization":"{authorization}"'+'}'
body = '{'+f'"fh_inicio": "{fecha_inicio}","fh_fin": "{fecha_fin}","user": "{user}","password": "{psw}","tipoAuditoria": "Lighthouse","sn_semanaAnterior": "0" '+ '}'

try:
#Se realiza la petición a la API
    response = requests.post(api_url, json=json.loads(body), headers=json.loads(headers))
    #Se guarda el json con los datos de la petición
    data = response.json()
    #Se guardan los datos en un data frame
    df_data = pd.DataFrame.from_dict(data["data"]["metricas"][:],orient="columns")
except requests.exceptions.RequestException as e:  # Muestra el mensaje de error ystatus http
    print(f'Error: {e}\nHTTP ERROR: {response.status_code}')
    df_data = pd.DataFrame()

if len(df_data)>0:
    df_data.to_csv('/home/isra/Descargas/csv_lighthouse.csv',index=False) 