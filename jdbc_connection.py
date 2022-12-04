#Israel Valencia

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import numpy as np

from airflow.models import DagModel

from typing import Iterable

import jaydebeapi,os


#
dsn_database = "airflow_db"
dsn_hostname = "localhost"
dsn_port = "5432"
dsn_uid = "isra"
dsn_pwd = "I$ra2022"
jdbc_driver_name = "org.postgresql.Driver"
jdbc_driver_loc = os.path.join(r'/home/isra/Documentos/JDBC Drivers/postgresql-42.5.1.jar')
#

sql_str = "select version()"

connection_string='jdbc:postgresql://'+ dsn_hostname+':'+ dsn_port +'/'+ dsn_database

url = f'{connection_string}:user={dsn_uid};password={dsn_pwd}'

print("Connection String: " + url)

conn = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': dsn_uid, 'password': dsn_pwd},
jars=jdbc_driver_loc)

curs = conn.cursor()
curs.execute(sql_str)
result = curs.fetchall()

print(f"Versión del postgresql:\n{result[0]}")