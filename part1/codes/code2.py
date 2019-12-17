import json
import pandas as pd
import numpy as np
import pymysql
from methods import *

# getting configuration files
with open('./config2.json') as config_data:
    config = json.load(config_data)

# connecting to mysql server
print('connecting to mysql server...')
sqlconf = config['mysql']
connection = pymysql.connect(host=sqlconf['host'], user=sqlconf['user'], password=sqlconf['password'], charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
print('connected.')

# creating database
with connection.cursor() as cursor:
    cursor.execute("CREATE DATABASE IF NOT EXISTS digikala")
    cursor.execute("USE digikala")

# reading the data from excel file
print('reading data...')
try:
    data = pd.read_excel(config['data'])
except:
    data = pd.read_csv(config['data'])
print('read completed.')

data[['Date', 'Time']] = data.DateTime_CartFinalize.str.split(" ", expand=True)
data = data.drop(['DateTime_CartFinalize'], axis=1)

with connection.cursor() as cursor:
    create_sql_table(cursor, 'buy_history', data, 'id')
    insert_into_sql_table(connection, cursor, 'buy_history', data)
    cursor.execute('ALTER TABLE buy_history MODIFY COLUMN Date VARCHAR(255)')
    cursor.execute('CREATE INDEX `date` ON buy_history (Date)')
