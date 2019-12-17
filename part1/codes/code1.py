import json
import pandas as pd
import numpy as np
import pymysql
from methods import *

# getting configuration files
with open('./config1.json') as config_data:
    config = json.load(config_data)

# connecting to mysql server
print('connecting to mysql server...')
sqlconf = config['mysql']
connection = pymysql.connect(host=sqlconf['host'], user=sqlconf['user'], password=sqlconf['password'], charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
print('connected.')

# creating database
with connection.cursor() as cursor:
    cursor.execute("DROP DATABASE digikala")
    cursor.execute("CREATE DATABASE digikala")
    cursor.execute("USE digikala")

# reading the data from excel file
print('reading data...')
try:
    data = pd.read_excel(config['data'])
except:
    data = pd.read_csv(config['data'])
print('read completed.')

# defining the categories to work with
categories = ['کتاب چاپی', 'پازل', 'ماوس (موشواره)', 'کیبورد (صفحه کلید)', 'محافظ صفحه نمایش گوشی', 'کیف و کاور گوشی']
fnfs = {
    'title_to_url': ['product_title_fa', 'url_code'],
    'category_to_keywords': ['category_title_fa', 'category_keywords'],
    'brand_fa_to_en': ['brand_name_fa', 'brand_name_en'],
    'id_to_alt': ['id', 'title_alt'],
    'product': ['id', 'product_title_fa', 'product_title_en', 'category_title_fa', 'brand_name_fa']
}

for category in categories:
    print(category)
    data_category = data[data.category_title_fa == category]
    attrs_table = create_attrs_table(data_category, get_attrs_keys(data_category))
    category_name = translate(category)
    try:
        with connection.cursor() as cursor:
            create_sql_table(cursor, category_name, attrs_table, 'id')
            print(category_name, 'table created.')
            insert_into_sql_table(connection, cursor, category_name, attrs_table)
            print(category_name, 'data inserted.')
            for table_name in fnfs:
                create_sql_table(cursor, table_name, data_category[fnfs[table_name]])
                insert_into_sql_table(connection, cursor, table_name, data_category[fnfs[table_name]])
            print('products tables created.')
    except Exception as e:
        print(e)
        pass

connection.close()
print('connection closed.')
