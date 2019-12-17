import pandas as pd
import numpy as np
from googletrans import Translator


def isNull(value):
    return value == np.NaN or pd.isnull(value) or value == 'nan'


def normalize(string):
    '''
        Normalizes the input to a proper form.
    '''
    return string.lower().replace('/', '').replace(':', '').replace(' ', '_').replace('__', '_').replace("'", '_').replace('(', '').replace(')', '')


def translate(text):
    '''
        Using google translate API to translate persian names to english.
    '''
    try:
        return normalize(Translator().translate(text).text)
    except:
        raise Exception('You are offline. Please connect to the internet and try again.')


def get_attrs_keys(dataframe):
    '''
        Extract keys from attribute column of the dataframe.
    '''
    keys = []
    for index, row in dataframe.iterrows():
        try:
            attrs = eval(str(row.product_attributes))
            for tup in attrs:
                if tup['Key'] not in keys:
                    keys.append(tup['Key'])
        except:
            pass
    return keys


def create_table_from_keys(data, keys):
    '''
        Creates a table with id equal to @data ids and with column names equal to keys.
    '''
    table = pd.DataFrame(data={'id': data.id})
    for key in keys:
        table[key] = np.NaN
    return table


def insert_values_to_tables(data, keys, table):
    '''
        Updates cell values based on data in product attribues column.
    '''
    for index, row in data.iterrows():
        attrs = {key: np.NaN for key in keys}
        try:
            pa = eval(str(row['product_attributes']))
            for tup in pa:
                try:
                    attrs[tup['Key']] = tup['Value']
                except:
                    pass
            for key in attrs:
                try:
                    table.at[index, key] = attrs[key]
                except:
                    table[key] = table[key].astype(type(attrs[key]))
                    table.at[index, key] = attrs[key]
        except:
            pass


def create_attrs_table(data, keys):
    '''
        Creates the table with correct values.
    '''
    table = create_table_from_keys(data, keys)
    insert_values_to_tables(data, keys, table)
    return table.rename(columns={key: translate(key) for key in keys})


def create_sql_table(cursor, name, table, key=None):
    '''
        Creates a table based on values in table.
    '''
    sql = []
    for col in table.columns:
        sql.append(col)
        #if (col == 'Date'):
        #    sql.append('Date,')
        #else:
        if (key is not None and col == key):
            sql.append('INT PRIMARY KEY,' if table[col].dtype == np.int64 or table[col].dtype == np.float64 else 'TEXT PRIMARY KEY,')
        else:
            sql.append('INT,' if table[col].dtype == np.int64 or table[col].dtype == np.float64 else 'TEXT,')
    sql[-1] = sql[-1].replace(',', '')
    sql = 'CREATE TABLE IF NOT EXISTS ' + name + ' (' + ' '.join(sql) + ')'
    cursor.execute(sql)


def insert_into_sql_table(connection, cursor, name, table):
    '''
        Inserts values in table.
    '''
    for index, row in table.iterrows():
        if index % 1000 == 0:
            connection.commit()
        # if index > 10000:
            # break
        try:
            sql = 'SELECT * FROM ' + name + ' WHERE ' + table.columns[0] + ' = %s'
            cursor.execute(sql, row[table.columns[0]])
            if (cursor.fetchone() is None):
                values = []
                for col in table.columns:
                    if isNull(row[col]):
                        col_val = None
                    else:
                        col_val = str(row[col])# if col != 'Date' else 'STR_TO_DATE(' + str(row[col]) + ', %Y-%m-%d)'
                    values.append(col_val)
                sql = 'INSERT INTO `' + name + '` (' + ', '.join([col for col in table.columns]) + ') VALUES (' + ', '.join(['%s' for i in range(len(values))]) + ')'
                cursor.execute(sql, tuple(values))
        except Exception as e:
            print(e)
            pass
    connection.commit()
