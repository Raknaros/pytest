import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_proveedores(ruta: str):
    engine = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                           ':3306/salessystem')

    customers = pd.read_excel(ruta + '/proveedores.xlsx', sheet_name='Result 1', date_format='%d/%m/%Y',
                              dtype={'ruc': np.int64}, na_values=' ')

    return print(customers.to_sql('proveedores', engine, if_exists='append', index=False))  #


load_proveedores('D:/OneDrive/facturacion')
