import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_proveedores(ruta: str):
    engine = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                           ':3306/salessystem')

    customers = pd.read_excel(ruta + '/importar.xlsx', sheet_name='customers', date_format='%d/%m/%Y',
                                   dtype={'ruc': np.int64}, na_values=' ')

    str_columns = ['alias', 'nombre_razon', 'related_user', 'observaciones']

    # sumar peso total de los articulos por cotizacion y colocarlo en datos adicionales de guia
    for column in str_columns:
        if customers[column].notna().any():
            customers[column] = customers[column].apply(lambda x: x.strip().upper() if pd.notna(x) else x)

    return print(customers.to_sql('customers', engine, if_exists='append', index=False))  #


load_customers('C:/Users/Raknaros/Desktop/temporal')